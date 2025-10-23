import pytest
import pytest_asyncio
import asyncio
import os
import tempfile
import gc
from src.model import Event, StatsResponse
from src.aggregator import EventAggregator
from src.dedup_store import DeduplicationStore
from pydantic import ValidationError

# FIXTURE: Membuat lingkungan test dengan database sementara
@pytest_asyncio.fixture
async def test_app():
    """Fixture membuat instance aggregator dan dedup store baru di setiap test"""
    # Membuat file database unik sementara
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        test_db = tmp.name

    # Inisialisasi dedup store dan aggregator
    dedup_store = DeduplicationStore(test_db)
    aggregator = EventAggregator(dedup_store)

    # Pastikan database dalam kondisi bersih
    await dedup_store.clear()

    # yield → memberikan objek ke test function
    yield aggregator, dedup_store

    # Setelah test selesai, hentikan proses aggregator & tutup koneksi DB
    await aggregator.stop()
    await dedup_store.close()
    await asyncio.sleep(0.5)  # memberi waktu thread pool tertutup
    gc.collect()

    # Hapus file database sementara
    if os.path.exists(test_db):
        try:
            os.remove(test_db)
        except (PermissionError, OSError) as e:
            print(f"Warning: gagal hapus DB {test_db}: {e}")

# TEST 1: Deduplication
@pytest.mark.asyncio
async def test_1_deduplication_basic(test_app):
    """Pastikan event duplikat tidak diproses dua kali"""
    agg, _ = test_app
    await agg.start()

    # Dua event dengan event_id sama → duplikat
    e1 = Event(topic="test.topic", event_id="evt_001",
               timestamp="2025-10-18T10:00:00Z", source="test", payload={})
    e2 = Event(topic="test.topic", event_id="evt_001",
               timestamp="2025-10-18T10:01:00Z", source="test", payload={})

    # Publish keduanya
    await agg.publish([e1, e2])
    await asyncio.sleep(0.5)

    # Hanya satu event unik yang boleh diproses
    assert agg.stats["unique_processed"] == 1
    assert agg.stats["duplicate_dropped"] >= 1

# TEST 2: Persistensi Setelah Restart
@pytest.mark.asyncio
async def test_2_persistence_after_restart(test_app):
    """Pastikan dedup store masih mengenali event setelah restart"""
    agg, store = test_app
    db_path = store.db_path
    await agg.start()

    event = Event(topic="persist", event_id="evt_persist_001",
                  timestamp="2025-10-18T10:00:00Z", source="test", payload={})

    await agg.publish([event])
    await asyncio.sleep(0.5)
    await agg.stop()

    # Buat aggregator baru tapi dengan DB yang sama → seharusnya tahu event lama
    new_store = DeduplicationStore(db_path)
    new_agg = EventAggregator(new_store)
    await new_agg.start()
    await new_agg.publish([event])
    await asyncio.sleep(0.5)

    assert new_agg.stats["duplicate_dropped"] >= 1  # karena event sudah ada
    assert new_agg.stats["unique_processed"] == 0
    await new_agg.stop()

# TEST 3: Validasi Schema (Event)
@pytest.mark.asyncio
async def test_3_schema_validation():
    """Pastikan timestamp tervalidasi sesuai format ISO8601"""
    valid = Event(topic="valid", event_id="evt_valid",
                  timestamp="2025-10-18T10:00:00Z", source="test", payload={})
    assert valid.topic == "valid"

    # Kasus gagal validasi
    with pytest.raises(ValidationError):
        Event(topic="invalid", event_id="evt_invalid",
              timestamp="not-a-timestamp", source="test", payload={})

# TEST 4: Konsistensi Statistik
@pytest.mark.asyncio
async def test_4_stats_consistency(test_app):
    """Memastikan statistik aggregator mencerminkan data sebenarnya"""
    agg, _ = test_app
    await agg.start()

    # 10 event unik + 3 duplikat
    events = [Event(topic="topic1", event_id=f"evt_{i}",
                    timestamp="2025-10-18T10:00:00Z", source="test", payload={})
              for i in range(10)]
    events.extend(events[:3])
    await agg.publish(events)
    await asyncio.sleep(0.5)

    stats = await agg.get_stats()
    assert stats.received == 13
    assert stats.unique_processed == 10
    assert stats.duplicate_dropped == 3
    await agg.stop()

# TEST 5: Filter Event per Topic
@pytest.mark.asyncio
async def test_5_get_events_filtering(test_app):
    """Pastikan /events dapat memfilter berdasarkan topik"""
    agg, _ = test_app
    await agg.start()

    events = [Event(topic="topic_a", event_id=f"evt_a_{i}",
                    timestamp="2025-10-18T10:00:00Z", source="test", payload={})
              for i in range(5)] + \
             [Event(topic="topic_b", event_id=f"evt_b_{i}",
                    timestamp="2025-10-18T10:00:00Z", source="test", payload={})
              for i in range(3)]

    await agg.publish(events)
    await asyncio.sleep(0.5)

    # Filter berdasar topik
    a = await agg.get_events("topic_a")
    b = await agg.get_events("topic_b")
    assert len(a) == 5
    assert len(b) == 3
    await agg.stop()

# TEST 7: Concurrency
@pytest.mark.asyncio
async def test_7_concurrent_publishing(test_app):
    """Uji publish paralel oleh banyak publisher"""
    agg, _ = test_app
    await agg.start()

    async def publisher(src_id: int):
        ev = [Event(topic="concurrent", event_id=f"evt_{src_id}_{i}",
                    timestamp="2025-10-18T10:00:00Z",
                    source=f"src_{src_id}", payload={})
              for i in range(100)]
        await agg.publish(ev)

    # Jalankan 5 publisher bersamaan
    await asyncio.gather(*[publisher(i) for i in range(5)])

    # Tunggu antrean kosong
    while not agg.queue.empty():
        await asyncio.sleep(0.1)
    await asyncio.sleep(0.5)

    assert agg.stats["unique_processed"] == 500
    await agg.stop()

# TEST 8: Idempotency
@pytest.mark.asyncio
async def test_8_idempotency_guarantee(test_app):
    """Pastikan event yang sama tidak diproses berulang"""
    agg, _ = test_app
    await agg.start()

    e = Event(topic="idempotent", event_id="evt_idem_001",
              timestamp="2025-10-18T10:00:00Z", source="test", payload={})

    # Kirim event sama 10x
    for _ in range(10):
        await agg.publish([e])
    await asyncio.sleep(0.5)

    assert agg.stats["unique_processed"] == 1
    assert agg.stats["duplicate_dropped"] == 9
    await agg.stop()

# TEST 9: Perbandingan Publish Batch vs Tunggal
@pytest.mark.asyncio
async def test_9_batch_vs_single_publish(test_app):
    """Bandingkan hasil publish tunggal dan batch"""
    agg, _ = test_app
    await agg.start()

    # Publish tunggal
    single = Event(topic="single", event_id="evt_single",
                   timestamp="2025-10-18T10:00:00Z", source="test", payload={})
    await agg.publish([single])

    # Publish batch
    batch = [Event(topic="batch", event_id=f"evt_batch_{i}",
                   timestamp="2025-10-18T10:00:00Z", source="test", payload={})
             for i in range(10)]
    await agg.publish(batch)
    await asyncio.sleep(0.5)

    assert agg.stats["unique_processed"] == 11
    await agg.stop()