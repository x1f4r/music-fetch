import threading
import time

from melody_index.net import SlidingWindowLimiter


def test_limiter_caps_rate_across_threads(tmp_path):
    lim = SlidingWindowLimiter(tmp_path / "l", limit=10, window=0.5)
    stamps: list[float] = []
    lock = threading.Lock()

    def worker():
        for _ in range(8):
            lim.acquire()
            with lock:
                stamps.append(time.time())

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stamps.sort()
    assert len(stamps) == 32
    for i in range(len(stamps) - 10):
        assert stamps[i + 10] - stamps[i] >= 0.5 - 0.02


def test_pause_blocks_everyone(tmp_path):
    lim = SlidingWindowLimiter(tmp_path / "l", limit=100, window=1.0)
    lim.pause(0.3)
    t = time.time()
    lim.acquire()
    assert time.time() - t >= 0.25
