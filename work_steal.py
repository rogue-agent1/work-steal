#!/usr/bin/env python3
"""work_steal - Work-stealing thread pool scheduler."""
import sys, collections, threading, random, time

class WorkStealingPool:
    def __init__(self, num_workers=4):
        self.num_workers = num_workers
        self.queues = [collections.deque() for _ in range(num_workers)]
        self.results = {}
        self.lock = threading.Lock()
        self.task_id = 0
        self._running = False
        self.workers = []
        self.completed = 0

    def submit(self, fn, *args):
        with self.lock:
            tid = self.task_id
            self.task_id += 1
            worker_idx = tid % self.num_workers
        self.queues[worker_idx].append((tid, fn, args))
        return tid

    def _steal(self, my_idx):
        for i in range(self.num_workers):
            if i != my_idx and self.queues[i]:
                try:
                    return self.queues[i].pop()
                except IndexError:
                    continue
        return None

    def _worker(self, idx):
        while self._running:
            task = None
            if self.queues[idx]:
                try:
                    task = self.queues[idx].popleft()
                except IndexError:
                    pass
            if task is None:
                task = self._steal(idx)
            if task is None:
                time.sleep(0.001)
                continue
            tid, fn, args = task
            result = fn(*args)
            with self.lock:
                self.results[tid] = result
                self.completed += 1

    def start(self):
        self._running = True
        self.workers = [threading.Thread(target=self._worker, args=(i,), daemon=True)
                       for i in range(self.num_workers)]
        for w in self.workers:
            w.start()

    def stop(self):
        self._running = False
        for w in self.workers:
            w.join(timeout=1)

    def get_result(self, tid, timeout=5):
        end = time.time() + timeout
        while time.time() < end:
            with self.lock:
                if tid in self.results:
                    return self.results[tid]
            time.sleep(0.001)
        return None

def test():
    pool = WorkStealingPool(num_workers=2)
    pool.start()
    def square(x):
        return x * x
    tids = [pool.submit(square, i) for i in range(10)]
    results = []
    for tid in tids:
        r = pool.get_result(tid)
        results.append(r)
    pool.stop()
    for i in range(10):
        assert results[i] == i * i, f"Expected {i*i}, got {results[i]}"
    assert pool.completed == 10
    pool2 = WorkStealingPool(num_workers=4)
    pool2.start()
    tid = pool2.submit(lambda: 42)
    assert pool2.get_result(tid) == 42
    pool2.stop()
    print("All tests passed!")

if __name__ == "__main__":
    test() if "--test" in sys.argv else print("work_steal: Work-stealing pool. Use --test")
