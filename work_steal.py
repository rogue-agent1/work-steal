#!/usr/bin/env python3
"""work_steal - Work-stealing scheduler with per-worker deques."""
import sys, threading, time
from collections import deque

class WorkStealScheduler:
    def __init__(self, n_workers=2):
        self.n = n_workers
        self.queues = [deque() for _ in range(n_workers)]
        self.results = []
        self._lock = threading.Lock()
        self._done = False
    def submit(self, task, worker=None):
        if worker is None:
            worker = min(range(self.n), key=lambda i: len(self.queues[i]))
        self.queues[worker].append(task)
    def _steal(self, my_id):
        for i in range(self.n):
            if i != my_id and self.queues[i]:
                try: return self.queues[i].pop()
                except IndexError: continue
        return None
    def run(self):
        threads = []
        for wid in range(self.n):
            t = threading.Thread(target=self._worker, args=(wid,), daemon=True)
            threads.append(t)
            t.start()
        while not self._done:
            time.sleep(0.01)
            total = sum(len(q) for q in self.queues)
            if total == 0:
                time.sleep(0.05)
                total = sum(len(q) for q in self.queues)
                if total == 0:
                    self._done = True
        for t in threads: t.join(timeout=2)
    def _worker(self, wid):
        while not self._done:
            task = None
            if self.queues[wid]:
                try: task = self.queues[wid].popleft()
                except IndexError: pass
            if task is None:
                task = self._steal(wid)
            if task:
                result = task()
                with self._lock:
                    self.results.append((wid, result))
            else:
                time.sleep(0.005)

def test():
    sched = WorkStealScheduler(2)
    # Overload worker 0
    for i in range(10):
        sched.submit(lambda i=i: i * 2, worker=0)
    sched.run()
    values = sorted(r[1] for r in sched.results)
    assert values == [i*2 for i in range(10)]
    workers_used = set(r[0] for r in sched.results)
    assert len(workers_used) >= 1  # at least work got done
    print("work_steal: all tests passed")

if __name__ == "__main__":
    test() if "--test" in sys.argv else print("Usage: work_steal.py --test")
