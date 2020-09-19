"""
Throttler limits how many requests can issue given a specific time window
Copied from https://github.com/hallazzang/asyncio-throttle
"""
import asyncio
import contextlib
import time
from typing import AsyncIterator, Deque, Set, Callable
from uuid import uuid4, UUID

# class Throttler:
#     """
#     Throttler
#     """

#     req_per_window: int
#     window: float
#     retry_interval: float

#     def __init__(
#         self, req_per_window: int, window: float = 1.0, retry_interval: float = 0.01
#     ):
#         """
#         Create a throttler.
#         """
#         self.req_per_window = req_per_window
#         self.window = window
#         self.retry_interval = retry_interval

#         self._task_logs: Deque[float] = deque()

#     def _flush(self) -> None:
#         now = time.time()
#         while self._task_logs:
#             if now - self._task_logs[0] > self.window:
#                 self._task_logs.popleft()
#             else:
#                 break

#     async def _acquire(self) -> None:
#         while True:
#             self._flush()
#             if len(self._task_logs) < self.req_per_window:
#                 break
#             await asyncio.sleep(self.retry_interval)

#         self._task_logs.append(time.time())

#     def release(self) -> None:
#         self._task_logs.pop()

#     async def __aenter__(self) -> None:
#         await self._acquire()

#     async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
#         pass


class Throttler:
    """
    Throttler, but also keeps request in order by
    requiring them a seq number
    """

    _req_per_window: int
    _window: float
    _retry_interval: float
    _backoff_n: int = 0
    _task_logs: Deque[float]
    _running_tasks: Set[UUID]

    def __init__(
        self, req_per_window: int, window: float = 1.0, retry_interval: float = 0.01
    ):
        """Create a throttler."""
        self._req_per_window = req_per_window
        self._window = window
        self._retry_interval = retry_interval

        self._task_logs = Deque()
        self._running_tasks = set()

    def flush(self) -> None:
        """Clear tasks that are out of the window."""
        now = time.time()
        while self._task_logs:
            if now - self._task_logs[0] > self._window:
                self._task_logs.popleft()
            else:
                break

    def ntasks_in_window(self) -> int:
        """How many tasks are in current window."""
        return len(self._task_logs) + len(self._running_tasks)

    def running(self, task_id: UUID) -> None:
        """Add a running task."""
        self._running_tasks.add(task_id)

    def finish(self, task_id: UUID, cancel: bool = False) -> None:
        """Finish a running task.
        This removes the task from the running queue and
        add the finished time to the task log."""

        self._running_tasks.remove(task_id)
        if not cancel:
            self._task_logs.append(time.time())  # append the finish time

    def ordered(self) -> "OrderedThrottleSession":
        """returns an ordered throttler session"""
        return OrderedThrottleSession(self)

    @property
    def retry_interval(self) -> int:
        return self._retry_interval

    @property
    def req_per_window(self) -> int:
        return max(self._req_per_window - 2 ** self._backoff_n, 1)

    def backoff(self) -> None:
        self._backoff_n += 1


class OrderedThrottleSession:  # pylint: disable=protected-access
    """OrderedThrottleSession share a same rate throttler but
    can have independent sequence numbers."""

    thr: Throttler
    seqs: Set[int]

    def __init__(self, thr: Throttler) -> None:
        self.thr = thr
        self.seqs = set()

    @contextlib.asynccontextmanager
    async def acquire(self, i: int) -> AsyncIterator[Callable[[], None]]:
        """Wait for the request being allowed to send out,
        without violating the # reqs/sec constraint and the order constraint."""
        while (
            self.thr.ntasks_in_window() >= self.thr.req_per_window
            or self.next_seq() != i
        ):
            await asyncio.sleep(self.thr.retry_interval)
            self.thr.flush()

        self.seqs.add(i)
        task_id = uuid4()
        self.thr.running(task_id)
        cancelled = False

        def cancel() -> None:
            nonlocal cancelled
            cancelled = True
            self.seqs.remove(i)

        yield cancel

        self.thr.finish(task_id, cancelled)

    def next_seq(self) -> int:
        if not self.seqs:
            return 0

        for i in range(max(self.seqs) + 2):
            if i not in self.seqs:
                return i
        raise RuntimeError("Unreachable")

    def backoff(self) -> None:
        self.thr.backoff()

    @property
    def req_per_window(self) -> int:
        return self.thr.req_per_window
