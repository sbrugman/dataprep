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

    req_per_window: int
    window: float
    retry_interval: float
    _task_logs: Deque[float]
    _running_tasks: Set[UUID]

    def __init__(
        self, req_per_window: int, window: float = 1.0, retry_interval: float = 0.01
    ):
        """Create a throttler."""
        self.req_per_window = req_per_window
        self.window = window
        self.retry_interval = retry_interval

        self._task_logs = Deque()
        self._running_tasks = set()

    def flush(self) -> None:
        """Clear tasks that are out of the window."""
        now = time.time()
        while self._task_logs:
            if now - self._task_logs[0] > self.window:
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


class OrderedThrottleSession:  # pylint: disable=protected-access
    """OrderedThrottleSession share a same rate throttler but
    can have independent sequence numbers."""

    thr: Throttler
    seq: int = -1
    cancelled: Set[UUID]

    def __init__(self, thr: Throttler) -> None:
        self.thr = thr
        self.cancelled = set()

    @contextlib.asynccontextmanager
    async def acquire(self, i: int) -> AsyncIterator[Callable[[], None]]:
        """Wait for the request being allowed to send out,
        without violating the # reqs/sec constraint and the order constraint."""
        if self.seq >= i:
            raise RuntimeError(f"{i} already acquired")

        while (
            self.thr.ntasks_in_window() >= self.thr.req_per_window or self.seq != i - 1
        ):
            await asyncio.sleep(self.thr.retry_interval)
            self.thr.flush()

        self.seq = i
        task_id = uuid4()
        self.thr.running(task_id)

        yield lambda: self.cancelled.add(task_id)

        self.thr.finish(task_id, task_id in self.cancelled)
