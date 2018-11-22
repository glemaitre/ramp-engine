import asyncio
import os
import random
import time

from worker import LocalWorker


def _simulator_submission():
    """Give ``None`` or a submission id."""
    return random.choice(['starting_kit', 'random_forest_10_10', None])


class Scheduler:
    """Scheduler to fetch submission, launch submission, and collect results.

    Parameters
    ----------
    event_loop : asyncio loop,
        The event loop to which the queue will be subscribed.
    n_worker : int,
        The number of workers which can be launch concurrently.
    """

    def __init__(self, event_loop=None, n_worker=1):
        self.event_loop = event_loop
        self.n_worker = n_worker
        self._worker_queue = asyncio.Queue(loop=self.event_loop,
                                           maxsize=self.n_worker)
        self._process_queue = asyncio.LifoQueue(loop=self.event_loop,
                                                maxsize=self.n_worker)

    async def fetch_from_db(self):
        """Coroutine to fetch submission from the database and create a worker.
        """
        while True:
            generated_submission = _simulator_submission()
            if generated_submission is not None:
                # temporary path to the submissions
                module_path = os.path.dirname(__file__)
                ramp_kit_dir = os.path.join(module_path, 'kits', 'iris')
                ramp_data_dir = ramp_kit_dir
                worker = LocalWorker(conda_env='ramp',
                                     submission=generated_submission,
                                     ramp_data_dir=ramp_data_dir,
                                     ramp_kit_dir=ramp_kit_dir)
                await self._worker_queue.put(worker)

    async def launch_worker(self):
        """Coroutine to launch awaiting workers."""
        while True:
            worker = await self._worker_queue.get()
            print(f'launch the worker {worker}')
            worker.setup()
            proc = await worker.launch_submission()
            await self._process_queue.put((proc, worker))
            print(f'Queuing the process {proc}')

    async def collect_result(self):
        """Collect result from processed workers."""
        while True:
            proc, worker = await self._process_queue.get()
            if proc.returncode is None:
                # await process_queue.put(proc) lock proc.returncode to change
                # status.
                self._process_queue.put_nowait((proc, worker))
                await asyncio.sleep(0)
            else:
                log, _ = await proc.communicate()
                print(f'collect the log of the submission {proc}')
                print(log)
                # print(worker.collect_submission())


if __name__ == "__main__":
    # import os
    # from worker import LocalWorker
    # module_path = os.path.dirname(__file__)
    # ramp_kit_dir = os.path.join(module_path, 'kits', 'iris')
    # ramp_data_dir = ramp_kit_dir
    # worker = LocalWorker(conda_env='ramp')
    # worker.setup()
    # worker.launch_submission()
    # print(worker.collect_submission())
    loop = asyncio.get_event_loop()
    scheduler = Scheduler(event_loop=loop, n_worker=5)
    loop.run_until_complete(asyncio.gather(scheduler.fetch_from_db(),
                                           scheduler.launch_worker(),
                                           scheduler.collect_result()))
    print('loop completed')
