import asyncio
import os
import random
import time

from worker import CondaEnvWorker


def _simulator_submission():
    """Give ``None`` or a submission id."""
    submission = random.choice(['starting_kit', 'random_forest_10_10', None])
    print(f'The name of the submission generated is {submission}')
    return submission


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
        self._awaiting_worker_queue = asyncio.Queue(loop=self.event_loop,
                                                    maxsize=self.n_worker)
        self._processing_worker_queue = asyncio.LifoQueue(
            loop=self.event_loop, maxsize=self.n_worker)

    async def fetch_from_db(self):
        """Coroutine to fetch submission from the database and create a worker.
        """
        while True:
            generated_submission = _simulator_submission()
            if generated_submission is not None:
                # temporary path to the submissions
                module_path = os.path.dirname(__file__)
                config = {'ramp_kit_dir': os.path.join(
                              module_path, 'kits', 'iris'),
                          'ramp_data_dir': os.path.join(
                              module_path, 'kits', 'iris'),
                          'local_log_folder': os.path.join(
                              module_path, 'kits', 'iris', 'log'),
                          'local_predictions_folder': os.path.join(
                              module_path, 'kits', 'iris', 'predictions'),
                          'conda_env': 'ramp'}
                worker = CondaEnvWorker(config, generated_submission)
                await self._awaiting_worker_queue.put(worker)

    async def launch_worker(self):
        """Coroutine to launch awaiting workers."""
        while True:
            worker = await self._awaiting_worker_queue.get()
            print(f'launch worker {worker}')
            worker.setup()
            await worker.launch_submission()
            await self._processing_worker_queue.put(worker)
            print(f'queue worker {worker}')

    async def collect_result(self):
        """Collect result from processed workers."""
        while True:
            worker = await self._processing_worker_queue.get()
            if worker.status == 'running':
                # await process_queue.put(proc) lock proc.returncode to change
                # status.
                self._processing_worker_queue.put_nowait(worker)
                await asyncio.sleep(0)
            else:
                print(f'collect results of worker {worker}')
                await worker.collect_results()
                worker.teardown()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    scheduler = Scheduler(event_loop=loop, n_worker=5)
    loop.run_until_complete(asyncio.gather(scheduler.fetch_from_db(),
                                           scheduler.launch_worker(),
                                           scheduler.collect_result()))
