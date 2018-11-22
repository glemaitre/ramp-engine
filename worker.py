import asyncio
import os
import time

from base import BaseWorker


class LocalWorker(BaseWorker):
    def __init__(self, conda_env='base', submission='starting_kit',
                 ramp_kit_dir='.', ramp_data_dir='.', logger=None):
        super().__init__(conda_env=conda_env,
                         submission=submission,
                         ramp_kit_dir=ramp_kit_dir,
                         ramp_data_dir=ramp_data_dir,
                         logger=logger)
        # self._log = {}

    def setup(self):
        self.status = 'setup'
        return super().setup()

    def teardown(self):
        return super().teardown()

    def _is_training_finished(self):
        return False if self._process.returncode is None else True

    async def launch_submission(self):
        if self.status == 'running':
            raise ValueError('Wait that the submission is processed before to '
                             'launch a new one.')
        cmd_ramp = os.path.join(self._python_bin_path,
                                'ramp_test_submission')
        cmd_ramp = (f'{cmd_ramp} '
                    f'--submission {self.submission} '
                    f'--ramp_kit_dir {self.ramp_kit_dir} '
                    f'--ramp_data_dir {self.ramp_data_dir} '
                    f'--save-y-preds')
        self._process = await asyncio.subprocess.create_subprocess_shell(
            cmd_ramp,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )
        self.status = 'running'
        return self._process

    async def collect_submission(self):
        pass
        # log, _ = await self._process.communicate()
        # return log
