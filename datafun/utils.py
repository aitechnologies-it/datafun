import time
from tqdm import tqdm
import atexit

class ProgressBar():
    def __init__(self, update_interval_sec: float = 0.5):
        self.update_interval_sec = update_interval_sec
        self.pbar = None
        self.n_read_total = 0
        self.n_processed_from_last_update = 0
        self.last_start_time = float('inf')
        self.do_update_pbar_processed = True
        self.do_update_pbar_total = False

    @staticmethod
    def compact_number(num):
        if num < 10**3:
            magnitude = ''
            s = str(num)
        elif num < 10**6:
            magnitude = 'K'
            s = f"{num / 10**3:.1f}"
        elif num < 10**9:
            magnitude = 'M'
            s = f"{num / 10**6:.1f}"
        elif num < 10**12:
            magnitude = 'B'
            s = f"{num / 10**9:.1f}"
        else:
            magnitude = 'T'
            s = f"{num / 10**12:.1f}"
        return f"{s}{magnitude}"

    def _init_state(self):
        self.n_read_total = 0
        self.n_processed_from_last_update = 0
        self.last_start_time = time.time()

    def _set_doupdate_booleans(self):
        now = time.time()
        if now - self.last_start_time >= self.update_interval_sec:
            self.do_update_pbar_processed = True
            self.do_update_pbar_total = True
            self.last_start_time = now

    def create(self, msg:str):
        self._init_state()
        self.pbar = tqdm(desc=msg, position=0, postfix="total_read=0")
        atexit.register(self.close)

    def update_read(self):
        self._set_doupdate_booleans()
        self.n_read_total += 1
        if self.pbar is not None and self.do_update_pbar_total:
            self.pbar.set_postfix(total_read=self.compact_number(self.n_read_total), refresh=True)
            self.do_update_pbar_total = False

    def update_pbar(self):
        self.n_processed_from_last_update += 1
        if self.pbar is not None and self.do_update_pbar_processed:
            self.pbar.update(self.n_processed_from_last_update)
            self.n_processed_from_last_update = 0
            self.do_update_pbar_processed = False

    def close(self):
        if self.pbar is not None:
            # manually update counters in pbar
            self.pbar.set_postfix(total_read=self.compact_number(self.n_read_total), refresh=True)
            self.pbar.update(self.n_processed_from_last_update)
            # close
            self.pbar.close()
            self.pbar = None
            atexit.unregister(self.close)
