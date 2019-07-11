import mock_db
import uuid
from worker import worker_main
from threading import Thread, Lock
import time
import sys

mutex = Lock()

def lock_is_free():
    """
        CHANGE ME, POSSIBLY MY ARGS

        Return whether the lock is free

        Args:
            db: an instance of MockDB
    """
    
    return mutex.acquire()


def attempt_run_worker(worker_hash, give_up_after, db, retry_interval):
    """
        CHANGE MY IMPLEMENTATION, BUT NOT FUNCTION SIGNATURE

        Run the worker from worker.py by calling worker_main

        Args:
            worker_hash: a random string we will use as an id for the running worker
            give_up_after: if the worker has not run after this many seconds, give up
            db: an instance of MockDB
            retry_interval: continually poll the locking system after this many seconds
                            until the lock is free, unless we have been trying for more
                            than give_up_after seconds
    """
    
    runtime = 0
    while runtime < give_up_after:
        if lock_is_free():
            try:
                worker_main(worker_hash, db)
                break
            except Exception:
                pass
            finally:
                mutex.release()

        runtime+=retry_interval
        time.sleep(retry_interval)

if __name__ == "__main__":
    """
        DO NOT MODIFY

        Main function that runs the worker five times, each on a new thread
        We have provided hard-coded values for how often the worker should retry
        grabbing lock and when it should give up. Use these as you see fit, but
        you should not need to change them
    """

    db = mock_db.DB()
    threads = []
    for _ in range(5):
        t = Thread(target=attempt_run_worker, args=(uuid.uuid1(), 2000, db, 5))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
