import mock_db
import uuid
from worker import worker_main
from threading import Thread
import time
import sys

def lock_is_free(db, worker_hash):
    """
        CHANGE ME, POSSIBLY MY ARGS

        Return whether the lock is free

        Args:
            db: an instance of MockDB
    """
    smallest_time = sys.maxsize
    matching_hash = ''
    for obj in db.find_many({}):
        if obj.get('time') < smallest_time:
            smallest_time = obj.get('time')
            matching_hash = obj.get('_id')
    if worker_hash == matching_hash:
        return True

    return False


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

    try:
        db.insert_one({"_id": worker_hash, 'time': time.time()})
    except Exception:
        db.update_one({"_id": worker_hash}, {'time': time.time()})
    
    runtime = 0
    while runtime < give_up_after:
        if lock_is_free(db, worker_hash):
            try:
                worker_main(worker_hash, db)
                db.delete_one({"_id":worker_hash})
                break
            except Exception:
                pass
                

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
