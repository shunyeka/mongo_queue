import pymongo
from datetime import datetime, timedelta
import traceback
from pymongo import ReturnDocument
from datetime import datetime, timedelta
import time


class Job:

    def __init__(self, queue, data):
        """
        """
        self._queue = queue
        self._data = data
    
    @property
    def payload(self):
        return self._data['payload']

    @property
    def job_id(self):
        return self._data["_id"]

    @property
    def priority(self):
        return self._data["priority"]

    @property
    def attempts(self):
        return self._data["attempts"]

    @property
    def locked_by(self):
        return self._data["locked_by"]

    @property
    def locked_at(self):
        return self._data["locked_at"]

    @property
    def queued_at(self):
        return self._data["queued_at"]

    @property
    def depends_on(self):
        return self._data["depends_on"]

    @property
    def last_error(self):
        return self._data["last_error"]
    
    @property
    def state(self):
        return self._data.get("state")

    @property
    def run_after(self):
        return self._data.get("run_after")

    @property
    def progress_count(self):
        return self._data.get("progress", 0)

    ## Job Controller

    def complete(self):
        """Job has been completed.
        """
        # remove the dependency from other jobs.
        deleted_job = self._queue.collection.delete_one(
            filter={"_id": self.job_id, "locked_by": self._queue.consumer_id})
        
        def remove_from_dependencies(tries=0):
            if tries >= 3:
                return
            try:
                self._queue.collection.update_many({"depends_on": self.job_id},
                                 {"$pull": {"depends_on": self.job_id}})
            except pymongo.errors.WriteError as e:
                time.sleep(0.5*tries)
                remove_from_dependencies(tries=tries+1)
        remove_from_dependencies()
        return deleted_job

    def error(self, message=None):
        """Note an error processing a job, and return it to the queue.
        """
        self._data = self._queue.collection.find_one_and_update(
            filter={"_id": self.job_id, "locked_by": self._queue.consumer_id},
            update={"$set": {
                "locked_by": None, "locked_at": None, "last_error": message},
                "$inc": {"attempts": 1}},
            return_document=ReturnDocument.AFTER)

    def progress(self, count=0):
        """Note progress on a long running task.
        """
        self._data = self._queue.collection.find_one_and_update(
            filter={"_id": self.job_id, "locked_by": self._queue.consumer_id},
            update={"$set": {"progress": count, "locked_at": datetime.now()}},
            return_document=ReturnDocument.AFTER)

    def release(self, sleep=0, state=None, inc_attempt=True):
        """Put the job back into_queue.
        """
        now = datetime.now()
        now_plus_seconds = now + timedelta(seconds = sleep)        
        attempt_increment_by = 1 if inc_attempt else 0
        self._data = self._queue.collection.find_one_and_update(
            filter={"_id": self.job_id, "locked_by": self._queue.consumer_id},
            update={"$set": {"locked_by": None, "locked_at": None, "run_after": now_plus_seconds, "state": state},
                    "$inc": {"attempts": attempt_increment_by}},
            return_document=ReturnDocument.AFTER)

    def __str__(self):
        return str(self._data)

    ## Context Manager support

    def __enter__(self):
        return self._data

    def __exit__(self, type, value, tb):
        if (type, value, tb) == (None, None, None):
            self.complete()
        else:
            error = traceback.format_exc()
            self.error(error)