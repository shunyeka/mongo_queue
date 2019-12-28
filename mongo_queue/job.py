import pymongo
from datetime import datetime, timedelta
import traceback


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
    def last_error(self):
        return self._data["last_error"]

    @property
    def progress_count(self):
        return self._data.get("progress", 0)

    ## Job Control

    def complete(self):
        """Job has been completed.
        """
        return self._queue.collection.delete_one(
            filter={"_id": self.job_id, "locked_by": self._queue.consumer_id})

    def error(self, message=None):
        """Note an error processing a job, and return it to the queue.
        """
        self._data = self._queue.collection.find_one_and_update(
            filter={"_id": self.job_id, "locked_by": self._queue.consumer_id},
            update={"$set": {
                "locked_by": None, "locked_at": None, "last_error": message},
                "$inc": {"attempts": 1}},
            return_document=True)

    def progress(self, count=0):
        """Note progress on a long running task.
        """
        self._data = self._queue.collection.find_one_and_update(
            filter={"_id": self.job_id, "locked_by": self._queue.consumer_id},
            update={"$set": {"progress": count, "locked_at": datetime.now()}},
            return_document=True)

    def release(self):
        """Put the job back into_queue.
        """
        self._data = self._queue.collection.find_one_and_update(
            filter={"_id": self.job_id, "locked_by": self._queue.consumer_id},
            update={"$set": {"locked_by": None, "locked_at": None},
                    "$inc": {"attempts": 1}},
            return_document=True)

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