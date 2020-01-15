import pymongo
from datetime import datetime, timedelta
from mongo_queue.job import Job
from uuid import uuid4
from pymongo import errors

DEFAULT_INSERT = {
    "attempts": 0,
    "locked_by": None,
    "locked_at": None,
    "last_error": None
}


class Queue:
    def __init__(self, collection, consumer_id, timeout=300, max_attempts=3):
        """
        """
        self.collection = collection
        self.consumer_id = consumer_id
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.ensure_indexes()

    def ensure_indexes(self):
        """
        "locked_by": None,
                   "locked_at": None,
                   "channel": channel,
                   "attempts"
        """
        next_index = pymongo.IndexModel([("locked_by", pymongo.ASCENDING), ("locked_at", pymongo.ASCENDING),
                                         ("channel", pymongo.ASCENDING),
                                         ("attempts", pymongo.ASCENDING)], name="next_index")
        update_index = pymongo.IndexModel([("_id", pymongo.ASCENDING),
                                           ("locked_by", pymongo.ASCENDING)], name="update_index")
        unique_index = pymongo.IndexModel([("job_id", pymongo.ASCENDING),
                                           ("channel", pymongo.ASCENDING)], name="unique_index", unique=True)
        self.collection.create_indexes([next_index, update_index, unique_index])

    def close(self):
        """Close the in memory queue connection.
        """
        self.collection.connection.close()

    def clear(self):
        """Clear the queue.
        """
        return self.collection.drop()

    def size(self):
        """Total size of the queue
        """
        return self.collection.count_documents(filter={})

    def repair(self):
        """Clear out stale locks.
        Increments per job attempt counter.
        """
        self.collection.find_one_and_update(
            filter={
                "locked_by": {"$ne": None},
                "locked_at": {
                    "$lt": datetime.now() - timedelta(self.timeout)}},
            update={
                "$set": {"locked_by": None, "locked_at": None},
                "$inc": {"attempts": 1}}
        )

    def drop_max_attempts(self):
        """
        """
        self.collection.update_many( filter={},
            update={"attempts": {"$gte": self.max_attempts}},
            remove=True)

    def put(self, payload, priority=0, channel="default", job_id=None):
        """Place a job into the queue
        """
        job = dict(DEFAULT_INSERT)
        job['priority'] = priority
        job['payload'] = payload
        job['channel'] = channel
        job['job_id'] = job_id or str(uuid4())
        try:
            return self.collection.insert_one(job)
        except errors.DuplicateKeyError as e:
            return False

    def next(self, channel="default"):
        return self._wrap_one(self.collection.find_one_and_update(
            filter={"locked_by": None,
                   "locked_at": None,
                   "channel": channel,
                   "attempts": {"$lt": self.max_attempts}},
            update={"$set": {"locked_by": self.consumer_id,
                             "locked_at": datetime.now()}},
            sort=[('priority', pymongo.DESCENDING)],
        ))

    def _jobs(self):
        return self.collection.find(
            query={"locked_by": None,
                   "locked_at": None,
                   "attempts": {"$lt": self.max_attempts}},
            sort=[('priority', pymongo.DESCENDING)],
        )

    def _wrap_one(self, data):
        return data and Job(self, data) or None

    def stats(self):
        """Get statistics on the queue.
        Use sparingly requires a collection lock.
        """

        js = """function queue_stat(){
        return db.eval(
        function(){
           var a = db.%(collection)s.count(
               {'locked_by': null,
                'attempts': {$lt: %(max_attempts)i}});
           var l = db.%(collection)s.count({'locked_by': /.*/});
           var e = db.%(collection)s.count(
               {'attempts': {$gte: %(max_attempts)i}});
           var t = db.%(collection)s.count();
           return [a, l, e, t];
           })}""" % {
            "collection": self.collection.name,
            "max_attempts": self.max_attempts}

        return dict(zip(
            ["available", "locked", "errors", "total"],
            self.collection.database.eval(js)))
