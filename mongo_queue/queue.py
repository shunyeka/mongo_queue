import pymongo
from datetime import datetime, timedelta
from mongo_queue.job import Job
from uuid import uuid4
from pymongo import errors, ReturnDocument
from bson import ObjectId

DEFAULT_INSERT = {
    "attempts": 0,
    "locked_by": None,
    "locked_at": None,
    "last_error": None,
    "depends_on": []
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

    def __str__(self):
        return str({
            "consumer_id": self.consumer_id,
            "timeout": self.timeout,
            "max_attempts": self.max_attempts,
            "collection": self.collection
        })

    def ensure_indexes(self):
        """
        "locked_by": None,
                   "locked_at": None,
                   "channel": channel,
                   "attempts"
        """
        next_index = pymongo.IndexModel([("locked_by", pymongo.ASCENDING), ("locked_at", pymongo.ASCENDING),
                                         ("queued_at", pymongo.ASCENDING),
                                         ("channel", pymongo.ASCENDING),
                                         ("attempts", pymongo.ASCENDING),
                                         ("depends_on", pymongo.ASCENDING)], name="next_index")
        update_index = pymongo.IndexModel([("_id", pymongo.ASCENDING),
                                           ("locked_by", pymongo.ASCENDING)], name="update_index")
        depend_index = pymongo.IndexModel([("depends_on", pymongo.ASCENDING)], name="depend_index", sparse=True)
        unique_index = pymongo.IndexModel([("job_id", pymongo.ASCENDING),
                                           ("channel", pymongo.ASCENDING)], name="unique_index", unique=True)
        repair_index = pymongo.IndexModel([("locked_by", pymongo.ASCENDING),
                                           ("locked_at", pymongo.ASCENDING)], name="repair_index", unique=False)
        self.collection.create_indexes([next_index, update_index, unique_index, depend_index, repair_index])

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
        self.collection.update_many(
            filter={
                "locked_by": {"$ne": None},
                "locked_at": {"$lt": datetime.now() - timedelta(seconds=self.timeout)}},
            update={
                "$set": {"locked_by": None, "locked_at": None},
                "$inc": {"attempts": 1}}
        )
        # TODO: Find the jobs with dependencies and the job it depends on does not exist.

    def drop_max_attempts(self):
        """
        """
        self.collection.update_many(filter={},
                                    update={"attempts": {"$gte": self.max_attempts}},
                                    remove=True)

    def put(self, payload, priority=0, channel="default", job_id=None, depends_on=[]):
        """Place a job into the queue
        """
        depends_on_bson = Queue._depends_on_bson(depends_on)
        job = dict(DEFAULT_INSERT)
        job['priority'] = priority
        job['payload'] = payload
        job['channel'] = channel
        job['job_id'] = job_id or str(uuid4())
        job['depends_on'] = depends_on_bson
        job['queued_at'] = datetime.now()
        try:
            return self.collection.insert_one(job).inserted_id
        except errors.DuplicateKeyError as e:
            return False

    def running_count(self, channel="default"):
        return self.collection.count_documents(filter={'locked_by': {"$ne": None}, 'locked_at': {"$ne": None},
                                                                  "channel": channel,
                                                                  "attempts": {"$lt": self.max_attempts}
                                                                  })

    def _pending_filter(self, channel):
        return {'locked_by': None, 
                'locked_at': None,
                "channel": channel,
                "attempts": {"$lt": self.max_attempts},
                "$and": [
                    {"$or": [{"depends_on": {"$exists": False}, "depends_on": {"$size": 0}}]},
                    {"$or": [{"run_after": {"$exists": False}}, {"run_after": {"$lt": datetime.now()}}]}
                ]
                }

    def pending_count(self, channel="default"):
        return self.collection.count_documents(filter=self._pending_filter(channel=channel))

    def next(self, channel="default"):
        next_job = self.collection.find_one_and_update(
            filter=self._pending_filter(channel=channel),
            update={"$set": {"locked_by": self.consumer_id,
                             "locked_at": datetime.now()}},
            sort=[('priority', pymongo.DESCENDING), ("queued_at", pymongo.ASCENDING)],
            return_document=ReturnDocument.AFTER
        )
        if next_job:
            next_job = self._wrap_one(next_job)
        return next_job

    def find_job_by_id(self, _id):
        if not _id:
            raise AttributeError("id required.")
        if not isinstance(_id, ObjectId):
            _id = ObjectId(_id)
        return self._wrap_one(self.collection.find_one(filter={"_id": _id}))

    def _jobs(self):
        return self.collection.find(
            query={"locked_by": None,
                   "locked_at": None,
                   "attempts": {"$lt": self.max_attempts}},
            sort=[('priority', pymongo.DESCENDING)],
        )

    def _wrap_one(self, data):
        return data and Job(self, data) or None

    @staticmethod
    def _depends_on_bson(depends_on):
        if not depends_on:
            return depends_on
        depends_on_bson = []
        for item in depends_on:
            if isinstance(item, ObjectId):
                depends_on_bson.append(item)
            elif isinstance(item, str):
                depends_on_bson.append(ObjectId(item))
            else:
                print("Unsupported dependency", item)
        return depends_on_bson

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
