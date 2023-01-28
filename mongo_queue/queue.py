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
                    "$lt": datetime.now() - timedelta(seconds=self.timeout)}},
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

    def next(self, channel="default", recursion_limit: int = 10, current_level: int = 0):
        if current_level > recursion_limit:
            return None
        aggregate_result = list(self.collection.aggregate([
            {'$match': {'locked_by': None, 'locked_at': None,
                        "channel": channel,
                        "attempts": {"$lt": self.max_attempts},
                        "$or": [{"run_after": { "$exists": False }}, { "run_after" : {"$lt": datetime.now()}}]
            }},
            {"$lookup":
                {
                    "from": self.collection.name,
                    "localField": "depends_on",
                    "foreignField": "_id",
                    "as": "dependencies"
            }},
            {"$addFields": {
                "dependencies": {"$size": "$dependencies"}
            }},    
            {"$match": {"dependencies": {"$eq": 0}}},
            {"$sort": {'priority': pymongo.DESCENDING, "queued_at": pymongo.ASCENDING}},
            {"$limit": 1}
        ]))
        if not aggregate_result:
            return None
        next_job = self.collection.find_one_and_update(
            filter={"_id": aggregate_result[0]["_id"], 'locked_by': None, 'locked_at': None},
            update={"$set": {"locked_by": self.consumer_id,
                             "locked_at": datetime.now()}},
            sort=[('priority', pymongo.DESCENDING)],
            return_document=ReturnDocument.AFTER
        )
        if not next_job:
            return self.next(channel=channel, recursion_limit=recursion_limit, current_level=current_level+1)
        else:
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
