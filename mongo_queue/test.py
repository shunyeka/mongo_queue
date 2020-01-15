import os
import time

from datetime import datetime

import pymongo
from unittest import TestCase

from mongo_queue import Queue
from mongo_queue.lock import MongoLock, lock


class MongoLockTest(TestCase):

    def setUp(self):

        self.client = pymongo.MongoClient('localhost', 27017)
        self.db = self.client.test_queue
        self.collection = self.db.locks

    def tearDown(self):
        self.client.drop_database("test_queue")

    def test_lock_acquire_release_context_manager(self):
        with lock(self.collection, 'test1') as l:
            self.assertTrue(l.locked)
        self.assertEqual(self.collection.count_documents(filter={}), 0)

    def test_auto_expires_old(self):
        lock = MongoLock(self.collection, 'test2', lease=2)
        self.assertTrue(lock.acquire())

        time.sleep(2.2)
        # Reports truthfully it doesn't have the local anymore,
        # using ttl time, stale db record extant.
        self.assertFalse(lock.locked)

        lock2 = MongoLock(self.collection, 'test2')
        # New lock acquire will take ownership based on old ttl\
        self.assertTrue(lock2.acquire())

        # Releasing the original doesn't change ownership
        self.assertTrue(lock.release())
        records = list(self.collection.find())
        self.assertEqual(len(records), 1)

        self.assertEqual(records.pop()['client_id'], lock2.client_id)
        self.assertFalse(lock.acquire(wait=False))

        lock2.release()
        self.assertFalse(list(self.collection.find()))


class QueueTest(TestCase):

    def setUp(self):
        self.client = pymongo.MongoClient('localhost', 27017)
        self.db = self.client.test_queue
        self.queue = Queue(self.db.queue_1, "consumer_1")

    def tearDown(self):
        self.client.drop_database("test_queue")

    def assert_job_equal(self, job, data):
        for k, v in data.items():
            self.assertEqual(job.payload[k], v)

    def test_put_default_channel_next(self):
        data = {"context_id": "alpha",
                "data": [1, 2, 3],
                "more-data": time.time()}
        self.queue.put(dict(data))
        job = self.queue.next()
        self.assert_job_equal(job, data)

    def test_put_custom_channel_next(self):
        data = {"context_id": "alpha",
                "data": [1, 2, 3],
                "more-data": time.time()}
        self.queue.put(dict(data), channel="channle_1", priority=2)
        self.queue.put(dict(data), channel="channle_2", priority=1)
        job = self.queue.next(channel="channle_1")
        self.assert_job_equal(job, data)

    def test_get_empty_queue(self):
        job = self.queue.next()
        self.assertEqual(job, None)

    def test_priority(self):
        self.queue.put({"name": "alice"}, priority=1)
        self.queue.put({"name": "bob"}, priority=2)
        self.queue.put({"name": "mike"}, priority=0)

        self.assertEqual(
            ["bob", "alice", "mike"],
            [self.queue.next().payload['name'],
             self.queue.next().payload['name'],
             self.queue.next().payload['name']])

    def test_complete(self):
        data = {"context_id": "alpha",
                "data": [1, 2, 3],
                "more-data": datetime.now()}

        self.queue.put(data)
        self.assertEqual(self.queue.size(), 1)
        job = self.queue.next()
        job.complete()
        self.assertEqual(self.queue.size(), 0)

    def test_release(self):
        data = {"context_id": "alpha",
                "data": [1, 2, 3],
                "more-data": time.time()}

        self.queue.put(data)
        job = self.queue.next()
        job.release()
        self.assertEqual(self.queue.size(), 1)
        job = self.queue.next()
        self.assert_job_equal(job, data)

    def test_attempts(self):
        data = {"context_id": "alpha",
                "data": [1, 2, 3],
                "more-data": time.time()}

        self.queue.put(data, channel="attempt_test")
        job = self.queue.next(channel="attempt_test")
        job.release()
        job = self.queue.next(channel="attempt_test")
        job.release()
        job = self.queue.next(channel="attempt_test")
        self.assertEqual(job.attempts, 2)

    def test_error(self):
        data = {"context_id": "alpha",
                "data": [1, 2, 3],
                "more-data": time.time()}

        self.queue.put(data)
        job = self.queue.next()
        job.error("Test Error")
        self.assertEqual(job.last_error, "Test Error")

    def test_ensure_indexes(self):
        indexes = []
        for index in self.queue.collection.list_indexes():
            indexes.append(index.to_dict()["name"])
        required_indexes = ["next_index", "update_index"]
        for index in required_indexes:
            self.assertIn(index, indexes)

    def test_progress(self):
        data = {"context_id": "alpha",
                "data": [1, 2, 3],
                "more-data": time.time()}

        self.queue.put(data)
        job = self.queue.next()
        job.progress(10)
        self.assertEqual(job.progress_count, 10)

    def test_unique_job(self):
        data = {"context_id": "alpha",
                "data": [1, 2, 3],
                "more-data": time.time()}

        self.queue.put(data, job_id=1)
        job2 = self.queue.put({"context_id": "beta",
                "data": [1, 2, 3],
                "more-data": time.time()}, job_id=1)
        self.assertEqual(job2, False)
        self.assert_job_equal(self.queue.next(), data)
