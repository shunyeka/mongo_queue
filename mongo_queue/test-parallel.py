from unittest import TestCase
import os
import time

from datetime import datetime

import pymongo

from mongo_queue import Queue
from mongo_queue.lock import MongoLock, lock
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import traceback

class QueueTest(TestCase):

    def setUp(self):
        self.client = pymongo.MongoClient(os.environ.get("MONGO_URI"), retryWrites=False, maxPoolSize=1, directConnection=True)
        self.db = self.client.test_queue
        self.queue = Queue(self.db.queue_1, "consumer_1")

    def tearDown(self):
        # self.client.drop_database("test_queue")
        pass


    # def test010_produce(self):
    #     for x in range(100000):
    #         data = {"context_id": "pratham",
    #             "data": [x],
    #             "more-data": time.time()}
    #         self.queue.put(dict(data))
    
    def test020_consume(self):
        def consume_and_act(job):
            if job:
                a = random.randint(1,10)
                if a == 7:
                    start = time.time()
                    job.release()
                    end = time.time()      
                    print(f"Releasing job, {job.job_id}, {end-start}")                    
                elif a == 8:           
                    start = time.time()
                    job.error("dummy failure")
                    end = time.time()                               
                    print(f"Erroring job, {job.job_id}, {end-start}")
                elif a == 9:           
                    start = time.time()         
                    job.progress(10)
                    end = time.time()  
                    print(f"Progressing job, {job.job_id}, {end-start}")
                else:                  
                    start = time.time()  
                    job.complete()
                    end = time.time()  
                    print(f"Completing job, {job.job_id}, {end-start}")
                return job.job_id
            
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []                        
            while self.queue.size() > 1:
                start = time.time()
                job = self.queue.next()
                end = time.time()                
                if job:
                    print(f"Getting Next job took, {end-start}")
                    futures.append(executor.submit(consume_and_act, job))
            for future in as_completed(futures):
                try:
                    result = future.result()
                    print(result)
                except Exception as exc:
                    traceback.print_exc()                
