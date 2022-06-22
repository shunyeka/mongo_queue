from time import sleep
from mongo_queue.queue import Queue
from pymongo import MongoClient
from bson import ObjectId

conn_str = "mongodb+srv://username:pwd@mongourl/test?retryWrites=true&w=majority"

client = MongoClient(conn_str)
db = client.queue_db
col = db.test_queue_col

queue = Queue(col, consumer_id="consumer-1", timeout=300, max_attempts=3)

job_id_1 = queue.put({"task_id": 1}, priority=1)

job = queue.next()
job.release(sleep=15, state={"test": "asdfasf"})
job = queue.next()
print("after release", job)
sleep(17)
job = queue.next()
print("after sleep", job)
#
# while job := queue.next():
#     print(job)
#     job.complete()
#




