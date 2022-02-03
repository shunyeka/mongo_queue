from mongo_queue.queue import Queue
from pymongo import MongoClient
from bson import ObjectId

conn_str = "mongodb+srv://username:pwd@mongourl/test?retryWrites=true&w=majority"

client = MongoClient(conn_str)
db = client.queue_db
col = db.test_queue_col

queue = Queue(col, consumer_id="consumer-1", timeout=300, max_attempts=3)

job_id_1 = queue.put({"task_id": 1}, priority=1)
job_id_0 = queue.put({"task_id": 0}, priority=1)
job_id_2 = queue.put({"task_id": 2}, priority=1, depends_on=[job_id_1])
job_id_3 = queue.put({"task_id": 3}, priority=1, depends_on=[job_id_0, job_id_1])
job = queue.next()
job.complete()
#
# while job := queue.next():
#     print(job)
#     job.complete()
#




