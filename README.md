# mongo_queue
Task queue built on mongo with channels

[Website](http://www.shunyeka.com) • [autobotAI Cloud Governance](https://autobot.live/)

Inspired from [kapilt/mongoqueue](https://github.com/kapilt/mongoqueue)

## Usage

Install the package.

```
pip install mongo_queue
```

###  Usage Example:

*  Create Queue Object
```python
from mongo_queue.queue import Queue
from pymongo import MongoClient

queue = Queue(MongoClient('localhost', 27017).task_queue, consumer_id="consumer-1", timeout=300, max_attempts=3)
```
* Add task to queue default channel

```python
queue.put({"task_id": 1})
```

* Add task to queue with priority to default channel

```python
queue.put({"task_id": 1}, priority=1)
```

* Add task to queue in a specific channel

```python
queue.put({"task_id": 1}, priority=1, channel="channel_1")
```

* Get the next job to be executed from the default channel

```python
job = queue.next()
```

* Get the next job to be executed from a specific channel

```python
job = queue.next(channel="channel_1")
```

* Update job progress for long running jobs

```python
job.progress(count=10)
```

* Put the job back in queue, this will be picked up again later, this will update attempts after max attempts the job will not be picked up again.

```python
job.release()
```

* Put the job back in queue with error, this will be picked up again later, this will update attempts after max attempts the job will not be picked up again.

```python
job.error("Some error occured")
```

* Complete the job. This will delete job from the database.

```python
job.complete()
```
