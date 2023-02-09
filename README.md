# mongo_queue
Task queue built on mongo with channels and unique job id.

[Website](http://www.shunyeka.com) • [autobotAI Automation Platform](https://autobot.live/)

Inspired from [kapilt/mongoqueue](https://github.com/kapilt/mongoqueue)

### Change Log:

#### v0.1.5

- Fixes issue with the next job method, it was picking up jobs with dependency

#### v0.1.4

- Improved the job.next method. Removed the lookup and removed double operation.

#### v0.1.3

- Added dependency index for faster lookup. Update `complete` method to retry 3 times while pulling the dependencies.
- Corrected pull dependency query by adding filter. It was updating all the documents.

#### v0.1.2

- Added diskUsage for larger queue dependency resolution.

#### v0.1.1

- Added find_and_update for finding the next job added process to pick the next job if the previous is already locked with multiple retries..

#### v0.1.0

- Added optional inc_attempt parameter for job.release. This will allow user to choose if they want to increment the attempt when releasing a job.

#### v0.0.9

- Added method find_by_id to find a job by it's id.

#### v0.0.7

- Added mongo backward compatibility. The aggregate function was using lookup which is only available after Mongo 3.6 (Not avaialble in the DocumentDB), Modified lookup to use old syntax.

#### v0.0.6

- Added sleep and state feature while releasing a job. This provides a way to not pickup job until provided seconds and store state for long running jobs.

#### v0.0.5

- Added depends_on feature. You can create dependency between jobs by supplying depends_on[] with previously created job ids. 

#### v0.0.3

-  Added unique index with job_id and channel. This is to make sure that the same job is not added multiple times. If not job id provided an unique id generated by default. 

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

* Add task to queue with unique job_id

```python
queue.put({"task_id": 1}, priority=1, channel="channel_1", job_id="x_job")
```

* Add task with dependency

```python
job1 = queue.put({"task_id": 1}, priority=1, channel="channel_1", job_id="x_job")
job2 = queue.put({"task_id": 2}, priority=1, channel="channel_1", job_id="x_job", depends_on=[job1])
```

* Get the next job to be executed from the default channel

```python
job = queue.next()
```

* Get the next job to be executed from a specific channel

```python
job = queue.next(channel="channel_1")
```

* Update job progress for long-running jobs

```python
job.progress(count=10)
```

* Put the job back in queue, this will be picked up again later, this will update attempts after max attempts the job will not be picked up again.
* You can also set state and sleep while releaseing a job
* `sleep` in seconds. The job will not be picked up again till the sleep time expires.
* `state` you can store state in the job for long running jobs.

```python
job.release()
# or
job.release(sleep=10, state={"some": "state"})
```

* Put the job back in queue with error, this will be picked up again later, this will update attempts after max attempts the job will not be picked up again.

```python
job.error("Some error occured")
```

* Complete the job. This will delete job from the database.

```python
job.complete()
```


## Build Steps

```bash
# Setup venv of python version 3.6 and above
python3.9 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install wheel
pip install --upgrade twine
rm -rf dist
python3 setup.py sdist bdist_wheel
python3 -m twine upload --repository-url https://upload.pypi.org/legacy/ dist/*
```

# Local Development and Testing

```
export MONGO_URI=mongodb+srv://username:pwd@mongourl/test?retryWrites=true&w=majority
cd mong_queue # Root directory of the package
python3.9 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 -m unittest mongo_queue.test
```