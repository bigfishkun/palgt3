#  -*- coding: utf-8 -*-

from database import *
from datetime import *

"""
Schema:
{
    'startTime': date
  , 'endTime': date
  , 'createdOn': date
  , 'priority': number
  , 'payload': object
}

link:http://learnmongodbthehardway.com/schema/queues/
"""


class Queue():
    def __init__(self):
        self.queue = DB_CONN['queue']

    def insertJob(self, job, param, priority=1):
        self.queue.insert({
            'startTime': None
            , 'endTime': None
            , 'createdOn': datetime.now()
            , 'priority': priority
            , 'payload': {
                'job': job
                , 'param': param
            }
        })

    def fetchNextFIFOJob(self):
        job = self.queue.find_and_modify(
            query={"startTime": None}
            , sort={"createdOn": 1}
            , update={"$set": {"startTime": datetime.now()}}
            , new=True
        )
        return job

    def fetchNextHighestPriorityJob(self):
        pass

    def finishJob(self, job):
        self.queue.update({"_id": job.get("_id")}, {"$set": {"endTime": datetime.now()}})

if __name__ == "__main__":
    queue = Queue()
    queue.insertJob("daily", {"code":"600000", "start": '2017-01-01', "end": '2018-12-31'})

    job = queue.fetchNextFIFOJob()
    print(job)

    queue.finishJob(job)
    job = queue.fetchNextFIFOJob()
    print(job)
