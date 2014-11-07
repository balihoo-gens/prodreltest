import boto
import boto.swf.layer2 as swf
from boto.swf.exceptions import SWFTypeAlreadyExistsError, SWFDomainAlreadyExistsError
from threading import Thread, Event
from collections import namedtuple
import time
import json

try:
    import Queue as queue
except ImportError:
    import queue

class UnknownRegionException(Exception):
    """ explicit exception (over keyerror) when a region cannot be resolved """
    pass

class PollInfo(namedtuple('PollInfo', ['stop', 'get'])):
    """ container class for access to async swf task polling """
    pass

class Task(namedtuple('Task', ['params', 'complete', 'fail'])):
    """ wrapper for a SWF task. Attaches complete and fail functions """
    pass

class SwfWorker(swf.ActivityWorker):
    """ Extension of the SWF Activity worker supporting asynchronous polling """

    def __init__(self, region_name, name, domain, version):
        """ construct the worker, and register it """
        region = self.resolve_region(region_name)
        task_list = name + version
        super(SwfWorker, self).__init__(
            region=region,
            domain=domain,
            name=name,
            task_list=task_list,
            version=version
        )
        self.register()

    def resolve_region(self, region_name):
        """ resolves a region object from a region name """
        region_list = boto.swf.regions()
        region_names = [r.name for r in region_list]
        regions = dict(zip(region_names, region_list))
        try:
            return regions[region_name]
        except KeyError:
            raise UnknownRegionException(region_name)

    def register(self):
        """ register a new activity type for this worker
        Ignore the exception if already registered
        """
        at = swf.ActivityType(
            region=self.region,
            domain=self.domain,
            name=self.name,
            version=self.version,
            task_list=self.task_list
        )
        try:
            at.register()
        except SWFTypeAlreadyExistsError:
            pass

    def get_task(self):
        """ poll for a task
        @returns the task if valid, None otherwise
        """
        task = self.poll()
        if 'activityId' in task:
            return task
        return None

    def start_async_polling(self):
        """ starts a thread to poll swf into a queue
        task are wrapped into Task objects
        @returns PollInfo object - contains methods to
            get a task or stop the thread
        """
        e = Event()
        q = queue.Queue()
        complete = self.complete
        fail = self.fail

        def _run():
            """ function to run by the thread
            closes over self.poll and constructs new Task
            objects that close over self.fail and self.complete
            """
            while not e.is_set():
                task = self.poll()
                if 'activityId' in task:
                    token = task['taskToken']
                    q.put(Task(
                        params=json.loads(task['input']),
                        complete=lambda result=None: complete(token, result),
                        fail=lambda details=None: fail(token, details),
                    ))

        def _get ():
            """ local function to return as part of the pollinfo
            to get a task from the queue
            @returns Task object or None
            """
            try:
                return q.get_nowait()
            except queue.Empty:
                return None

        t = Thread(target=_run)
        t. start()
        return PollInfo(stop=e.set, get=_get)

if __name__ == "__main__":
    w = SwfWorker(region_name="us-west-2", domain="fauxfillment", name="launcher", version="1")
    pif = w.start_async_polling()
    start = time.time()
    while time.time() < start + 20:
        task = pif.get()
        if task:
            print("got one: ", task.params)
            task.complete("ya dood")
    pif.stop()
