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

""" container class for access to async swf task polling """
PollInfo = namedtuple('PollInfo', ['thread', 'queue', 'event', 'stop', 'get'])

""" wrapper for a SWF task. Attaches complete and fail functions """
Task = namedtuple('Task', ['params', 'complete', 'fail'])

class SwfWorker(swf.ActivityWorker):
    """ """
    def __init__(self, region_name, name, domain, version):
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
        region_list = boto.swf.regions()
        region_names = [r.name for r in region_list]
        regions = dict(zip(region_names, region_list))
        try:
            return regions[region_name]
        except KeyError:
            raise UnknownRegionException(region_name)

    def register(self):
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
        task = self.poll()
        if 'activityId' in task:
            return task
        return None

    def start_async_polling(self):
        e = Event()
        q = queue.Queue()

        def _run():
            while not e.is_set():
                task = self.poll()
                if 'activityId' in task:
                    token = task['taskToken']
                    q.put(Task(
                        params=json.loads(task['input']),
                        complete=lambda result=None: self.complete(token, result),
                        fail=lambda details=None: self.fail(token, details),
                    ))

        def _stop():
            e.set()

        def _get ():
            try:
                return q.get_nowait()
            except queue.Empty:
                return None

        t = Thread(target=_run)
        t. start()
        return PollInfo(thread=t, queue=q, event=e, stop=_stop, get=_get)

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
