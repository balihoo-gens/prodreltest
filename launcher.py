#!/usr/bin/env python
import sys, os
import subprocess
import argparse
import time
import re

try:
    import Queue as queue
except ImportError:
    import queue

from collections import namedtuple
from threading import Thread

running_local = False
try:
    from splogger import Splogger
except ImportError:
    #path hackery really just for local testing
    # because these are elsewhere on the EC2 instance
    deployment_dir = os.path.join(os.path.dirname(__file__), 'deployment')
    sys.path.append(os.path.join(deployment_dir, 'deployment'))
    sys.path.append(os.path.join(deployment_dir, 'scripts'))
    from splogger import Splogger
    running_local = True

#imports that depend on path changes if local
from swfworker import SwfWorker, Task, PollInfo

Timeouts = namedtuple('Timeouts', ["ping", "quit", "terminate", "kill"])

class Component(object):
    class Responsiveness(object):
        NOT_RUNNING = "not running"
        LAUNCHED = "launched"
        KILLING = "being killed"
        TERMINATING = "terminating"
        QUITTING = "quitting"
        PINGING = "awaiting ping"
        RESPONSIVE = "responsive"

    def __init__(self, jar, classpath, nragent_path=None):
        self._name = classpath.split('.')[-1]
        self._proc = None
        self._launchtime = 0
        self._last_heard_from = 0
        self._waiting = False
        self._pid = None
        self._responsiveness = Component.Responsiveness.NOT_RUNNING
        self._stdout_queue = queue.Queue()
        self._stderr_queue = queue.Queue()
        self._stdout_thread = None
        self._stderr_thread = None
        self._cwd = os.path.dirname(self._jar if not running_local else os.path.realpath(__file__))
        self._cmdline = self._make_cmdline(jar, classpath, nragent_path)

    def __str__(self):
        s = self.name
        if self.pid:
            s += " [%d]" % (self.pid,)
        if self.is_alive():
            s += " launched " + time.asctime(time.gmtime(self._launchtime))
            s += " responsiveness: %s" + self._responsiveness
        return s

    @property
    def name(self):
        return self._name

    @property
    def pid(self):
        return self._pid

    @property
    def launchtime(self):
        return self._launchtime

    @property
    def last_heard_from(self):
        return self._last_heard_from

    @property
    def waiting(self):
        return self._waiting

    @waiting.setter
    def waiting(self, value):
        self._waiting = value

    def _make_cmdline(self, jar, classpath, nragent_path):
        cmdline = ["java"]
        if nragent_path:
            cmdline += ["-javaagent:" + nragent_path]
        return ["-cp", jar, classpath]

    def is_alive(self):
        if self._proc:
            self._retval = self._proc.poll()
            return self._retval is None
        return False

    def launch(self):
        self._proc = subprocess.Popen(
            self._cmdline,
            #run in the jar dir, config uses relative paths from cwd. Unless local, then use the dir this script is in...
            cwd=self._cwd,
            #if output is piped, it HAS to be consumed to avoid deadlock due to full pipes
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize = 1
        )
        self._launchtime = time.time()
        self._last_heard_from = self._launchtime
        self._waiting = False
        self._pid = self._proc.pid
        self._responsiveness = Component.Responsiveness.LAUNCHED
        return self._pid

    def _act_on_proc(self, status, f):
        if self._responsiveness != status:
            if self.is_alive():
                if f: f()
                self._responsiveness = status
                return True
        return False

    def responsive(self):
        return self._act_on_proc(Component.Responsiveness.RESPONSIVE, None)

    def ping(self):
        def f():
            self._proc.stdin.write("ping")
            self._proc.stdin.flush()
        return self._act_on_proc(Component.Responsiveness.PINGING, f)

    def quit(self):
        def f():
            self._proc.stdin.write("quit")
            self._proc.stdin.flush()
        return self._act_on_proc(Component.Responsiveness.QUITTING, f)

    def terminate(self):
        return self._act_on_proc(Component.Responsiveness.TERMINATING, self._proc.terminate)

    def kill(self):
        return self._act_on_proc(Component.Responsiveness.KILLING, self._proc.kill)

    def _setup_out(self, t, q, s):
        if not (t and t.is_alive()):
            if self.is_alive():
                def reader():
                    try:
                        for line in iter(s.readline, b''):
                            q.put(line)
                    except IOError:
                        q.put("IOError")
                t = Thread(target=reader)
                t.start()
                return t
        return None

    def stdout(self):
        t = self._setup_out(
            self._stdout_thread,
            self._stdout_queue,
            self._proc.stdout,
        )
        if not t is None:
            self._stdout_thread = t
        try:
            line = self._stdout_queue.get_nowait()
            self._last_heard_from = time.time()
            yield line
        except queue.Empty:
           pass

    def stderr(self):
        t = self._setup_out(
            self._stderr_thread,
            self._stderr_queue,
            self._proc.stderr,
        )
        if not t is None:
            self._stderr_thread = t
        try:
            line = self._stderr_queue.get_nowait()
            self._last_heard_from = time.time()
            yield line
        except queue.Empty:
           pass

class Launcher(object):
    ALL_CLASSES = {
#        "com.balihoo.fulfillment.deciders.coordinator": True,
#        "com.balihoo.fulfillment.workers.adwords_accountcreator": True,
#        "com.balihoo.fulfillment.workers.adwords_accountlookup": True,
#        "com.balihoo.fulfillment.workers.adwords_adgroupprocessor": True,
#        "com.balihoo.fulfillment.workers.adwords_campaignprocessor": True,
#        "com.balihoo.fulfillment.workers.adwords_imageadprocessor": True,
#        "com.balihoo.fulfillment.workers.adwords_textadprocessor": True,
#        "com.balihoo.fulfillment.workers.geonames_timezoneretriever": True,
        "com.balihoo.fulfillment.workers.htmlrenderer": True,
        "com.balihoo.fulfillment.workers.layoutrenderer": False,
#        "com.balihoo.fulfillment.workers.email_addressverifier": False,
#        "com.balihoo.fulfillment.workers.email_sender": False,
#        "com.balihoo.fulfillment.workers.email_verifiedaddresslister": False,
#        "com.balihoo.fulfillment.workers.facebook_poster": False,
#        "com.balihoo.fulfillment.workers.ftp_uploader": False,
#        "com.balihoo.fulfillment.workers.ftp_uploadvalidator": False,
#        "com.balihoo.fulfillment.workers.rest_client": False,
    }

    def __init__(self, jar, logfile, cfgfile=None, nragent_path=None):
        self._jar = jar
        self._nragent_path = nragent_path
        self._components = {}
        self._log = Splogger(logfile)
        if cfgfile:
            self._task_poller = self._make_task_poller(cfgfile)

    def _make_task_poller(self, cfgfile):
        cfg = self._parse_config(cfgfile)
        w = SwfWorker(region_name=cfg['region'], domain=cfg['domain'], name="launcher", version="1")
        return w.start_async_polling()

    def _parse_config(self, cfgfile):
        cfg = {}
        rx = re.compile("^([\w-]+)\s*=\s*([\w-]+)\s*$")
        with open(cfgfile) as f:
            for line in f:
                mo = rx.match(line)
                if mo:
                    groups = mo.groups()
                    if len(groups) == 2:
                        cfg[groups[0]] = groups[1]
        return cfg

    def log_component(self, component):
        proc_data = {
            "pid" : str(component.pid),
            "procname" : str(component.name)
        }

        for line in component.stdout():
            self._log.info("stdout: %s" % (line,), additional_fields=proc_data)

        for line in component.stderr():
            self._log.error("stderr: %s" % (line,), additional_fields=proc_data)

    def monitor(self, seconds_between_launch, timeouts):
        while True:
            self.handle_tasks()
            for name in self._components:
                component = self._components[name]
                self.log_component(component)
                if component.is_alive():
                    self.check_responsiveness(component, timeouts)
                else:
                    time_since_last_launch = time.time() - component.launchtime
                    if not component.waiting:
                        self._log.error(
                            "died after %f seconds" % (time_since_last_launch),
                            additional_fields={ "pid" : str(component.pid), "procname" : name }
                        )
                    if time_since_last_launch > seconds_between_launch:
                        pid = component.launch()
                        self._log.warn("relaunched", additional_fields={ "pid" : str(pid), "procname" : name })
                    else:
                        component.waiting = True
            time.sleep(0.2)

    def check_responsiveness(self, component, timeouts):
        #time_since_last_heard_from
        tlhf = time.time() - component.last_heard_from
        if tlhf > timeouts.ping:
            proc_data = {
                "pid" : str(component.pid),
                "procname" : str(component.name)
            }
            if tlhf > timeouts.kill:
                #not even responding to terminate. Well, you asked for it: death is imminent
                if component.kill():
                    self._log.error("no response for %f seconds: kill" % (tlhf), additional_fields=proc_data)
            elif tlhf > timeouts.terminate:
                #reluctant to quit. I'll do it for you.
                if component.terminate():
                    self._log.error("no response for %f seconds: terminate" % (tlhf), additional_fields=proc_data)
            elif tlhf > timeouts.quit:
                #haven't heard from you despite pings, asking you to quit yourself
                if component.quit():
                    self._log.warn("no response for %f seconds: quit" % (tlhf), additional_fields=proc_data)
            else:
                #haven't heard from you in a while, just checking in
                if component.ping():
                    self._log.info("no response for %f seconds: ping" % (tlhf), additional_fields=proc_data)
        else:
            component.responsive()

    def handle_tasks(self):
        if self._task_poller:
            task = self._task_poller.get()
            if task:
                try:
                    class_name = str(task.params["classname"])
                    component = self.launch_new_component(class_name)
                    if not component:
                        raise Exception("unable to launch %s" % (class_name,))
                    task.complete(str(component.pid))
                except Exception as e:
                    self._log.error("failed to launch component from swf task: %s" % (e.message,))
                    task.fail(e.message)

    def resolve_class_name(self, class_name):
        """ try to find the class_name in the list and return the properly
            qualified name if matched. If the name cannot be found, it may
            refer to a main that is not in the default list, so try to run
            it anyway
        """
        for path in self.ALL_CLASSES:
            if path.find(class_name) > -1:
                return path
        return class_name

    def launch_new_component(self, class_name, nragent_path=None):
        if class_name not in self.ALL_CLASSES:
            class_name = self.resolve_class_name(class_name)
        component = Component(self._jar, class_name, nragent_path)
        name = component.name
        try:
            pid = component.launch()
            self._log.info("Launched", additional_fields={ "pid" : str(pid), "procname" : name })
            self._components[name] = component
            return component
        except:
            self._log.error("Unable to launch", additional_fields={ "jar": self._jar, "procname" : name })
            return None
        finally:
            self._log.info("Managing %d processes" % (len(self._components),))

    def launch(self, classes=None):
        if classes == None or len(classes) < 1:
            #select all the enabled classes if none are provided
            classes = [c for c in self.ALL_CLASSES if self.ALL_CLASSES[c]]

        for class_name in classes:
            self.launch_new_component(class_name, self._nragent_path)
            time.sleep(5)

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Launch the Fulfillment application")
    thisdir = os.path.dirname(os.path.realpath(__file__))
    jar = os.path.join(thisdir, "fulfillment.jar" if not running_local else "target/scala-2.10/fulfillment-assembly-1.0-SNAPSHOT.jar")

    parser.add_argument('classes', metavar='C', type=str, nargs='*', help='classes to run')
    parser.add_argument('-j','--jarname', help='the path of the jar to run from', default=jar)
    parser.add_argument('-l','--logfile', help='the log file', default='/var/log/balihoo/fulfillment/launcher.log')
    parser.add_argument('-d','--launchdelay', help='minumum number of seconds between launch of the same process', default='600')
    parser.add_argument('-p','--ping', help='number of seconds after which to ping a quiet process', default='90')
    parser.add_argument('-q','--quit', help='number of seconds after which to tell a process to quit', default='300')
    parser.add_argument('-t','--terminate', help='number of seconds after which to terminate (SIGTERM) a quiet process', default='600')
    parser.add_argument('-k','--kill', help='number of seconds after which to kill (SIGKILL) a quiet process', default='900')
    parser.add_argument('-c','--config', help='path to the swf config file', default='config/aws.properties.private')
    parser.add_argument('--newrelicagent', help='path to the newrelic agent to use for monitoring', default="/opt/balihoo/newrelic-agent.jar")
    parser.add_argument('--nonewrelic', help='disable newrelic agent', action="store_true", default=False)
    parser.add_argument('--noworker', help='disable swf worker', action="store_true", default=False)

    args = parser.parse_args()

    nragent_path = None
    if not args.nonewrelic:
        nragent_path = args.newrelicagent

    config_file = None
    if not args.noworker:
        config_file = args.config

    launcher = Launcher(args.jarname, args.logfile, config_file, nragent_path)
    launcher.launch(args.classes)
    timeouts = Timeouts(
        ping=int(args.ping),
        quit=int(args.quit),
        terminate=int(args.terminate),
        kill=int(args.kill),
    )
    launcher.monitor(int(args.launchdelay), timeouts)


