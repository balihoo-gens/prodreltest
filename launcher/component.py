import subprocess
import time
import os
from threading import Thread
try:
    import Queue as queue
except ImportError:
    import queue


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
        self._cmdline = self._make_cmdline(jar, classpath, nragent_path)

        self._cwd = os.path.dirname(jar)
        #configs should be next to the jar, unless running local,
        #  then they should be in the project root, above this pkg
        if not os.path.exists(os.path.join(self._cwd, "config")):
            self._cwd = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')

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
        return cmdline + ["-cp", jar, classpath]

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


