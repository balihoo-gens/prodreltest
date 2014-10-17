#!/usr/bin/env python
import sys, os
import subprocess
import argparse
import time

running_local = False
try:
    from splogger import Splogger
except ImportError:
    #path hackery really just for local testing
    # because these are elsewhere on the EC2 instance
    sys.path.append(os.path.join(os.path.dirname(__file__), 'deployment', 'deployment'))
    from splogger import Splogger
    running_local = True

class Component(object):
    def __init__(self, jar, classpath):
        self._name = classpath.split('.')[-1]
        self._jar = jar
        self._classpath = classpath
        self._proc = None
        self._launchtime = None
        self._waiting = False
        self._pid = None

    def __str__(self):
        s = self.name
        if self.pid:
            s += " [%d]" % (self.pid,)
        if self.is_alive():
            s += " launched " + time.asctime(time.gmtime(self._launchtime))
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
    def waiting(self):
        return self._waiting

    @waiting.setter
    def waiting(self, value):
        self._waiting = value

    def is_alive(self):
        if self._proc:
            self._retval = self._proc.poll()
            return self._retval is None
        return False

    def launch(self):
        cwd = os.path.dirname(self._jar if not running_local else os.path.realpath(__file__))
        cmdline = ["java", "-cp", self._jar, self._classpath]
        self._proc = subprocess.Popen(
            cmdline,
            #run in the jar dir, config uses relative paths from cwd. Unless local, then use the dir this script is in...
            cwd=cwd,
            #if output is piped, it HAS to be consumed to avoid deadlock due to full pipes
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self._launchtime = time.time()
        self._waiting = False
        self._pid = self._proc.pid
        return self._pid

    def stdout(self):
        if self._proc:
            line = self._proc.stdout.readline()
            while len(line) > 0:
                yield line
                line = self._proc.stdout.readline()

    def stderr(self):
        if self._proc:
            line = self._proc.stderr.readline()
            while len(line) > 0:
                yield line
                line = self._proc.stderr.readline()

class Launcher(object):
    ALL_CLASSES = [
        "com.balihoo.fulfillment.deciders.coordinator",
        "com.balihoo.fulfillment.workers.adwords_accountcreator",
        "com.balihoo.fulfillment.workers.adwords_accountlookup",
        "com.balihoo.fulfillment.workers.adwords_adgroupprocessor",
        "com.balihoo.fulfillment.workers.adwords_campaignprocessor",
        "com.balihoo.fulfillment.workers.adwords_imageadprocessor",
        "com.balihoo.fulfillment.workers.adwords_textadprocessor",
        "com.balihoo.fulfillment.workers.email_addressverifier",
        "com.balihoo.fulfillment.workers.email_sender",
        "com.balihoo.fulfillment.workers.email_verifiedaddresslister",
        "com.balihoo.fulfillment.workers.facebook_poster",
        "com.balihoo.fulfillment.workers.ftp_uploader",
        "com.balihoo.fulfillment.workers.ftp_uploadvalidator",
        "com.balihoo.fulfillment.workers.geonames_timezoneretriever",
        "com.balihoo.fulfillment.workers.htmlrenderer",
        "com.balihoo.fulfillment.workers.layoutrenderer",
        "com.balihoo.fulfillment.workers.rest_client",
    ]

    def __init__(self, jar, logfile):
        self._jar = jar
        self._components = {}
        self._log = Splogger(logfile)

    def log_component(self, component):
        proc_data = {
            "pid" : str(component.pid),
            "procname" : str(component.name)
        }

        for line in component.stdout():
            self._log.info("stdout: %s" % (line,), additional_fields=proc_data)

        for line in component.stderr():
            self._log.error("stderr: %s" % (line,), additional_fields=proc_data)

    def monitor(self, seconds_between_launch):
        count = len(self._components)
        self._log.info("Monitoring %d components" % (count,))
        while True:
            for name in self._components:
                component = self._components[name]
                self.log_component(component)
                if not component.is_alive():
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

    def resolve_classname(self, classname):
        """ try to find the classname in the list and return the properly
            qualified name if matched. If the name cannot be found, it may
            refer to a main that is not in the default list, so try to run
            it anyway
        """
        for path in self.ALL_CLASSES:
            if path.find(classname) > -1:
                return path
        return classname

    def launch(self, classes=None):
        if classes == None or len(classes) < 1:
            classes = self.ALL_CLASSES
        else:
            classes = [self.resolve_classname(classname) for classname in classes]

        for path in classes:
            component = Component(self._jar, path)
            name = component.name
            pid = component.launch()
            self._log.info("Launched", additional_fields={ "pid" : str(pid), "procname" : name })
            self._components[name] = component

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Launch the Fulfillment application")
    thisdir = os.path.dirname(os.path.realpath(__file__))
    jar = os.path.join(thisdir, "fulfillment.jar" if not running_local else "target/scala-2.10/fulfillment-assembly-1.0-SNAPSHOT.jar")

    parser.add_argument('classes', metavar='C', type=str, nargs='*', help='classes to run')
    parser.add_argument('-j','--jarname', help='the path of the jar to run from', default=jar)
    parser.add_argument('-l','--logfile', help='the log file', default='/var/log/balihoo/fulfillment/launcher.log')
    parser.add_argument('-s','--seconds', help='minumum number of seconds between launch of the same process', default='600')

    args = parser.parse_args()

    launcher = Launcher(args.jarname, args.logfile)
    launcher.launch(args.classes)
    launcher.monitor(int(args.seconds))


