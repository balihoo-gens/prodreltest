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
        self._procs = {}
        self._log = Splogger(logfile)

    def plog(self, p):
        line = p.stdout.readline()
        while len(line) > 0:
            self._log.info("pid %d stdout %s" % (p.pid, line))
            line = p.stdout.readline()

        line = p.stderr.readline()
        while len(line) > 0:
            self._log.error("pid %d stderr %s" % (p.pid, line))
            line = p.stderr.readline()

    def monitor(self):
        start = time.time()
        done = []
        proc_count = len(self._procs)
        self._log.info("Monitoring %d processes" % (proc_count,))
        while len(done) < proc_count:
            for procname in self._procs:
                proc = self._procs[procname]
                if proc.pid not in done:
                    retval = proc.poll()
                    if not retval is None:
                        elapsed = time.time() - start
                        self._log.error("%s [pid %d] died within %f seconds, returncode %d" % (procname, proc.pid, elapsed, retval))
                        self.plog(proc)
                        done.append(proc.pid)
            time.sleep(0.2)
        self._log.info("Done: no processes left to monitor")

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

    def launch(self, classes=None, pipe=False):
        if classes == None or len(classes) < 1:
            classes = self.ALL_CLASSES
        else:
            classes = [self.resolve_classname(classname) for classname in classes]

        for path in classes:
            procname = path.split('.')[-1]
            cwd = os.path.dirname(self._jar if not running_local else os.path.realpath(__file__))
            cmdline = ["java", "-cp", self._jar, path]
            self._log.debug("Launching %s: '%s' in dir '%s'" % (procname, cmdline, cwd))
            proc = subprocess.Popen(
                cmdline,
                #run in the jar dir, config uses relative paths from cwd. Unless local, then use the dir this script is in...
                cwd=cwd,
                #if output is piped, it HAS to be consumed to avoid deadlock due to full pipes
                stdout=subprocess.PIPE if pipe else None,
                stderr=subprocess.PIPE if pipe else None,
            )
            self._procs[procname] = proc
            s = "Launched %s with pid %d" % (procname, proc.pid)
            self._log.info(s)

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Launch the Fulfillment application")
    thisdir = os.path.dirname(os.path.realpath(__file__))
    jar = os.path.join(thisdir, "fulfillment.jar" if not running_local else "target/scala-2.10/fulfillment-assembly-1.0-SNAPSHOT.jar")

    parser.add_argument('classes', metavar='C', type=str, nargs='*', help='classes to run')
    parser.add_argument('-j','--jarname', help='the path of the jar to run from', default=jar)
    parser.add_argument('-l','--logfile', help='the log file', default='/var/log/balihoo/fulfillment/launcher.log')

    args = parser.parse_args()

    launcher = Launcher(args.jarname, args.logfile)
    launcher.launch(args.classes, True)
    launcher.monitor()


