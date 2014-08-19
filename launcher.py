import sys, os
import subprocess

class Launcher(object):
    ALL_CLASSES = [
        'com.balihoo.fulfillment.workers.adwords_accountcreator',
        'com.balihoo.fulfillment.workers.adwords_accountlookup',
        'com.balihoo.fulfillment.workers.adwords_adgroupprocessor',
        'com.balihoo.fulfillment.workers.adwords_campaignprocessor',
        'com.balihoo.fulfillment.workers.adwords_imageadprocessor',
        'com.balihoo.fulfillment.workers.chaosworker',
        'com.balihoo.fulfillment.workers.email_addressverifier',
        'com.balihoo.fulfillment.workers.email_sender',
        'com.balihoo.fulfillment.workers.email_verifiedaddresslister',
        'com.balihoo.fulfillment.workers.noopworker',
        'com.balihoo.fulfillment.workers.timezoneworker',
        'com.balihoo.fulfillment.deciders.coordinator'
    ]

    def __init__(self, jar, splogger=None):
        self._jar = jar
        self._splogger = splogger

    def launch(self, classes = ALL_CLASSES):
        procs = {}
        for path in classes:
            cname = path.split('.')[-1]
            proc = subprocess.Popen(
                ["java", "-cp", self._jar, path],
                #these should log properly to files themselves
                # if the output is to be splogged here, threads and nonblocking io are required.
                # stdout=subprocess.PIPE,
                # stderr=subprocess.PIPE
            )
            procs[proc.pid] = proc
            s = "Launched %s with pid %d" % (cname, proc.pid)
            if self._splogger:
                self._splogger.info(s)
            else:
                print(s)
        return procs


