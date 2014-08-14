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

    def __init__(self, jar, log_filename):
        self._jar = jar

    def launch(self, classes = ALL_CLASSES):
        procs = {}
        for path in classes:
            cname = path.split('.')[-1]
            proc = subprocess.Popen(
                ["java", "-cp", self._jar, path],
                #these should log properly to files themselves
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            procs[proc.pid] = proc
            print("Running %s: %d" % (cname, proc.pid))
        return procs


