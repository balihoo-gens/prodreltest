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

    def __init__(self, jar):
        self._jar = jar

    def launch(self, classes=None, pipe=False):
        if classes == None or len(classes) < 1:
            classes = self.ALL_CLASSES
        procs = {}
        for path in classes:
            cname = path.split('.')[-1]
            proc = subprocess.Popen(
                ["java", "-cp", self._jar, path],
                #run in the jar dir, config uses relative paths from cwd
                cwd=os.path.dirname(self._jar),
                #if output is piped, it HAS to be consumed to avoid deadlock due to full pipes
                stdout=subprocess.PIPE if pipe else None,
                stderr=subprocess.PIPE if pipe else None,
            )
            procs[cname] = proc
        #return the procs so the caller can do something with them
        return procs


