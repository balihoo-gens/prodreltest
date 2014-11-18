#!/usr/bin/env python
import sys, os
import argparse
import time
import re

from collections import namedtuple

running_local = False
try:
    from splogger import Splogger
except ImportError:
    #path hackery really just for local testing
    # because these are elsewhere on the EC2 instance
    deployment_dir = os.path.join(os.path.dirname(__file__), '..', 'deployment')
    sys.path.append(deployment_dir)
    from deployment.splogger import Splogger
    running_local = True

#imports that depend on path changes if local
from swfworker import SwfWorker, Task, PollInfo
from component import Component

#container class for timeout values
Timeouts = namedtuple('Timeouts', ["ping", "quit", "terminate", "kill"])

class Launcher(object):
    """ Launcher both launches and monitors components.
    Class names are accepted command line or from SWF
    Monitored processes are restarted when dead and terminated
    when unresponsive
    """

    ALL_CLASSES = {
        "com.balihoo.fulfillment.deciders.coordinator": True,
        "com.balihoo.fulfillment.workers.adwords_accountcreator": True,
        "com.balihoo.fulfillment.workers.adwords_accountlookup": True,
        "com.balihoo.fulfillment.workers.adwords_adgroupprocessor": True,
        "com.balihoo.fulfillment.workers.adwords_campaignprocessor": True,
        "com.balihoo.fulfillment.workers.adwords_imageadprocessor": True,
        "com.balihoo.fulfillment.workers.adwords_textadprocessor": True,
        "com.balihoo.fulfillment.workers.geonames_timezoneretriever": True,
        "com.balihoo.fulfillment.workers.htmlrenderer": True,
        "com.balihoo.fulfillment.workers.layoutrenderer": False,
        "com.balihoo.fulfillment.workers.email_addressverifier": False,
        "com.balihoo.fulfillment.workers.email_sender": False,
        "com.balihoo.fulfillment.workers.email_verifiedaddresslister": False,
        "com.balihoo.fulfillment.workers.facebook_poster": False,
        "com.balihoo.fulfillment.workers.ftp_uploader": False,
        "com.balihoo.fulfillment.workers.ftp_uploadvalidator": False,
        "com.balihoo.fulfillment.workers.rest_client": True,
        "com.balihoo.fulfillment.workers.benchmark": False,
        "com.balihoo.fulfillment.workers.sendgrid_lookupsubaccount": False,
        "com.balihoo.fulfillment.dashboard.dashboard": False,
    }

    def __init__(self, jar, logfile, cfgfile=None, nragent_path=None):
        """ constructs the launcher. There is commonly just one (per jar anyway)
        @param jar string - the path to the jar file to use
        @param logfile string - path to the logfile used by Splogger
        @param cfgfile string - path to the config file used to set up the SwfWorker
                                if ommitted, no SwfWorker is created
        @param nragent_path - the option new relic agent passed on the java cmdline
        """
        self._jar = jar
        self._nragent_path = nragent_path
        self._components = {}
        self._log = Splogger(logfile)
        if cfgfile:
            self._task_poller = self._make_task_poller(cfgfile)

    def _make_task_poller(self, cfgfile):
        """ creates an async Swf task poller
        @param cfgfile string - path to config file
        @returns PollInfo object - contains tasks received async from SWF
        """
        cfg = self._parse_config(cfgfile)
        w = SwfWorker(region_name=cfg['region'], domain=cfg['domain'], name="launcher", version="1")
        return w.start_async_polling()

    def _parse_config(self, cfgfile):
        """ parse the aws config file for SWF settings
        @param cfgfile string - path to config file
        @returns dictionary containing config values
        """
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
        """ reads any stdout and stderr from the component
        and log it along with pid and procname information
        """
        proc_data = {
            "pid" : str(component.pid),
            "procname" : str(component.name)
        }

        for line in component.stdout():
            self._log.info("stdout: %s" % (line,), additional_fields=proc_data)

        for line in component.stderr():
            self._log.error("stderr: %s" % (line,), additional_fields=proc_data)

    def monitor(self, seconds_between_launch, timeouts):
        """ endless loop to monitor, terminate or restart components
        Also looks for SWF tasks to come in
        @param seconds_between_launch integer - minimum number of seconds between
               launch of the same process. This is the time from the last launch
               time, not the time it terminated.
        @param timeouts Timeouts object - container with the different timeout
               values to monitor
        """
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
        """ checks to see if a component has been responsive, and if not take
        appropriate action based on the specified timeouts
        @param component Component object - the component to check
        @param timeouts Timeouts object - container with the different timeout
               values to monitor
        """
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
        """ handles any SWF tasks that may have come in.
        Do nothing if there is no task_poller defined
        """
        if self._task_poller:
            task = self._task_poller.get()
            if task:
                try:
                    class_name = str(task.params["classname"])
                    component = self.launch_new_component(class_name, self._nragent_path)
                    if not component:
                        raise Exception("unable to launch %s" % (class_name,))
                    task.complete(str(component.pid))
                except Exception as e:
                    self._log.error("failed to launch component from swf task: %s" % (e.message,))
                    try:
                        task.fail(e.message)
                    except Exception as e:
                        self._log.error("failed to fail swf task: %s" % (e.message,))

    def resolve_class_name(self, class_name):
        """ try to find the class_name in the list and return the properly
        qualified name if matched. If the name cannot be found, it may
        refer to a main that is not in the default list, so try to run
        it anyway
        @param class_name string: part or all of a classname
        @returns string: the fully qualified classname, or the input if not found
        """
        for path in self.ALL_CLASSES:
            if path.find(class_name) > -1:
                return path
        return class_name

    def launch_new_component(self, class_name, nragent_path=None):
        """ start up a brand new component. This can be of the same class as an existing one
        @param class_name string: part or all of a classname
        @param nragent_path - the option new relic agent passed on the java cmdline
        @returns Component object or None - if successful, the launched component
        """
        if class_name not in self.ALL_CLASSES:
            class_name = self.resolve_class_name(class_name)
        component = Component(self._jar, class_name, nragent_path)
        name = component.name
        try:
            pid = component.launch()
            self._log.info("Launched", additional_fields={ "pid" : str(pid), "procname" : name })
            self._components[name] = component
            return component
        except Exception as e:
            self._log.error(
                "Unable to launch: %s" % (str(e),),
                additional_fields={ "jar": self._jar, "procname" : name }
            )
            return None
        finally:
            self._log.info("Managing %d processes" % (len(self._components),))

    def launch(self, classes=None):
        """ launch a each class provided, or the default set if none are provided
        @params classes list of strings or None - the list of class names with a main 
            that can be launched in the jar
        """
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
    parser.add_argument('-p','--ping', help='number of seconds after which to ping a quiet process', default='300')
    parser.add_argument('-q','--quit', help='number of seconds after which to tell a process to quit', default='600')
    parser.add_argument('-t','--terminate', help='number of seconds after which to terminate (SIGTERM) a quiet process', default='900')
    parser.add_argument('-k','--kill', help='number of seconds after which to kill (SIGKILL) a quiet process', default='1200')
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


