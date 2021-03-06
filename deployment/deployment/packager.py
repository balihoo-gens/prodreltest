#!/usr/bin/env python
import sys, os, stat
import subprocess
import shutil
import datetime
from splogger import Splogger

class Packager:
    """ Packages up everything needed for deployment
        into a directory, ready to be sync-ed to S3
    """
    def __init__(self, rootdir, unattended=False, log_filename="/var/log/balihoo/fulfillment/packup.log",debug=False):
        self._rootdir = rootdir
        self._log = Splogger(log_filename, component="packager")
        self._unattended = unattended
        self._debug = debug

    def info(self,msg):
        print(msg)
        with self._log.increased_indirection():
            self._log.info(msg)

    def error(self,msg):
        print("ERROR: " + msg)
        with self._log.increased_indirection():
            self._log.error(msg)

    def assemble_fat_jar(self):
        jarname = os.path.join(self._rootdir, "target/scala-2.10/fulfillment-assembly-1.0-SNAPSHOT.jar")
        proc = subprocess.Popen(
            ["sbt", "assembly"],
            cwd=self._rootdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        #block and accumulate output
        out, err = proc.communicate()
        if len(out) > 0:
            self._log.info(out)
        if len(err) > 0:
            self._log.error(out)
        if (proc.returncode != 0):
            raise Exception("fat jar creation failed. See %s" % (self._log.filename(),))
        return jarname

    def package(self, env):
        """ returns the path of the dir to be
            sync-ed to the S3 bucket
        """
        tmpdir = os.path.join(self._rootdir, "deployments", "%s_%s" % (env, datetime.datetime.now().strftime("%Y%m%d_%Hh%Mm%Ss%f")))
        if os.path.isdir(tmpdir):
            shutil.rmtree(tmpdir)
        os.makedirs(tmpdir)
        cfgsrc = os.path.join(self._rootdir, "config")
        cfgdst = os.path.join(tmpdir, "config")
        shutil.copytree(cfgsrc, cfgdst)

        self.info("gathering fat jar")
        jarname = self.assemble_fat_jar()
        shutil.copy(jarname, os.path.join(tmpdir, "fulfillment.jar"))

        self.info("gathering launch script")
        launch_dir = os.path.join(self._rootdir, "launcher")
        shutil.copy(os.path.join(launch_dir, "launcher.py"), tmpdir)
        shutil.copy(os.path.join(launch_dir, "component.py"), tmpdir)
        shutil.copy(os.path.join(launch_dir, "swfworker.py"), tmpdir)

        self.info("gathering install splogger")
        splogger = os.path.join(self._rootdir, "deployment", "deployment", "splogger.py")
        shutil.copy(splogger, tmpdir)

        self.info("gathering virtualenv setup script")
        vesetup = os.path.join(self._rootdir, "deployment", "virtualenv", "setup")
        shutil.copy(vesetup, os.path.join(tmpdir, "vesetup"))

        self.info("gathering install script and deps")
        installscript = os.path.join(self._rootdir, "deployment", "scripts", "ffinstall.py")
        shutil.copy(installscript, tmpdir)

        if self._debug:
            debugdir = os.path.join(self._rootdir, "deployment", "debug")
            shutil.copy(os.path.join(debugdir, "Dockerfile"), tmpdir)
            shutil.copy(os.path.join(debugdir, "OS-Dockerfile"), tmpdir)
            shutil.copy(os.path.join(debugdir, "debug.sh"), tmpdir)

        return tmpdir
