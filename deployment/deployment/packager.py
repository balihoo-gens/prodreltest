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
    def __init__(self, rootdir, unattended=False, log_filename="/var/log/balihoo/fulfillment/packup.log"):
        self._rootdir = rootdir
        self._log = Splogger(log_filename, component="packager")
        self._unattended = unattended

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
        ret = subprocess.Popen(
            ["sbt", "assembly"],
            cwd=self._rootdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        return jarname

    def package(self):
        """ returns the path of the dir to be
            sync-ed to the S3 bucket
        """
        tmpdir = os.path.join(self._rootdir, "deployments/%s" % (datetime.datetime.now().strftime("%Y%m%d-%H:%M:%S.%f")))
        if os.path.isdir(tmpdir):
            shutil.rmtree(tmpdir)
        os.makedirs(tmpdir)
        cfgsrc = os.path.join(self._rootdir, "config")
        cfgdst = os.path.join(tmpdir, "config")
        shutil.copytree(cfgsrc, cfgdst)

        self.info("gathering fat jar")
        jarname = self.assemble_fat_jar()
        #cp $JARNAME $TMPDIR/fulfillment.jar
        shutil.copy(jarname, os.path.join(tmpdir, "fulfillment.jar"))

        self.info("gathering launch script")
        #cp $ROOTDIR/launch_fulfillment $TMPDIR
        launchfile = os.path.join(self._rootdir, "launcher.py")
        shutil.copy(launchfile, tmpdir)

        self.info("gathering install script and deps")
        #cp $ROOTDIR/launch_fulfillment $TMPDIR
        installscript = os.path.join(self._rootdir, "deployment/scripts/ffinstall")
        splogger = os.path.join(self._rootdir, "deployment/deployment/splogger.py")
        shutil.copy(installscript, tmpdir)

        self.info("setting launch script execute permissions")
        #chmod a+x $TMPDIR/launch_fulfillment
        launchfile = os.path.join(tmpdir, "launch_fulfillment")
        os.chmod(launchfile, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                             stat.S_IRGRP |                stat.S_IXGRP |
                             stat.S_IROTH |                stat.S_IXOTH )
        return tmpdir
