#!/usr/bin/env python
import sys, os, stat
import subprocess
import shutil
import time
from splogger import Splogger


class Packager:
    """ Packages up everything needed for deployment
        into a directory, ready to be sync-ed to S3
    """
    def __init__(self, rootdir, unattended=False, log_filename="/var/log/balihoo/fulfillment/packup.log"):
        self._rootdir = rootdir
        self._log = Splogger(log_filename)
        self._unattended = unattended

    def info(self,msg):
        print(msg)
        with self._log.increased_indirection():
            self._log.info(msg)

    def error(self,msg):
        print("ERROR: " + msg)
        with self._log.increased_indirection():
            self._log.error(msg)

    def cmd_exists(self, cmd):
        if cmd.find(";") > -1:
            raise Exception("semi-colon in file name")
        ret = subprocess.call("hash " + cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        return ret == 0

    def install_aws(self):
        cmds = ["wget https://s3.amazonaws.com/aws-cli/awscli-bundle.zip",
               "unzip awscli-bundle.zip",
               "sudo awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws"]
        for cmd in cmds:
            ret = subprocess.call(cmd, shell=True)
            if ret != 0:
                self.error("Installation failed.")
                return False

        return True

    def verify_env_var(self, var):
        if var not in os.environ:
            if self._unattended:
                self.error(var + " not found in env.")
                return False

            print(var + " not found in env. Please enter it now")
            os.environ[var] = raw_input(var + ": ")
        return True

    def check_aws_requirements(self):
        if not self.cmd_exists("aws"):
            if self._unattended:
                self.error("AWS CLI tools not found")
                return False

            print("AWS CLI tools required. Would you like to install them? (Y/N)")
            while True:
                res = raw_input(">>")
                if res in ["Y", "y", "Yes", "yes"]:
                    return install_aws()
                elif res in ["N", "n", "No", "no"]:
                    return False
                else:
                    print("choose 'yes' or 'no' please")

        if self.verify_env_var("AWS_ACCESS_KEY_ID"):
            if self.verify_env_var("AWS_SECRET_ACCESS_KEY"):
                #consider a quick aws S3 access check?
                return True
        return False

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
        tmpdir = os.path.join(self._rootdir, "deployments/%s" % (datetime.datetime.now().strftime("%Y%m%d-%H:%M:%S.%f"))
        if os.path.isdir(tmpdir):
            shutil.rmtree(tmpdir)
        os.makedirs(tmpdir)
        cfgsrc = os.path.join(self._rootdir, "config")
        cfgdst = os.path.join(tmpdir, "config")
        shutil.copytree(cfgsrc, cfgdst)

        self.info("gathering fat jar")
        jarname = self.assemble_fat_jar()
        #cp $JARNAME $TMPDIR/fulfillment.jar
        shutil.copy(jarname, tmpdir)

        self.info("gathering launch script")
        #cp $ROOTDIR/launch_fulfillment $TMPDIR
        launchfile = os.path.join(self._rootdir, "launch_fulfillment")
        shutil.copy(launchfile, tmpdir)

        self.info("setting launch script execute permissions")
        #chmod a+x $TMPDIR/launch_fulfillment
        launchfile = os.path.join(tmpdir, "launch_fulfillment")
        os.chmod(launchfile, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                             stat.S_IRGRP |                stat.S_IXGRP |
                             stat.S_IROTH |                stat.S_IXOTH )
        return tmpdir
