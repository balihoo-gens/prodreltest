#!/usr/bin/env python
import sys, os
import subprocess
import shutil

class Packager:
    def __init__(self, logger=Splogger()):
        self.logger = logger

    def log(self, level, msg):
        logger.log(

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
                print("Installation failed.")
                return False

        return True

    def verify_env_var(self, var):
        if var not in os.environ:
            print(var + " not found in env. Please enter it now")
            os.environ[var] = raw_input(var + ": ")

    def check_aws_requirements(self):
        if not cmd_exists("aws"):
            print("AWS CLI tools required. Would you like to install them? (Y/N)")
            while True:
                res = raw_input(">>")
                if res in ["Y", "y", "Yes", "yes"]:
                    return install_aws()
                elif res in ["N", "n", "No", "no"]:
                    return False
                else:
                    print("choose 'yes' or 'no' please")

        verify_env_var("AWS_ACCESS_KEY_ID")
        verify_env_var("AWS_SECRET_ACCESS_KEY")
        #consider a quick aws S3 access check?
        return True

    def assemble_fat_jar(rootdir):
        jarname = os.path.join(rootdir, "target/scala-2.10/fulfillment-assembly-1.0-SNAPSHOT.jar")
        ret = subprocess.Popen(["sbt", "assembly"], cwd=rootdir)
        return jarname

    def build_tarball(rootdir):
        tmpdir = os.path.join(rootdir, "installtmp")
        cfgdir = os.path.join(rootdir, "config")
        shutil.copytree(cfgdir, tmpdir)

        print("gathering fat jar")
        #cp $JARNAME $TMPDIR/fulfillment.jar
        jarname = assemble_fat_jar(rootdir)
        shutil.copy(jarname, tmpdir)

        print("gathering launch script")
        #cp $ROOTDIR/launch_fulfillment $TMPDIR
        launchfile = os.path.join(rootdir, "launch_fulfillment")
        shutil.copy(launchfile, tmpdir)

        print("setting launch script execute permissions")
        #chmod a+x $TMPDIR/launch_fulfillment
        launchfile = os.path.join(tmpdir, "launch_fulfillment")
        os.chmod(launchfile, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                             stat.S_IRGRP |                stat.S_IXGRP |
                             stat.S_IROTH |                stat.S_IXOTH )

        tarball = "fulfillment-%d" % (int(time.time()))
        print("creating application tarball: " + tarball)
        #tar -zcf ${TARBALL} ${TMPDIR}
        shutil.make_archive(tarball, "gztar", tmpdir)

def main(args):
    thisdir = os.path.dirname(os.path.realpath(__file__))
    rootdir = os.path.join(thisdir, "..")

    logger = Splogger(
    ret = subprocess.call(cmd, shell=True)
    if check_aws_requirements():
        gather_files():

if __name__ == "__main__":
    main(sys.argv)
