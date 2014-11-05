import sys, os, stat
try:
    from splogger import Splogger
    from launcher import Launcher
except ImportError:
    #path hackery really just for local testing
    # because these are elsewhere on the EC2 instance
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'deployment'))
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'launcher'))
    from splogger import Splogger
    from launcher import Launcher

import argparse
import subprocess
import time
import urllib
import tarfile
import json

class Installer(object):
    def __init__(self, logfile, distro):
        self._log = Splogger(logfile)
        self._distro = distro

    def launch_app(self, classes, noworker):
        thisdir = os.path.dirname(os.path.realpath(__file__))
        cmdline = ["python", "launcher.py"]
        if noworker:
            cmdline += ["--noworker"]
        cmdline += classes
        proc = subprocess.Popen(cmdline, cwd=thisdir)
        self._log.info("started launcher process with pid %d" % (proc.pid,))

    def run_wait_log(self, cmd, cwd=None, raise_on_err=False):
        self._log.info("running " + " ".join(cmd))
        kwargs = {}
        if not cwd is None:
          kwargs["cwd"] = cwd
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **kwargs
        )
        self._log.info("%s process started with pid %d" % (cmd[0], proc.pid))
        #block and accumulate output
        out, err = proc.communicate()
        if len(out) > 0:
            self._log.info(out)
        if len(err) > 0:
            self._log.error(err)
        if raise_on_err and (proc.returncode != 0):
            raise Exception("process %d returned %d" % (proc.pid, proc.returncode))
        return proc.returncode

    def make_executable(self, path):
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                       stat.S_IRGRP |                stat.S_IXGRP |
                       stat.S_IROTH |                stat.S_IXOTH )

    def run_s3_installer(self, s3bucket, script_name, params=None):
        if params is None:
            params = []
        s3url = s3bucket + "/" + script_name
        if not s3url.startswith("s3://"):
            s3url = "s3://" + s3url
        self._log.info("installing from s3: url=[%s]" % (s3url,))
        try:
            if self.run_wait_log(["aws", "s3","cp", s3url, "."]) >= 0:
                script = os.path.join(".", script_name)
                self.make_executable(script)
                self.run_wait_log([script] + params)
        except Exception as e:
            self._log.error("Failed to install from s3: %s" % (e.message,))

    def install_package(self, package_name):
        installer = "apt-get" if self._distro in ["Ubuntu", "Debian"] else "yum"
        self.run_wait_log([installer, "install", "-y", package_name], raise_on_err=True)

    #custom phantom build
    def install_phantom_custom(self):
        s3bucket = "s3://balihoo.dev.fulfillment"
        s3url = os.path.join(s3bucket, "phantomjs/builtfromsource/master/bin", "phantomjs")
        try:
            self.run_wait_log(["aws", "s3","cp", s3url, "/usr/bin/phantomjs"], raise_on_err=True)
            self.make_executable("/usr/bin/phantomjs")
            self.install_package("libjpeg8")
            self.install_package("libfontconfig1")
        except Exception as e:
            self._log.error("Failed to install phantom: %s" % (e.message,))

    def install_phantom(self, version):
        self._log.info("installing phantomjs binary version " + version)

        fullname = "phantomjs-%s-linux-x86_64" % (version,)
        tarballname = "%s.tar.bz2" % (fullname,)
        url = "https://bitbucket.org/ariya/phantomjs/downloads/%s" % (tarballname,)
        targetdir = "/opt/phantomjs"
        urllib.urlretrieve(url,tarballname)
        if not os.path.isdir(targetdir):
            os.makedirs(targetdir)
        archive = tarfile.open(tarballname)
        archive.extractall(path=targetdir)
        os.symlink(os.path.join(targetdir,fullname,"bin","phantomjs"), "/usr/bin/phantomjs")

    def associate_eip(self, eip):
        self._log.info("associating with eip " + eip)
        try:
            import boto.ec2 as ec2
        except ImportError:
            self._log.error("unable to import boto")
            return
        #http://stackoverflow.com/questions/625644/find-out-the-instance-id-from-within-an-ec2-machine
        awsinstanceurl = "http://169.254.169.254/latest/dynamic/instance-identity/document"
        instance_data = json.loads(urllib.urlopen(awsinstanceurl).read())
        instance_id = instance_data["instanceId"]
        access_key = os.environ["AWS_ACCESS_KEY_ID"]
        secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
        region = os.environ["AWS_REGION"]
        ec2conn = ec2.connect_to_region(region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        if ec2conn.disassociate_address(public_ip=eip):
            self._log.info("successfully disassociated eip " + eip)
        if ec2conn.associate_address(instance_id=instance_id, public_ip=eip):
            self._log.info("successfully associated with eip " + eip)
        else:
            self._log.error("failed to associate with eip " + eip)

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Install the Fulfillment application")

    #defaults
    splunk_s3bucket = "balihoo.dev.splunk"
    splunk_script = "installSplunkForwarder.sh"
    newrelic_s3bucket = "balihoo.dev.aws-installs/newrelic"
    newrelic_config = "fulfillment_dev.newrelic.yml"
    phantomversion = "1.9.8"

    parser.add_argument('classes', metavar='C', type=str, nargs='*', help='classes to run')
    parser.add_argument('-l','--logfile', help='the log file', default='/var/log/balihoo/fulfillment/installer.log')
    parser.add_argument('--eip', help='the eip for this instance', default=None)
    parser.add_argument('--distro', help='the linux distribution to use for this instance', default="Ubuntu")
    parser.add_argument('--env', help='the environment to use for this instance', default="dev")
    parser.add_argument('--nolaunch', help='do not launch the app', action='store_true')
    parser.add_argument('--noworker', help='do not launch with a swfworker', action='store_true')
    #phantom
    parser.add_argument('--phantomversion', help='the phantomjs version to download', default=phantomversion)
    parser.add_argument('--nophantom', help='do not install phantomjs', action='store_true')
    #splunk
    parser.add_argument('--splunk_s3bucket', help='the AWS s3 bucket URL used to install splunk', default=splunk_s3bucket)
    parser.add_argument('--splunk_script', help='the script name used to install splunk', default=splunk_script)
    parser.add_argument('--nosplunk', help='do not install splunk', action='store_true')
    #new relic
    parser.add_argument('--newrelic_s3bucket', help='the AWS s3 bucket URL used to install newrelic', default=newrelic_s3bucket)
    parser.add_argument('--newrelic_config', help='the config file used to install newrelic', default=newrelic_config)
    parser.add_argument('--nonewrelic', help='do not install newrelic', action='store_true')

    args = parser.parse_args()

    installer = Installer(args.logfile, args.distro)

    if not args.nonewrelic:
        nrsysmond_params = ["--s3-bucket", newrelic_s3bucket]
        javaagent_params = nrsysmond_params + ["--config", newrelic_config]
        installer.run_s3_installer(newrelic_s3bucket, "nrsysmond-install.sh", nrsysmond_params)
        installer.run_s3_installer(newrelic_s3bucket, "javaagent-install.sh", javaagent_params)
    if not args.nosplunk:
        installer.run_s3_installer(args.splunk_s3bucket, args.splunk_script)
    if not args.nophantom:
        installer.install_phantom(args.phantomversion)
    if args.eip:
        installer.associate_eip(args.eip)
    if not args.nolaunch:
        installer.launch_app(args.classes, args.noworker)
