from splogger import Splogger
from packager import Packager
from cloudformer import CloudFormer
from uploader import Uploader

import os
import json
import time

class Deployment(object):
    def __init__(self, log_filename, region, pyversion, veversion, dasheip, unattended=False):
        self._dasheip = dasheip
        self._veversion = veversion
        self._pyversion = pyversion
        self._region = region
        self._unattended = unattended
        self._log_filename = log_filename
        self.log = Splogger(self._log_filename, component="deployment")

    def verify_env_var(self, var):
        if var not in os.environ:
            if self._unattended:
                self.error(var + " not found in env.")
                return False

            print(var + " not found in env. Please enter it now")
            os.environ[var] = raw_input(var + ": ")
        return True

    def check_aws_requirements(self):
        if self.verify_env_var("AWS_ACCESS_KEY_ID"):
            if self.verify_env_var("AWS_SECRET_ACCESS_KEY"):
                return True
        return False

    def package(self, rootdir):
        self.log.debug("packaging " + rootdir)
        p = Packager(rootdir, log_filename=self._log_filename)
        return p.package()

    def upload(self, pkgpath, s3bucket):
        if not self.check_aws_requirements():
            raise Exception("AWS Credentials not in environment")
        self.log.debug("uploading " + pkgpath)
        u = Uploader(
            s3bucket,
            self._region,
            os.environ["AWS_ACCESS_KEY_ID"],
            os.environ["AWS_SECRET_ACCESS_KEY"]
        )
        return u.upload_dir(pkgpath)

    def create_stack(self, s3url, template_file, script_file):
        if not self.check_aws_requirements():
            raise Exception("AWS Credentials not in environment")

        template_data = None
        with open(template_file) as tf:
            #validate json here by loading it
            template_data = json.loads(tf.read())


        access_key = os.environ["AWS_ACCESS_KEY_ID"]
        secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
        self.log.debug("creating a stack using " + s3url)
        c = CloudFormer(self._region, access_key, secret_key)

        parameters = {
            "KeyName" : "paul-ami-pair",
            "WorkerInstanceType" : "m1.small",
            "DashboardInstanceType" : "t1.micro",
            "MinInstances" : "1",
            "MaxInstances" : "10",
            "WebPort" : "8080",
            "WorkerScript" : self.gen_script(script_file, access_key, secret_key, s3url, ""),
            "DashboardScript" : self.gen_script(script_file, access_key, secret_key, s3url, "com.balihoo.fulfillment.dashboard.dashboard")
        }

        #json dump guarantees valid json, but not a valid template per se
        stackname = "fulfillment%d" % (int(time.time()),)
        return c.create_stack(stackname, json.dumps(template_data), parameters)

    def gen_script(self, script_file, access_key, secret_key, s3_bucket_url, classes):
        pieces = [
            "#!/bin/bash",
            "AWSACCESSKEY=%s" % access_key,
            "AWSSECRETKEY=%s" % secret_key,
            "AWSREGION=%s" % self._region,
            "S3BUCKETURL=%s" % s3_bucket_url,
            "CLASSNAMES=%s" % classes,
            "VEDIR=/opt/balihoo/virtualenv",
            "PYVERSION=%s" % self._pyversion,
            "VEVERSION=%s" % self._veversion,
        ]
        #optionally add the dashboard eip option.
        if self._dasheip:
            pieces.append('DASHEIPOPT="--dasheip %s"' % self._dasheip)

        with open(script_file) as f:
            pieces.append(f.read())

        return "\n".join(pieces)

