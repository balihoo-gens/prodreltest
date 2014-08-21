from splogger import Splogger
from packager import Packager
from cloudformer import CloudFormer
from uploader import Uploader

import os
import json
import time

class Deployment(object):
    def __init__(self, log_filename, unattended=False):
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
            "us-west-2",
            os.environ["AWS_ACCESS_KEY_ID"],
            os.environ["AWS_SECRET_ACCESS_KEY"]
        )
        return u.upload_dir(pkgpath)

    def create_stack(self, s3url, template_file):
        if not self.check_aws_requirements():
            raise Exception("AWS Credentials not in environment")

        template_data = None
        with open(template_file) as tf:
            #validate json here by loading it
            template_data = json.loads(tf.read())

        self.log.debug("creating a stack using " + s3url)
        c = CloudFormer(
            "us-west-2",
            os.environ["AWS_ACCESS_KEY_ID"],
            os.environ["AWS_SECRET_ACCESS_KEY"]
        )
        parameters = {
            "KeyName" : "paul-ami-pair",
            "InstanceType" : "t1.micro",
            "MinInstances" : "1",
            "MaxInstances" : "10",
            "AWSAccessKey" : os.environ["AWS_ACCESS_KEY_ID"],
            "AWSSecretKey" : os.environ["AWS_SECRET_ACCESS_KEY"],
            "S3BucketURL" : s3url
        }
        #json dump guarantees valid json, but not a valid template per se
        stackname = "fulfillment%d" % (int(time.time()),)
        return c.create_stack(stackname, json.dumps(template_data), parameters)

