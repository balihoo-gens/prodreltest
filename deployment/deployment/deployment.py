from splogger import Splogger
from packager import Packager
from cloudformer import CloudFormer
from uploader import Uploader
from collections import namedtuple
import os
import json
import time

class Deployment(object):
    Config = namedtuple('DeploymentConfig', [
        "access_key",
        "secret_key",
        "region",
        "s3bucket",
        "ssh_keyname",
        "worker_instance_type",
        "dash_instance_type",
        "min_instances",
        "max_instances",
        "access_ip_mask",
        "web_port",
        "pyversion",
        "veversion",
        "dasheip",
        "distro",
        "env",
        "nonewrelic",
        "debug"
    ])

    def __init__(self, log_filename, cfg):
        self._cfg = cfg
        self._log_filename = log_filename
        self.log = Splogger(self._log_filename, component="deployment")

    def package(self, rootdir):
        self.log.debug("packaging " + rootdir)
        p = Packager(rootdir, log_filename=self._log_filename, debug=self._cfg.debug)
        return p.package()

    def upload(self, pkgpath):
        self.log.debug("uploading " + pkgpath)
        u = Uploader(
            self._cfg.s3bucket,
            self._cfg.region,
            self._cfg.access_key,
            self._cfg.secret_key,
        )
        return u.upload_dir(pkgpath)

    def create_stack(self, s3dir, template_file, script_file):
        template_data = None
        with open(template_file) as tf:
            #validate json here by loading it
            template_data = json.loads(tf.read())

        self.log.debug("creating a stack using " + s3dir)
        c = CloudFormer(self._cfg.region, self._cfg.access_key, self._cfg.secret_key)

        dash_class = "com.balihoo.fulfillment.dashboard.dashboard"
        cfg = self._cfg

        parameters = {
            "KeyName"               : cfg.ssh_keyname,
            "WorkerInstanceType"    : cfg.worker_instance_type,
            "DashboardInstanceType" : cfg.dash_instance_type,
            "MinInstances"          : cfg.min_instances,
            "MaxInstances"          : cfg.max_instances,
            "AccessIPMask"          : cfg.access_ip_mask,
            "WebPort"               : cfg.web_port,
            "WorkerScript"          : self.gen_script(script_file, s3dir, None, False, ""),
            "DashboardScript"       : self.gen_script(script_file, s3dir, self._cfg.dasheip, True, dash_class),
            "LinuxDistro"           : cfg.distro,
            "Environment"           : cfg.env,
        }

        #stackname must be unique, so add a ms resolution unix timestamp
        hextime = hex(int(time.time()*1000))[2:]
        #also add the environment
        stackname = "fulfillment-%s-%s" % (cfg.env, hextime,)
        #json dump guarantees valid json, but not a valid template per se
        return c.create_stack(stackname, json.dumps(template_data), parameters)

    def gen_script(self, script_file, s3dir, eip, noworker, classes):
        pieces = [
            "#!/bin/bash",
            "set -e",
            "AWSACCESSKEY=%s" % self._cfg.access_key,
            "AWSSECRETKEY=%s" % self._cfg.secret_key,
            "AWSREGION=%s"    % self._cfg.region,
            "S3BUCKET=%s"     % self._cfg.s3bucket,
            "S3DIR=%s"        % s3dir,
            "CLASSNAMES=%s"   % classes,
            "VEDIR=%s"        % "/opt/balihoo/virtualenv",
            "PYVERSION=%s"    % self._cfg.pyversion,
            "VEVERSION=%s"    % self._cfg.veversion,
            "DISTRO=%s"       % self._cfg.distro,
            "ENV_NAME=%s"     % self._cfg.env,
        ]

        if self._cfg.nonewrelic:
            pieces.append('NONEWRELIC="--nonewrelic"')

        #optionally add the dashboard eip option.
        if eip:
            pieces.append('EIPOPT="--eip %s"' % eip)

        #dashboard should not have a swf worker to launch addl processes
        if noworker:
            pieces.append('NOWORKER="--noworker"')

        with open(script_file) as f:
            pieces.append(f.read())

        return "\n".join(pieces)

