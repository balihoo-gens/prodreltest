import boto.s3 as awss3
import boto
import os, sys

class Uploader(object):

    def __init__(self, s3bucket, region, access_key, secret_key):
        #self._conn = awss3.connect_to_region(
        #    region,
        self._conn = boto.connect_s3 (
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        self._s3bucket_name = s3bucket
        self._s3bucket = self._conn.get_bucket(self._s3bucket_name)

    def upload_dir(self, srcpath):
        if os.path.isdir(srcpath):
            predirs, dirname = os.path.split(srcpath)
            dirnameindex = len(predirs)
            if dirnameindex > 0:
                dirnameindex += 1
            for root, subs, files in os.walk(srcpath):
                #abs path may have been provided, but we
                #only need the part from the dirname and on
                reldir = root[dirnameindex:]
                for f in files:
                    s3path = os.path.join(reldir, f)
                    localpath = os.path.join(root, f)
                    s3key = self._s3bucket.new_key(s3path)
                    s3key.set_contents_from_filename(localpath)
                    print("uploaded %s to %s" % (localpath, s3path))
            return "s3://" + self._s3bucket_name + "/" + dirname
        else:
            raise Exception("Unable to upload: %s is not a directory" % (srcpath,))
