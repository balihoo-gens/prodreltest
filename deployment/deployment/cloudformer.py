import boto.cloudformation as awscf
class CloudFormer(object):

    def __init__(self, region, access_key, secret_key):
        self._conn = awscf.connect_to_region(
            region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

    def create_stack(self, name, template, parameters={}):
        self._conn.create_stack(
            name,
            template_body=template,
            parameters=parameters.items()
        )

