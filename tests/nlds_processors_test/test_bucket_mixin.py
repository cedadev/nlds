"""
test_bucket_mixin.py
"""

__author__ = "Neil Massey and Jack Leland"
__date__ = "07 Apr 2025"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from nlds_processors.bucket_mixin import BucketMixin
import json
import minio
import os.path
import nlds.server_config as CFG
import logging


def connect(key_path: str, tenancy: str):
    access_key, secret_key = get_keys(key_path)
    client = minio.Minio(
        tenancy, access_key=access_key, secret_key=secret_key, secure=False
    )
    return client


def get_keys(key_path: str):
    fh = open(os.path.expanduser(key_path))
    jc = json.load(fh)
    access_key = jc["access_key"]
    secret_key = jc["secret_key"]
    fh.close()
    return access_key, secret_key

def log(msg: str, level: int):
    """Dummy log function"""
    print(msg)

if __name__ == "__main__":
    tenancy = "nlds-staging-o.s3.jc.rl.ac.uk"
    bucket = "nlds.4829e19d-bf69-4a0e-a71d-c0ddfd8eb713"
    #bucket = "nlds.5eac7a13-084b-4927-aa6b-cdeb11ddf508"
    key_path = "~/.nlds-staging-o-keys.json"
    group = "cedaproc"

    client = connect(key_path, tenancy)
    bucket_mixin = BucketMixin()
    bucket_mixin.whole_config = CFG.load_config()
    bucket_mixin.s3_client = client
    bucket_mixin.tenancy = tenancy
    bucket_mixin.log = log
    for i in range(0,10):
        bucket_mixin._set_access_policies(bucket, group)    
        print(bucket_mixin._get_bucket_policy(bucket))
