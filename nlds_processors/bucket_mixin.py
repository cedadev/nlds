"""
bucket_mixin.py
"""

__author__ = "Neil Massey"
__date__ = "27 Feb 2025"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import nlds.rabbit.routing_keys as RK
from minio.error import S3Error
from urllib3.exceptions import HTTPError, MaxRetryError
import json
from nlds.errors import MessageError
import grp

class BucketError(MessageError):
    pass

class BucketMixin:
    """
    Some common functions for working with S3 buckets.
    Requires a parent class that contains:
      minio client as self.s3_client.
      nlds config object as self.whole_config
    """

    OBJECT_STORE_ACCESS_POLICY_CONFIG = "object_store_access_policy"
    NLDS_USER_ACCESS_POLICY_CONFIG = "nlds_user"
    GROUP_ACCESS_POLICY_CONFIG = "group"

    @staticmethod
    def _get_bucket_name(transaction_id: str):
        if transaction_id is None:
            raise BucketError(message="Transaction id is None")
        bucket_name = f"nlds.{transaction_id}"
        return bucket_name

    def _make_bucket(self, bucket_name: str):
        """Check bucket exists and create it if it doesn't"""
        if self.s3_client is None:
            raise BucketError(message="self.s3_client is None")

        if bucket_name is None:
            raise BucketError(message="Transaction id is None")

        # Check that bucket exists, and create if not
        try:
            if not self.s3_client.bucket_exists(bucket_name):
                self.s3_client.make_bucket(bucket_name)
                self.log(
                    f"Creating bucket ({bucket_name}) for this transaction",
                    RK.LOG_INFO,
                )
            else:
                self.log(
                    f"Bucket ({bucket_name}) already exists",
                    RK.LOG_INFO,
                )
        except (S3Error, MaxRetryError) as e:
            raise BucketError(message=str(e))

    def _bucket_exists(self, bucket_name: str):
        """Check that the bucket name actually exists."""
        bucket_exists = False
        # try to get the bucket - may throw exception if user does not have access
        # permissions
        try:
            bucket_exists = self.s3_client.bucket_exists(bucket_name)
        except (S3Error, HTTPError) as e:
            raise BucketError(message=str(e))
        return bucket_exists

    def _get_bucket_name_object_name(self, path_details: str):
        """Get the bucket name and object name from the path_details"""
        if path_details.bucket_name is None:
            # Check that bucket_name is not None first
            reason = "Unable to get bucket_name from message info"
            self.log(
                f"{reason}, adding {path_details.object_name} to failed list.",
                RK.LOG_INFO,
            )
            raise BucketError(message=reason)
        else:
            bucket_name = path_details.bucket_name
            object_name = path_details.object_name
        return bucket_name, object_name

    def _get_bucket_policy(self, bucket_name: str):
        try:
            bucket_policy_raw = self.s3_client.get_bucket_policy(bucket_name)
            # bucket_policy_raw is a string - convert to dictionaries and lists
            bucket_policy = json.loads(bucket_policy_raw)
        except S3Error as e:
            # create an empty bucket policy as the return for policy doesn't exist
            if e.code == "NoSuchBucketPolicy":
                bucket_policy = {}
            else:
                raise BucketError(
                    message=f"Error getting access policy for bucket {bucket_name} in "
                    f"tenancy {self.tenancy}. Original error {e}"
                )
        return bucket_policy

    @staticmethod
    def _create_bucket_policy_template(bucket_name: str):
        bucket_policy = {
            "Version": "2008-10-17",
            "Id": f"{bucket_name} policy",
            "Statement": [],
        }
        return bucket_policy

    def _edit_nlds_user_policy(self):
        statements = self.bucket_policy["Statement"]
        for s in statements:
            try:
                principal = s["Principal"]
                if "nlds" in principal["user"]:
                    # delete the nlds entry
                    statements.remove(s)
            except KeyError:
                # not found - is okay
                pass

        # create the NLDS user statement
        try:
            nlds_statement = self.access_policies[self.NLDS_USER_ACCESS_POLICY_CONFIG]
            # edit the user to nlds
            nlds_statement["Principal"]["user"] = ["nlds"]
        except KeyError:
            raise BucketError(
                message=f"Could not find the {self.NLDS_USER_ACCESS_POLICY_CONFIG} key "
                f"in the {self.OBJECT_STORE_ACCESS_POLICY_CONFIG} section of the "
                f"config file"
            )
        statements.append(nlds_statement)

    def _edit_group_policy(self, group: str):
        statements = self.bucket_policy["Statement"]
        create = True
        for s in statements:
            try:
                principal = s["Principal"]
                if group in principal["group"]:
                    # don't override the group as the group admins might have altered it
                    create = False
            except KeyError:
                # not found - is okay, create remains True
                pass

        if create:
            try:
                group_statement = self.access_policies[self.GROUP_ACCESS_POLICY_CONFIG]
                # edit the user to nlds
                group_statement["Principal"]["group"] = [group]
            except KeyError:
                raise BucketError(
                    message=f"Could not find the {self.GROUP_ACCESS_POLICY_CONFIG} key "
                    f"in the {self.OBJECT_STORE_ACCESS_POLICY_CONFIG} section of the "
                    f"config file"
                )
            statements.append(group_statement)

    def _get_group_name_from_group_id(self, gid: int):
        """Get a group name from the group id using the linux database."""
        grp_details = grp.getgrgid(gid)
        return grp_details.gr_name

    # write the policy setting functions below
    def _set_access_policies(self, bucket_name: str, group: str|int):
        """Set the access policies for a bucket.
        These are stored in the /etc/nlds-config file, in the
        `object_store_access_policy` section, under the `nlds_user` and `group` keys.

        This part of the mixin relies on the parent class having a self.whole_config
        member variable containing the config, which is loaded by the parent class.
        """

        if type(group) is int:
            # convert the integer group to a group name
            try:
                group = self._get_group_name_from_group_id(group)
            except KeyError:
                raise BucketError(
                    message=f"Could not find the group name for group id {group}"
                )

        # get the section from the config
        try:
            self.access_policies = self.whole_config[
                self.OBJECT_STORE_ACCESS_POLICY_CONFIG
            ]
        except KeyError:
            raise BucketError(
                message=f"Could not find the {self.OBJECT_STORE_ACCESS_POLICY_CONFIG} "
                "section in the config file"
            )

        # get the current bucket policy
        self.bucket_policy = self._get_bucket_policy(bucket_name)
        if self.bucket_policy == {}:
            self.bucket_policy = self._create_bucket_policy_template(bucket_name)

        # edit the user policy
        self._edit_nlds_user_policy()
        # edit the group policy
        self._edit_group_policy(group)

        # set the bucket policy
        try:
            self.s3_client.set_bucket_policy(
                bucket_name, json.dumps(self.bucket_policy)
            )
        except S3Error as e:
            if e.code == "MalformedPolicy":
                raise BucketError(message=f"Malformed policy: {self.bucket_policy}")
            elif e.code == "AccessDenied":
                raise BucketError(
                    message=f"You do not have permission to change the policy for "
                    f"bucket {bucket_name}"
                )
            else:
                raise BucketError(
                    message=f"Encountered error when changing access policy for bucket "
                    f"{bucket_name}. Original error: {e}"
                )

        self.log(
            f"Set access for bucket {bucket_name} to {self.bucket_policy}",
            RK.LOG_INFO,
        )
