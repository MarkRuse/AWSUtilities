# -*- coding: utf-8 -*-
import logging
import os
import boto3
import botocore
import shutil
import tempfile
from collections import deque, namedtuple
from botocore.exceptions import ClientError
from operator import attrgetter


class Helper(object):
    logger = None

    def __init__(self):
        self.logger = self.logger or logging.getLogger('%s.%s' % (os.environ.get("appname"), __name__))

    @staticmethod
    def get_bucket(bucket, key=None, secret=None):
        if key is None or secret is None:
            s3 = boto3.resource("s3") 
        else:
            s3 = boto3.session.Session(aws_access_key_id=key, aws_secret_access_key=secret).resource("s3")          

        return s3.Bucket(bucket)

    @staticmethod
    def load_object(bucket, key):
        client = boto3.client('s3')
        s3file = client.get_object(Bucket=bucket, Key=key)
        return s3file['Body'].read()

    @staticmethod
    def delete_object(bucket, key):
        client = boto3.client('s3')
        logger = logging.getLogger('%s.%s' % (os.environ.get("appname"), __name__))
        logger.info("Deleting file %s/%s", bucket, key)
        client.delete_object(Bucket=bucket, Key=key)

    @staticmethod
    def copy_object(src_bucket_name, src_object_name,
                    dest_bucket_name, dest_object_name=None):
        copy_source = {'Bucket': src_bucket_name, 'Key': src_object_name}
        if dest_object_name is None:
            dest_object_name = src_object_name

        logger = logging.getLogger('%s.%s' % (os.environ.get("appname"), __name__))
        s3 = boto3.client('s3')
        try:
            logger.debug('copy s3: Copy Source %s, Dest Bucket %s, Key %s' % (copy_source, dest_bucket_name, dest_object_name))
            s3.copy_object(CopySource=copy_source, Bucket=dest_bucket_name,
                      Key=dest_object_name)
        except ClientError as e:
            logging.error(e)
            raise FileNotFound(e)

    @staticmethod
    def concatenate_files(bucket, input_prefix, file_keys, output_key):
        total_contents = ""
        total_size = 0
        logger = logging.getLogger('%s.%s' % (os.environ.get("appname"), __name__))

        logger.info("Prefix %s, Concatinating files %s", input_prefix, file_keys)
        for file_key in file_keys:
            file_data = Helper.load_object(bucket, file_key)
            size = len(file_data)
            total_size += size
            total_contents += file_data
            logger.debug("part file %s found.  Size %s.  output size %s",file_key, size, len(total_contents))
        
        logger.info("Writing %s, bytes %s", output_key, total_size)
        Helper.put_string(total_contents, bucket, output_key)

        return total_contents

    @staticmethod
    def put_string(data, bucket_name, object_key):
        s3 = boto3.client('s3')
        try:
            s3.put_object(Body=data, Bucket=bucket_name, Key=object_key)
        except ClientError as e:
            logging.error(e)
            raise FileNotFound(e)

    @staticmethod
    def put_string_different_account(data, bucket_name, object_key, account_key, account_secret, region, acl='bucket-owner-full-control'):
        s3 = boto3.resource('s3', region_name=region,aws_access_key_id=account_key,
        aws_secret_access_key=account_secret)
        s3.Object(bucket_name, object_key).put(Body=data, ACL=acl)

    @staticmethod
    def stream_copy_no_creds(source, target, metadata=None):
        (source_bucket, source_file_key) = Helper.url_to_bucket_key(source)
        (target_bucket, target_file_key) = Helper.url_to_bucket_key(target)

        source_client = boto3.client("s3")
        source_response = source_client.get_object(Bucket=source_bucket,Key=source_file_key)

        destination_client = boto3.client("s3")
        if metadata is None:
            destination_client.upload_fileobj(source_response['Body'],target_bucket,target_file_key)
        else:
            destination_client.upload_fileobj(source_response['Body'],target_bucket,target_file_key, ExtraArgs={"Metadata": metadata})


    @staticmethod
    def stream_copy(source, target, source_key=None, source_secret=None, target_key=None, target_secret=None, ):
        (source_bucket, source_file_key) = Helper.url_to_bucket_key(source)
        (target_bucket, target_file_key) = Helper.url_to_bucket_key(target)

        if source_key is None or source_secret is None:
            source_client = boto3.client("s3")
        else:
            source_client = boto3.Session(source_key, source_secret).client("s3")
        source_response = source_client.get_object(Bucket=source_bucket, Key=source_file_key)

        if target_key is None or target_secret is None:
            destination_client = boto3.client("s3")
            destination_resource = boto3.resource("s3")
        else:
            destination_client = boto3.Session(target_key, target_secret).client("s3")
            destination_resource = boto3.Session(target_key, target_secret).resource("s3")

        destination_client.upload_fileobj(source_response['Body'], target_bucket, target_file_key)    
        destination_resource.ObjectAcl(target_bucket, target_key).put(ACL='bucket-owner-full-control')

    @staticmethod
    def object_exists(bucket, key):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket)
        objs = list(bucket.objects.filter(Prefix=key))
        if len(objs) > 0 and objs[0].key == key:
            return True
        else:
            return False

    @staticmethod
    def get_matching_s3_objects(bucket, prefix="", suffix="", delimiter=None, profile=None):
        if profile is not None:
            s3 = boto3.session.Session(profile_name=profile).client("s3")
        else:
            s3 = boto3.client("s3")

        paginator = s3.get_paginator("list_objects_v2")

        kwargs = {'Bucket': bucket}

        if isinstance(prefix, str):
            prefixes = (prefix, )
        else:
            prefixes = prefix

        for key_prefix in prefixes:
            kwargs["Prefix"] = key_prefix
            if delimiter: kwargs["Delimiter"] = delimiter

            for page in paginator.paginate(**kwargs):
                try:
                    if delimiter is not None:
                        # 'directory'
                        # yield [x['Prefix'] for x in page['CommonPrefixes']]
                        contents = page["CommonPrefixes"]
                    else:
                        if "Contents" in page:
                            contents = page["Contents"]
                        else:
                            return
                except KeyError:
                    return

                for obj in contents:
                        if "Key" in obj:
                            if obj["Key"].endswith(suffix):
                                yield obj
                        elif "Prefix" in obj:
                            if obj["Prefix"].endswith(suffix):
                                yield obj["Prefix"]


    @staticmethod
    def get_matching_s3_keys(bucket, prefix="", suffix=""):
        for obj in Helper.get_matching_s3_objects(bucket, prefix, suffix):
            yield obj["Key"]

    @staticmethod
    def url_to_bucket_key(url):
        if not url.startswith("s3://"):
            raise AttributeError("%s not a valid s3 url", url)
        else:
            parts = url.split("/")
            bucket = parts[2]
            key = "/".join(parts[3:])

        return (bucket, key)

    @staticmethod
    def __prev_str(s):
        if len(s) == 0:
            return s
        s, c = s[:-1], ord(s[-1])
        if c > 0:
            s += chr(c - 1)
        s += ''.join(['\u7FFF' for _ in range(10)])
        return s

    @staticmethod
    def s3list(bucket, path, suffix=None, start=None, end=None, recursive=True, list_dirs=True, list_objs=True, limit=None):
        """
        Iterator that lists a bucket's objects under path, (optionally) starting with
        start and ending before end.

        If recursive is False, then list only the "depth=0" items (dirs and objects).

        If recursive is True, then list recursively all objects (no dirs).

        Args:
            bucket:
                a boto3.resource('s3').Bucket().
            path:
                a directory in the bucket.
            start:
                optional: start key, inclusive (may be a relative path under path, or
                absolute in the bucket)
            end:
                optional: stop key, exclusive (may be a relative path under path, or
                absolute in the bucket)
            recursive:
                optional, default True. If True, lists only objects. If False, lists
                only depth 0 "directories" and objects.
            list_dirs:
                optional, default True. Has no effect in recursive listing. On
                non-recursive listing, if False, then directories are omitted.
            list_objs:
                optional, default True. If False, then directories are omitted.
            limit:
                optional. If specified, then lists at most this many items.

        Returns:
            an iterator of S3Obj.

        Examples:
            # set up
            >>> s3 = boto3.resource('s3')
            ... bucket = s3.Bucket(name)

            # iterate through all S3 objects under some dir
            >>> for p in s3ls(bucket, 'some/dir'):
            ...     print(p)

            # iterate through up to 20 S3 objects under some dir, starting with foo_0010
            >>> for p in s3ls(bucket, 'some/dir', limit=20, start='foo_0010'):
            ...     print(p)

            # non-recursive listing under some dir:
            >>> for p in s3ls(bucket, 'some/dir', recursive=False):
            ...     print(p)

            # non-recursive listing under some dir, listing only dirs:
            >>> for p in s3ls(bucket, 'some/dir', recursive=False, list_objs=False):
            ...     print(p)
    """

        S3Obj = namedtuple('S3Obj', ['key', 'mtime', 'size', 'ETag', 'bucket'])
        kwargs = dict()
        if start is not None:
            if not start.startswith(path):
                start = os.path.join(path, start)
            # note: need to use a string just smaller than start, because
            # the list_object API specifies that start is excluded (the first
            # result is *after* start).
            kwargs.update(Marker=Helper.__prev_str(start))
        if end is not None:
            if not end.startswith(path):
                end = os.path.join(path, end)
        if not recursive:
            kwargs.update(Delimiter='/')
            if not path.endswith('/'):
                path += '/'
        kwargs.update(Prefix=path)
        if limit is not None:
            kwargs.update(PaginationConfig={'MaxItems': limit})

        paginator = bucket.meta.client.get_paginator('list_objects')
        for resp in paginator.paginate(Bucket=bucket.name, **kwargs):
            q = []
            if 'CommonPrefixes' in resp and list_dirs:
                q = [S3Obj(f['Prefix'], None, None, None, bucket.name) for f in resp['CommonPrefixes']]
            if 'Contents' in resp and list_objs:
                q += [S3Obj(f['Key'], f['LastModified'], f['Size'], f['ETag'], bucket.name) for f in resp['Contents']]
            # note: even with sorted lists, it is faster to sort(a+b)
            # than heapq.merge(a, b) at least up to 10K elements in each list
            q = sorted(q, key=attrgetter('key'))
            if limit is not None:
                q = q[:limit]
                limit -= len(q)
            for p in q:
                if end is not None and p.key >= end:
                    return
                if suffix is None or p.key.endswith(suffix):
                    yield p

    @staticmethod
    def concatinate(output_url, filelist):

        if len(filelist) == 0:
            return

        client = boto3.session.Session().client('s3')

        (output_bucket, output_key) = Helper.url_to_bucket_key(output_url)    
        mpu = client.create_multipart_upload(Bucket=output_bucket, Key=output_key)
        parts = { "Parts": [] }
        pn=1

        stack = deque()

        for file in reversed(filelist):
            stack.append(file)

        while not len(stack) == 0:
            if len(stack) == 1 or not Helper.is_small_file(stack[-1]):
                current_item = stack.pop()
                if current_item.bucket is not None:
                    parts["Parts"].append(Helper.upload_part_from_cloud(output_bucket, output_key, pn, mpu["UploadId"], {'Bucket': current_item.bucket, 'Key': current_item.key}))
                else:
                    parts["Parts"].append(Helper.upload_part_from_local(output_bucket, output_key, pn, mpu["UploadId"], current_item.key))
                pn += 1
            else:
                small_file_list = []
                small_file_list.append(stack.pop())
                small_file_list.append(stack.pop())

                while len(stack) > 0 and Helper.is_small_file(stack[-1]):
                    small_file_list.append(stack.pop())

                stack.append(Helper.small_file_concat(small_file_list))

        client.complete_multipart_upload(Bucket=output_bucket, Key=output_key, UploadId=mpu["UploadId"], MultipartUpload=parts)

    @staticmethod
    def upload_part_from_cloud(bucket, key, part_number, upload_id, copy_source):
        client = boto3.session.Session().client('s3')
        part = client.upload_part_copy(Bucket=bucket, Key=key, PartNumber=part_number, UploadId=upload_id, CopySource=copy_source)
        return {"PartNumber": part_number, "ETag": part["CopyPartResult"]["ETag"]}

    @staticmethod
    def upload_part_from_local(bucket, key, part_number, upload_id, file):
        client = boto3.session.Session().client('s3')
        with open(file, "rb") as fd:
            part = client.upload_part(Bucket=bucket, Key=key, PartNumber=part_number, UploadId=upload_id, Body=fd)
        os.remove(file)
        return {"PartNumber": part_number, "ETag": part["ETag"]}
                
    @staticmethod
    def is_small_file(file):
        if file.size < (1024*1024*5):
            return True
        return False

    @staticmethod
    def small_file_concat(files):
        output_file = Helper.get_temp_filename()
        s3 = boto3.session.Session().client('s3')
        total_source_size = 0
        with open(output_file,'wb') as wfd:
            for file in files:
                total_source_size += file.size

                if file.bucket is not None:
                    local_file_name = Helper.get_temp_filename()
                    with open(local_file_name, 'wb') as f:
                        s3.download_fileobj(file.bucket, file.key, f)
                else:
                    local_file_name = file.key

                with open(local_file_name,'rb') as fd:
                    shutil.copyfileobj(fd, wfd)

                os.remove(local_file_name)
        
        total_target_size = os.path.getsize(output_file)

        if total_source_size != total_target_size:
            logger = logging.getLogger('%s.%s' % (os.environ.get("appname"), __name__))
            logger.error("file concatination source file size <> concatinated file size (%s) vs (%s) bytes", total_source_size, total_target_size)

        S3Obj = namedtuple('S3Obj', ['key', 'mtime', 'size', 'ETag', 'bucket'])
        return S3Obj(output_file,None, total_target_size, None, None)

    @staticmethod
    def get_temp_filename():
        return os.path.join(tempfile.gettempdir(), next(tempfile._get_candidate_names()))



class FileNotFound(Exception):
    def __init__(self, message):

        # Call the base class constructor with the parameters it needs
        super(FileNotFound, self).__init__(message)
