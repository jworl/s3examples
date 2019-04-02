#!/usr/bin/env python

import logging
import math
import os

from socket import gethostbyname, gaierror
from sys import argv

logging.basicConfig(filename='ceph_crud.log',level=logging.DEBUG)

try:
    from filechunkio import FileChunkIO
    import boto
    import boto.s3.connection
except ModuleNotFoundError as e:
    logging.critical("You need to pip install FileChunkIO and/or boto")
    exit(2)

def _DELETE_BUCKET(B):
    try:
        conn.delete_bucket(B)
        logging.info("{} bucket has been deleted".format(B))
        return(True)
    except boto.exception.S3ResponseError as e:
        if hasattr(e, 'error_code'):
            logging.error("S3 connection error: {}".format(e.error_code))
        else:
            print(e)
        return(False)

def _DNS_TEST(S):
    try:
        dns_resolve = gethostbyname('{}'.format(S))
        logging.info("{} resolves to {}".format(S, dns_resolve))
        return(True)
    except gaierror as e:
        logging.error("DNS is not resolving {}".format(S))
        return(False)

def _FILE_DELETE(B, F):
    try:
        b = conn.get_bucket(B)
        b.delete_key(F)
        logging.info("Deleted {} from {}".format(F, B))
        return(True)
    except boto.exception.S3ResponseError as e:
        if hasattr(e, 'error_code'):
            logging.error("S3 connection error: {}".format(e.error_code))
        else:
            logging.error(e)
        return(False)
    except Exception as e:
        logging.error(e)
        return(False)

def _MAKE_BUCKET(B):
    try:
        bucket = conn.create_bucket('{}'.format(B), policy='public-read')
        logging.info("{} bucket has been created".format(B))
        return(True)
    except gaierror:
        logging.error("Unable to connect to server; is the service running?")
        return(False)
    except boto.exception.S3ResponseError as e:
        if hasattr(e, 'error_code'):
            logging.error("S3 connection error: {}".format(e.error_code))
        else:
            logging.error(e)
        return(False)

def _MULTI_UPLOAD(B, F):
    try:
        b = conn.get_bucket(B)
        file_size = os.stat(F).st_size
        mp = b.initiate_multipart_upload(os.path.basename(F))
        chunk_size = 524288000
        chunk_count = int(math.ceil(file_size / float(chunk_size)))

        for i in range(chunk_count):
            offset = chunk_size * i
            bytes = min(chunk_size, file_size - offset)
            with FileChunkIO(F, 'r', offset=offset, bytes=bytes) as fp:
                mp.upload_part_from_file(fp, part_num=i + 1)

        mp.complete_upload()
        logging.info("Completed {} upload to {}".format(F, B))
    except boto.exception.S3ResponseError as e:
        if hasattr(e, 'error_code'):
            logging.error("S3 connection error: {}".format(e.error_code))
        else:
            logging.error(e)
        return(False)
    except FileNotFoundError as e:
        logging.error(e)
        return(False)
    except Exception as e:
        logging.error(e)
        return(False)

def _SIMPLE_UPLOAD(B, F, N):
    try:
        b = conn.get_bucket(B)
        k = b.new_key(N)
        k.set_contents_from_filename(F)
        logging.info("completed simple upload of {} to {}".format(F, B))
        return(True)
    except boto.exception.S3ResponseError as e:
        if hasattr(e, 'error_code'):
            logging.error("S3 connection error: {}".format(e.error_code))
        else:
            logging.error(e)
        return(False)
    except FileNotFoundError as e:
        logging.error(e)
        return(False)
    except Exception as e:
        logging.error(e)
        return(False)

if __name__ == "__main__":
    if len(argv) != 2:
        logging.critical("usage: python {} server-name".format(argv[0]))
        exit(2)
    else:
        s3gw = argv[1]
        bucket = "zabbix-crud"
        small_file = '/home/jworl/Pictures/ceph.jpeg'
        med_file = '/home/jworl/Videos/commute.mp4'
        big_file = '/home/jworl/Videos/GOPR0128.MP4'
        all_tests = ['small_test.jpeg', 'medium_test.mp4', 'GOPR0128.MP4']

    access_key = "YOURACCESSKEYHERE7QY"
    secret_key = "YOURSECRETKEYHEREdK9F6kp9THISONEISLONGER"

    conn = boto.connect_s3(
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key,
            host = '{0}'.format(s3gw),
            is_secure=False,        # uncomment if you are not using ssl
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),
            )

    if _DNS_TEST(s3gw) is False:
        print("{} not successfully resolving".format(s3gw))
        exit(2)

    if _MAKE_BUCKET(bucket) is False:
        print("Unable to create bucket")
        exit(2)

    if _SIMPLE_UPLOAD(bucket, small_file, 'small_test.jpeg') is False:
        print("Small file upload failed")
        exit(2)

    if _SIMPLE_UPLOAD(bucket, med_file, 'medium_test.mp4') is False:
        print("Medium file upload failed")
        exit(2)

    if _MULTI_UPLOAD(bucket, big_file) is False:
        print("Large multipart upload failed")
        exit(2)

    for fn in all_tests:
        if _FILE_DELETE(bucket, "{}".format(fn)) is False:
            print("{} file delete failed".format(fn))
            exit(2)

    if _DELETE_BUCKET(bucket) is False:
        print("DELETE_BUCKET failed")
        exit(2)
