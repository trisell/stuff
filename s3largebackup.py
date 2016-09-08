#! /usr/bin/python
import sys
import os
import math
import argparse
import subprocess
import hashlib
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key

parser = argparse.ArgumentParser()

parser.add_argument('-f', '--file',action='store', dest='file',
                    help='Location of file to be shipped to S3')
parser.add_argument('-b', '--bucket',action='store', dest='bucket',
                    help='S3 destination bucket for file.') 
parser.add_argument('-k', '--key',action='store', dest='access_key',
                    help='AWS key')
parser.add_argument('-sk', '--secret_key',action='store', dest='secret_key',
                    help='AWS Secret Key')
parser.add_argument('-l', '--location',action='store', dest='location',
                    help='Internal bucket folder location')
args = parser.parse_args()

def calculate_multipart_etag(source_path, expected=None):

    """
    calculates a multipart upload etag for amazon s3
    Arguments:
    source_path -- The file to calculate the etage for
    Keyword Arguments:
    expected -- If passed a string, the string will be compared to the resulting etag and raise an exception if they don't match
    """

    md5s = []
    source_path_list = source_path.split('/')
    print 'calculating md5' 
    new_etag = subprocess.check_output(['md5sum', source_path]).split(' ')[0]

    if expected:
        if not expected==new_etag:
            raise ValueError('new etag %s does not match expected %s' % (new_etag, expected))
        else:
            print "File was uploaded successfully, and the file MD5s match."
    return new_etag



def upload_file(s3, bucketname, file_path, folder):

    b = s3.get_bucket(bucketname)

    filename = os.path.basename(file_path)
    file_split = filename.split("/")
    #k = b.new_key(filename)
    k = b.new_key(folder + file_split[-1])
    log( ' saving in S3 as: ' + str(k.key), verbose )

    mp = bucket.initiate_multipart_upload(str(k.key))
    #log( "Initiating multipart upload of: " + filename, verbose )

    #mp = b.initiate_multipart_upload(filename)

    source_size = os.stat(file_path).st_size
    bytes_per_chunk = 5000*1024*1024
    chunks_count = int(math.ceil(source_size / float(bytes_per_chunk)))

    for i in range(chunks_count):
        offset = i * bytes_per_chunk
        remaining_bytes = source_size - offset
        bytes = min([bytes_per_chunk, remaining_bytes])
        part_num = i + 1

        print "uploading part " + str(part_num) + " of " + str(chunks_count)

        with open(file_path, 'r') as fp:
                fp.seek(offset)
                mp.upload_part_from_file(fp=fp, part_num=part_num, size=bytes)

    if len(mp.get_all_parts()) == chunks_count:
        mp.complete_upload()
        print "upload_file done"
    else:
        mp.cancel_upload()
        print "upload_file failed"

if __name__ == "__main__":

    bucketname = args.bucket

    filepath = args.file
    
    folder = args.location

    s3 = boto.connect_s3(args.access_key, args.secret_key)

    upload_file(s3, bucketname, filepath, folder)
