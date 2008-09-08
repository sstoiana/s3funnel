# jobs.py - Collection of workerpool Job objects
# Copyright (c) 2008 Andrey Petrov
#
# This module is part of s3funnel and is released under
# the MIT license: http://www.opensource.org/licenses/mit-license.php

from workerpool import Job
import boto
import time

import os
import logging
log = logging.getLogger()

# Various exceptions we're expecting
from boto.exception import BotoServerError, BotoClientError
from httplib import IncompleteRead
from socket import error as SocketError

# Jobs

class GetJob(Job):
    "Download the given key from S3."
    def __init__(self, key, config={}):
        self.key = key
        self.retries = config.get('retry', 5)

    def run(self, toolbox):
        k = toolbox.bucket.new_key(self.key)
        wait = 1
        for i in xrange(self.retries):
            try:
                # Note: This creates a file, even if the download fails
                k.get_contents_to_filename(self.key)
                log.info("Got: %s" % self.key)
                return
            except BotoServerError, e:
                log.error("Failed to get: %s" % self.key)
                os.unlink(self.key) # Remove file since download failed
                return
            except (IncompleteRead, SocketError, BotoClientError), e:
                log.warning("Caught exception: %r.\nRetrying..." % e)
                wait = (2 ** wait) / 2.0 # Exponential backoff
                time.sleep(wait)

class PutJob(Job):
    "Upload the given file to S3, where the key corresponds to basename(path)"
    def __init__(self, path, config={}):
        self.path = path
        self.key = self.path
        if not config.get('put_full_path'):
            self.key = os.path.basename(self.key)
        self.retries = config.get('retry', 5)
        self.acl = config.get('acl')
        if self.acl not in ['public-read', 'private']:
            self.acl = 'private'

    def run(self, toolbox):
        k = toolbox.bucket.new_key(self.key)
        wait = 1
        headers = {'x-amz-acl': self.acl}

        for i in xrange(self.retries):
            try:
                # Note: This creates a file, even if the download fails
                k.set_contents_from_filename(self.path, headers)
                log.info("Sent: %s" % self.key)
                return
            except (IncompleteRead, SocketError, BotoClientError, BotoServerError), e:
                log.warning("Caught exception: %r.\nRetrying..." % e)
                wait = (2 ** wait) / 2.0 # Exponential backoff
                time.sleep(wait)
            except IOError, e:
                log.warning("Path does not exist, skipping: %s" % self.path)
                return
        log.error("Failed to put: %s" % self.key)

class DeleteJob(Job):
    "Delete the given key from S3."
    def __init__(self, key, config={}):
        self.key = key
        self.retries = config.get('retry', 5)

    def run(self, toolbox):
        wait = 1
        for i in xrange(self.retries):
            try:
                k = toolbox.bucket.delete_key(self.key)
                log.info("Deleted: %s" % self.key)
                return
            except BotoServerError, e:
                log.error("Failed to delete: %s" % self.key)
                return
            except (IncompleteRead, SocketError, BotoClientError), e:
                log.warning("Caught exception: %r.\nRetrying..." % e)
                wait = (2 ** wait) / 2.0 # Exponential backoff
                time.sleep(wait)


# Helpers

def _create_connection(**config):
    "Given a configuration, return an s3 connection."
    aws_key, aws_secret_key = config.get('aws_key'), config.get('aws_secret_key')
    conn = boto.connect_s3(aws_key, aws_secret_key)
    return conn

# Singlethreaded methods
# TODO: Move these to a separate module?
# TODO: Remove the toolbox_factory parameter, use _create_connection for everything instead?

def show_buckets(**config):
    """
    List all the buckets in the account.
    """
    conn = _create_connection(**config)
    log.info("Listing buckets.")
    r = conn.get_all_buckets()
    for b in r:
        print b.name

def list_bucket(toolbox_factory, **config):
    """
    If no bucket is given, redirect call to ``show_buckets``. Otherwise:
    List all the keys in the bucket created by ``toolbox_factory``.
    Optionally, ``config`` may contain list_marker, list_prefix, list_delimiter
    for additional behaviours.
    """
    if not config.get('bucket'):
        return show_buckets(**config)
    toolbox = toolbox_factory()
    marker = config.get('list_marker', '')
    prefix = config.get('list_prefix', '')
    delimiter = config.get('list_delimiter', '')
    more_results = True
    k = None
    log.info("Listing keys from marker: %s" % marker)
    while more_results:
        try:
            r = toolbox.bucket.get_all_keys(marker=marker, prefix=prefix, delimiter=delimiter)
            for k in r:
                print k.name
            if k:
                marker = k.name
            more_results= r.is_truncated
        except BotoServerError, e:
            log.error("List failed.")
            return
        except (IncompleteRead, SocketError, BotoClientError), e:
            log.warning("Caught exception: %r.\nRetrying..." % e)
            wait = (2 ** wait) / 2.0 # Exponential backoff
            time.sleep(wait)
    log.info("Done listing bucket: %s" % config['bucket'])

def create_bucket(toolbox_factory, **config):
    """
    Create a bucket named ``config['bucket']``.
    """
    name = config['bucket']
    conn = _create_connection(**config)
    try:
        b = conn.create_bucket(name)
        log.info("Created bucket: %s" % name)
    except BotoServerError, e:
        log.error("Bucket `%s` could not be created. Try a different name." % name)
        return

def drop_bucket(toolbox_factory, **config):
    """
    Delete the bucket in the ``toolbox_factory``.
    """
    toolbox = toolbox_factory()
    name = toolbox.bucket.name
    try:
        toolbox.conn.delete_bucket(name)
        log.info("Deleted bucket: %s" % name)
    except BotoServerError, e:
        log.error("Bucket `%s` could not be deleted." % name)
        return

