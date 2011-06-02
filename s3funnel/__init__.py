import logging
log = logging.getLogger(__name__)

import boto
import workerpool

from boto.exception import BotoServerError, BotoClientError, S3ResponseError
from httplib import IncompleteRead
from socket import error as SocketError
from Queue import Queue, Empty
from exceptions import FunnelError

from jobs import GetJob, PutJob, DeleteJob, CopyJob

__all__ = ['GetJob','PutJob','DeleteJob','S3ToolBox','BucketFunnel']

# Helpers

def collapse_queue(q):
    "Given a queue, return all the items currently in it."
    items = []
    try:
        while 1:
            i = q.get(block=False)
            q.task_done()
            if isinstance(i, FunnelError):
                raise i
            items.append(i)
    except Empty:
        return items

# Factories used to instantiate a workerpool

class S3ToolBox(object):
    """
    Container object for resources needed to access S3.
    This includes a connection to S3 and an instance of the bucket.
    """
    def __init__(self, aws_key, aws_secret_key, secure):
        self.aws_key = aws_key
        self.aws_secret_key = aws_secret_key
        self.secure = secure

        self.reset()

    def reset(self):
        self.conn = None
        self.buckets = {}

    def get_conn(self):
        "Get an S3 connection instance (cached)"
        if self.conn: return self.conn

        log.debug("Starting new S3 connection.")
        self.conn = boto.connect_s3(self.aws_key, self.aws_secret_key, is_secure=self.secure)
        return self.conn

    def get_bucket(self, name):
        "Get an S3 bucket instance with the given ``name`` (cached)"
        bucket = self.buckets.get(name)
        if bucket: return bucket

        conn = self.get_conn()
        log.debug("Getting bucket instance: %s" % name)
        try:
            bucket = conn.get_bucket(name)
        except BotoServerError, e:
            raise FunnelError("Bucket not found: %s" % name, key=name)

        self.buckets[name] = bucket
        return bucket

# Abstraction objects

class S3Funnel(object):
    """
    Abstraction object for manipulating S3.

    This class is not thread-safe but individual methods will use multiple
    threads to accomplish the request (where appropriate).

    If a ``pool`` is not given then a workerpool will be created at the first
    need of one. Optional pool config settings include:

    numthreads [Default: 5]
        Number of threads to create the pool with

    maxjobs [Default: numthreads * 2]
        Number of jobs to accept into the queue at a time (block if full)
    """
    def __init__(self, aws_key=None, aws_secret_key=None, pool=None, **config):
        self.aws_key = aws_key or config.get('aws_key')
        self.aws_secret_key = aws_secret_key or config.get('aws_secret_key')

        self.config = config
        self.numthreads = config.get('numthreads', 5)
        self.maxjobs = config.get('maxjobs', self.numthreads*2)
        self.secure = config.get('secure', True)

        self.pool = pool
        self.conn = None
        self.buckets = {}

        toolbox = S3ToolBox(self.aws_key, self.aws_secret_key, self.secure)
        self._get_conn = toolbox.get_conn
        self._get_bucket = toolbox.get_bucket

    def shutdown(self):
        if self.pool:
            self.pool.shutdown()

    def _get_pool(self):
        "Get a worker pool (cached)"
        if self.pool: return self.pool

        def toolbox_factory():
            return S3ToolBox(self.aws_key, self.aws_secret_key, self.secure)
        def worker_factory(job_queue):
            return workerpool.EquippedWorker(job_queue, toolbox_factory)

        log.info("Starting pool with %d threads." % self.numthreads)
        self.pool = workerpool.WorkerPool(self.numthreads, maxjobs=self.maxjobs, worker_factory=worker_factory)
        return self.pool

    def show_buckets(self):
        "Return an iterator of all the available bucket names"
        conn = self._get_conn()
        return (i.name for i in conn.get_all_buckets())

    def create_bucket(self, name, **config):
        "Create bucket named ``name``"
        conn = self._get_conn()
        try:
            b = conn.create_bucket(name)
            log.info("Created bucket: %s" % name)
        except BotoServerError, e:
            raise FunnelError("Bucket could not be created: %s" % name, key=name)

    def drop_bucket(self, name, **config):
        "Delete bucket named ``name``"
        conn = self._get_conn()
        try:
            conn.delete_bucket(name)
            log.info("Deleted bucket: %s" % name)
        except BotoServerError, e:
            raise FunnelError("Bucket could not be deleted: %s" % name, key=name)

    def list_bucket(self, name, marker=None, prefix=None, delimiter=None, **config):
        "Return an iterator over all the keys in bucket named ``name``"
        bucket = self._get_bucket(name)

        marker = marker or config.get('list_marker') or ''
        prefix = prefix or config.get('list_prefix') or ''
        delimiter = delimiter or config.get('list_delimiter') or ''

        more_results = True
        k = None

        log.info("Listing keys from marker: %s" % marker)
        while more_results:
            try:
                r = bucket.get_all_keys(marker=marker, prefix=prefix, delimiter=delimiter)
                for k in r:
                    yield k.name
                if k:
                    marker = k.name
                more_results= r.is_truncated
            except BotoServerError, e:
                raise FunnelError("Failed to list bucket: %s" % name, key=name)
            except (IncompleteRead, SocketError, BotoClientError), e:
                log.warning("Caught exception: %r.\nRetrying..." % e)
                time.sleep(1.0)

        log.info("Done listing bucket: %s" % name)

    def delete(self, bucket, ikeys, retry=5, **config):
        """
        Given an iterator of key names, delete these keys from the current bucket.
        Return a list of failed keys (if any).
        """
        # Setup local config for this request
        c = {}
        c.update(config)
        c['retry'] = retry

        failed = Queue()
        pool = self._get_pool()
        for k in ikeys:
            j = DeleteJob(bucket, k, failed, c)
            pool.put(j)
        pool.join()

        return collapse_queue(failed)

    def get(self, bucket, ikeys, retry=5, **config):
        """
        Given an iterator of key names, download these files from the current bucket.
        Return a list of failed keys (if any).
        """
        # Setup local config for this request
        c = {}
        c.update(config)
        c['retry'] = retry

        failed = Queue()
        pool = self._get_pool()
        for k in ikeys:
            j = GetJob(bucket, k, failed, c)
            pool.put(j)
        pool.join()

        return collapse_queue(failed)

    def put(self, bucket, ikeys, retry=5, acl='public-read', **config):
        """
        Given an iterator of file paths, put these files into the current bucket.
        Return a list of failed keys (if any).
        """
        # Setup local config for this request
        c = {}
        c.update(config)
        c['retry'] = retry
        c['acl'] = acl

        failed = Queue()
        pool = self._get_pool()
        for k in ikeys:
            j = PutJob(bucket, k, failed, c)
            pool.put(j)
        pool.join()

        return collapse_queue(failed)

    def copy(self, bucket, ikeys, retry=5, acl='public-read', **config):
        """
        Given an iterator of file paths, copy these files into the current bucket from source bucket
        Return a list of failed keys (if any).
        """
        # Setup local config for this request
        c = {}
        c.update(config)
        c['retry'] = retry
        c['acl'] = acl

        failed = Queue()
        pool = self._get_pool()
        for k in ikeys:
            j = CopyJob(bucket, k, failed, c)
            pool.put(j)
        pool.join()

        return collapse_queue(failed)
