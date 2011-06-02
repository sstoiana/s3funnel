class FunnelError(Exception):
    """
    Base exception for all s3funnel errors

    Contains a ``message`` and optionally a ``key`` name for the key which
    the error is associated with. This is relative to the type of request --
    may be a bucket name or an item key inside a bucket.
    """
    def __init__(self, message, key=None):
        self._message = message
        self.key = key
    def __str__(self):
        return repr(self._message)