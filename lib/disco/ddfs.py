"""
:mod:`disco.ddfs` --- Client interface for Disco Distributed Filesystem
=======================================================================

See also: :ref:`DDFS`.

.. note::

        Parameters below which are indicated as tags can be specified as
        a `tag://` URL, or the name of the tag.
"""
import os, re, random
from cStringIO import StringIO
from urllib import urlencode

from disco.comm import upload, download, json, open_remote
from disco.error import CommError
from disco.settings import DiscoSettings
from disco.util import iterify, partition, urlsplit

unsafe_re = re.compile(r'[^A-Za-z0-9_\-@:]')

def canonizetags(tags):
    return [tagname(tag) for tag in iterify(tags)]

def tagname(tag):
    if isinstance(tag, list):
        if tag:
            return tagname(tag[0])
    elif tag.startswith('tag://'):
        return tag[6:]
    elif '://' not in tag:
        return tag

class DDFS(object):
    """
    Opens and encapsulates a connection to a DDFS master.

    :param master: address of the master,
                   for instance ``disco://localhost``.
    """
    def __init__(self, master=None,
                 proxy=None,
                 settings=DiscoSettings(),
                 token=None):
        self.proxy  = proxy or settings['DISCO_PROXY']
        self.master = self.proxy or master or settings['DISCO_MASTER']
        self.token = token

    @classmethod
    def safe_name(cls, name):
        return unsafe_re.sub('_', name)

    @classmethod
    def blob_name(cls, url):
        return url.split('/')[-1].split('$')[0]

    def attrs(self, tag):
        """Get a list of the attributes of the tag ``tag`` and their values."""
        t = self._download('/ddfs/tag/%s' % tagname(tag))
        if isinstance(t, dict) and t.has_key('user-data'):
            return t['user-data']
        else:
            return t

    def blobs(self, tag, ignore_missing=True):
        """
        Walks the tag graph starting at `tag`.

        Yields only the terminal nodes of the graph (`blobs`).

        :type  ignore_missing: bool
        :param ignore_missing: Whether or not missing tags will raise a
                               :class:`disco.error.CommError`.
        """
        for path, tags, blobs in self.walk(tag, ignore_missing=ignore_missing):
            if tags != blobs:
                for replicas in blobs:
                    yield replicas

    def delattr(self, tag, attr):
        """Delete the attribute ``attr` of the tag ``tag``."""
        return self._download('/ddfs/tag/%s/%s' % (tagname(tag), attr), method='DELETE')

    def delete(self, tag):
        """Delete ``tag``."""
        return self._download('/ddfs/tag/%s' % tagname(tag), method='DELETE')

    def exists(self, tag):
        """Returns whether or not ``tag`` exists."""
        try:
            if open_remote('%s/ddfs/tag/%s' % (self.master, tagname(tag))):
                return True
        except CommError, e:
            if e.code not in (403, 404):
                raise
        return False

    def findtags(self, tags=None, ignore_missing=True):
        import sys
        """
        Finds the nodes of the tag graph starting at `tags`.

        Yields a 3-tuple `(tag, tags, blobs)`.
        """
        seen = set()

        tag_queue = canonizetags(tags)

        for tag in tag_queue:
            if tag not in seen:
                try:
                    urls        = self.get(tag).get('urls', [])
                    tags, blobs = partition(urls, tagname)
                    tags        = canonizetags(tags)
                    yield tag, tags, blobs

                    tag_queue += tags
                    seen.add(tag)
                except CommError, e:
                    if ignore_missing and e.code == 404:
                        tags = blobs = ()
                    else:
                        raise

    def get(self, tag):
        """Return the tag object stored at ``tag``."""
        return self._download('/ddfs/tag/%s' % tagname(tag))

    def getattr(self, tag, attr):
        """Return the value of the attribute ``attr` of the tag ``tag``."""
        return self._download('/ddfs/tag/%s/%s' % (tagname(tag), attr))

    def list(self, prefix=''):
        """Return a list of all tags starting wtih ``prefix``."""
        return self._download('/ddfs/tags/%s' % prefix)

    def pull(self, tag, blobfilter=lambda x: True):
        for repl in self.get(tag)['urls']:
            if blobfilter(self.blob_name(repl[0])):
                random.shuffle(repl)
                for url in repl:
                    try:
                        yield open_remote(url)
                        break
                    except CommError, error:
                        continue
                else:
                    raise error

    def push(self, tag, files, replicas=None, retries=10, delayed=False):
        """
        Pushes a bunch of files to ddfs and tags them with `tag`.

        :type  files: a list of ``paths``, ``(path, name)``-tuples, or
                      ``(fileobject, name)``-tuples.
        :param files: the files to push as blobs to DDFS.
                      If names are provided,
                      they will be used as prefixes by DDFS for the blobnames.
                      Names may only contain chars in ``r'[^A-Za-z0-9_\-@:]'``.
        """
        def aim(tuple_or_path):
            if isinstance(tuple_or_path, basestring):
                source = tuple_or_path
                target = self.safe_name(os.path.basename(source))
            else:
                source, target = tuple_or_path
            return source, target

        urls = [self._push(aim(f), replicas=replicas, retries=retries)
                for f in files]
        return self.tag(tag, urls, delayed=delayed), urls

    def put(self, tag, urls):
        """Put the list of ``urls`` to the tag ``tag``.

        .. warning::

                Generally speaking, concurrent applications should use
                :meth:`DDFS.tag` instead.
        """
        return self._upload('%s/ddfs/tag/%s' % (self.master, tagname(tag)),
                            StringIO(json.dumps(urls)))

    def setattr(self, tag, attr, val):
        """Set the value of the attribute ``attr` of the tag ``tag``."""
        return self._upload('%s/ddfs/tag/%s/%s' % (self.master,
                                                   tagname(tag),
                                                   attr),
                            StringIO(json.dumps(val)))

    def tag(self, tag, urls, delayed=False):
        """Append the list of ``urls`` to the ``tag``."""
        return self._download('/ddfs/tag/%s?%s' %
            (tagname(tag), "delayed=1" if delayed else ""),
                json.dumps(urls))

    def tarblobs(self, tarball, compress=True, include=None, exclude=None):
        import tarfile, sys, gzip, os

        tar = tarfile.open(tarball)

        for member in tar:
            if member.isfile():
                if include and include not in member.name:
                    continue
                if exclude and exclude in member.name:
                    continue
                if compress:
                    buf    = StringIO()
                    gz     = gzip.GzipFile(mode='w', compresslevel=2, fileobj=buf)
                    size   = self._copy(tar.extractfile(member), gz)
                    gz.close()
                    buf.seek(0)
                    suffix = '_gz'
                else:
                    buf    = tar.extractfile(member)
                    size   = len(buf)
                    suffix = ''
                name = DDFS.safe_name(member.name) + suffix
                yield name, buf, size

    def walk(self, tag, ignore_missing=True, tagpath=()):
        """
        Walks the tag graph starting at `tag`.

        Yields a 3-tuple `(tagpath, tags, blobs)`.

        :type  ignore_missing: bool
        :param ignore_missing: Whether or not missing tags will raise a
                               :class:`disco.error.CommError`.
        """
        tagpath += (tagname(tag),)

        try:
            urls        = self.get(tag).get('urls', [])
            tags, blobs = partition(urls, tagname)
            tags        = canonizetags(tags)
            yield tagpath, tags, blobs
        except CommError, e:
            if ignore_missing and e.code == 404:
                tags = blobs = ()
                yield tagpath, tags, blobs
            else:
                yield tagpath, None, None
                raise e

        for next_tag in tags:
            for child in self.walk(next_tag,
                                   ignore_missing=ignore_missing,
                                   tagpath=tagpath):
                yield child

    def _copy(self, src, dst):
        s = 0
        while True:
            b = src.read(8192)
            if not b:
                break
            s += len(b)
            dst.write(b)
        return s

    def _maybe_proxy(self, url, method='GET'):
        if self.proxy:
            scheme, (host, port), path = urlsplit(url)
            return '%s/proxy/%s/%s/%s' % (self.proxy, host, method, path)
        return url

    def _push(self, (source, target), replicas=None, exclude=[], **kwargs):
        qs = urlencode([(k, v) for k, v in (('exclude', ','.join(exclude)),
                                            ('replicas', replicas)) if v])
        urls = self._download('/ddfs/new_blob/%s?%s' % (target, qs))

        try:
            return [json.loads(url)
                    for url in self._upload(urls, source, **kwargs)]
        except CommError, e:
            scheme, (host, port), path = urlsplit(e.url)
            return self._push((source, target),
                              replicas=replicas,
                              exclude=exclude + [host],
                              **kwargs)

    def _download(self, url, data=None, method='GET'):
        response = download(self.master + url,
                            data=data,
                            method=method,
                            token=self.token)
        return json.loads(response)

    def _upload(self, urls, source, **kwargs):
        urls = [self._maybe_proxy(url, method='PUT') for url in iterify(urls)]
        return upload(urls, source, token=self.token, **kwargs)
