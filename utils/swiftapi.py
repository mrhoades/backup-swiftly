#!/usr/bin/python -u
# Copyright (c) 2010-2011 OpenStack, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from errno import EEXIST, ENOENT
from hashlib import md5
from os import  makedirs, utime
from os.path import  getmtime, getsize, isdir, join
from Queue import Empty
from sys import  exc_info, stdout
from threading import  Thread
from time import sleep
from traceback import format_exception

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Inclusion of swift.common.client for convenience of single file distribution

import socket
from urllib import quote as _quote
from urlparse import urlparse, urlunparse, urljoin

try:
    from eventlet.green.httplib import HTTPException, HTTPSConnection
except ImportError:
    from httplib import HTTPException, HTTPSConnection

try:
    from eventlet import sleep
except ImportError:
    from time import sleep

try:
    from utils.swift.common.bufferedhttp \
        import BufferedHTTPConnection as HTTPConnection
except ImportError:
    try:
        from eventlet.green.httplib import HTTPConnection
    except ImportError:
        from httplib import HTTPConnection


def quote(value, safe='/'):
    """
    Patched version of urllib.quote that encodes utf8 strings before quoting
    """
    if isinstance(value, unicode):
        value = value.encode('utf8')
    return _quote(value, safe)


# look for a real json parser first
try:
    # simplejson is popular and pretty good
    from simplejson import loads as json_loads
    from simplejson import dumps as json_dumps
except ImportError:
    # 2.6 will have a json module in the stdlib
    from json import loads as json_loads
    from json import dumps as json_dumps

class ClientException(Exception):

    def __init__(self, msg, http_scheme='', http_host='', http_port='',
                 http_path='', http_query='', http_status=0, http_reason='',
                 http_device=''):
        Exception.__init__(self, msg)
        self.msg = msg
        self.http_scheme = http_scheme
        self.http_host = http_host
        self.http_port = http_port
        self.http_path = http_path
        self.http_query = http_query
        self.http_status = http_status
        self.http_reason = http_reason
        self.http_device = http_device

    def __str__(self):
        a = self.msg
        b = ''
        if self.http_scheme:
            b += '%s://' % self.http_scheme
        if self.http_host:
            b += self.http_host
        if self.http_port:
            b += ':%s' % self.http_port
        if self.http_path:
            b += self.http_path
        if self.http_query:
            b += '?%s' % self.http_query
        if self.http_status:
            if b:
                b = '%s %s' % (b, self.http_status)
            else:
                b = str(self.http_status)
        if self.http_reason:
            if b:
                b = '%s %s' % (b, self.http_reason)
            else:
                b = '- %s' % self.http_reason
        if self.http_device:
            if b:
                b = '%s: device %s' % (b, self.http_device)
            else:
                b = 'device %s' % self.http_device
        return b and '%s: %s' % (a, b) or a



def http_connection(url, proxy=None):
    """
    Make an HTTPConnection or HTTPSConnection

    :param url: url to connect to
    :param proxy: proxy to connect through, if any; None by default; str of the
                  format 'http://127.0.0.1:8888' to set one
    :returns: tuple of (parsed url, connection object)
    :raises ClientException: Unable to handle protocol scheme
    """
    parsed = urlparse(url)
    proxy_parsed = urlparse(proxy) if proxy else None
    if parsed.scheme == 'http':
        conn = HTTPConnection((proxy_parsed if proxy else parsed).netloc)
    elif parsed.scheme == 'https':
        conn = HTTPSConnection((proxy_parsed if proxy else parsed).netloc)
    else:
        raise ClientException('Cannot handle protocol scheme %s for url %s' %
                              (parsed.scheme, repr(url)))
    if proxy:
        conn._set_tunnel(parsed.hostname, parsed.port)
    return parsed, conn


def get_conn(options):
    """
    Return a connection building it from the options.
    """
    return Connection(options['auth'],
                      options['user'],
                      options['key'],
                      options['snet'],
                      auth_version=options['auth_version'])


def _get_auth_v1_0(url, user, key, snet):
    parsed, conn = http_connection(url)
    conn.request('GET', parsed.path, '',
                 {'X-Auth-User': user, 'X-Auth-Key': key})
    resp = conn.getresponse()
    resp.read()
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Auth GET failed', http_scheme=parsed.scheme,
                http_host=conn.host, http_port=conn.port,
                http_path=parsed.path, http_status=resp.status,
                http_reason=resp.reason)
    url = resp.getheader('x-storage-url')
    if snet:
        parsed = list(urlparse(url))
        # Second item in the list is the netloc
        parsed[1] = 'snet-' + parsed[1]
        url = urlunparse(parsed)
    return url, resp.getheader('x-storage-token',
                                                resp.getheader('x-auth-token'))


def _get_auth_v2_0(url, user, key, snet):
    if ':' in user:
        tenant, user = user.split(':')
    else:
        tenant = user

    def json_request(method, token_url, **kwargs):
        kwargs.setdefault('headers', {})
        if 'body' in kwargs:
            kwargs['headers']['Content-Type'] = 'application/json'
            kwargs['body'] = json_dumps(kwargs['body'])
        parsed, conn = http_connection(token_url)
        conn.request(method, parsed.path, **kwargs)
        resp = conn.getresponse()
        body = resp.read()
        if body:
            try:
                body = json_loads(body)
            except ValueError:
                pass
        else:
            body = None
        if resp.status < 200 or resp.status >= 300:
            raise ClientException('Auth GET failed', http_scheme=parsed.scheme,
                                  http_host=conn.host,
                                  http_port=conn.port,
                                  http_path=parsed.path,
                                  http_status=resp.status,
                                  http_reason=resp.reason)
        return resp, body
    body = {"auth": {"tenantName": tenant,
                     "passwordCredentials":
                         {"username": user, "password": key}}}
    token_url = urljoin(url, "tokens")
    resp, body = json_request("POST", token_url, body=body)
    token_id = None
    try:
        url = None
        catalogs = body['access']['serviceCatalog']
        for service in catalogs:
            if service['type'] == 'object-store':
                url = service['endpoints'][0]['publicURL']
        token_id = body['access']['token']['id']
        if not url:
            raise ClientException("There is no object-store endpoint " \
                                  "on this auth server.")
    except(KeyError, IndexError):
        raise ClientException("Error while getting answers from auth server")

    if snet:
        parsed = list(urlparse(url))
        # Second item in the list is the netloc
        parsed[1] = 'snet-' + parsed[1]
        url = urlunparse(parsed)

    return url, token_id


def get_auth(url, user, key, snet=False, auth_version="1.0"):
    """
    Get authentication/authorization credentials.

    The snet parameter is used for Rackspace's ServiceNet internal network
    implementation. In this function, it simply adds *snet-* to the beginning
    of the host name for the returned storage URL. With Rackspace Cloud Files,
    use of this network path causes no bandwidth charges but requires the
    client to be running on Rackspace's ServiceNet network.

    :param url: authentication/authorization URL
    :param user: user to authenticate as
    :param key: key or password for authorization
    :param snet: use SERVICENET internal network (see above), default is False
    :param auth_version: OpenStack authentication version (default is 1.0)
    :returns: tuple of (storage URL, auth token)
    :raises ClientException: HTTP GET request to auth URL failed
    """
    if auth_version == "1.0" or auth_version == "1":
        return _get_auth_v1_0(url, user, key, snet)
    elif auth_version == "2.0" or auth_version == "2":
        return _get_auth_v2_0(url, user, key, snet)


def get_account(url, token, marker=None, limit=None, prefix=None,
                http_conn=None, full_listing=False):
    """
    Get a listing of containers for the account.

    :param url: storage URL
    :param token: auth token
    :param marker: marker query
    :param limit: limit query
    :param prefix: prefix query
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :param full_listing: if True, return a full listing, else returns a max
                         of 10000 listings
    :returns: a tuple of (response headers, a list of containers) The response
              headers will be a dict and all header names will be lowercase.
    :raises ClientException: HTTP GET request failed
    """
    if not http_conn:
        http_conn = http_connection(url)
    if full_listing:
        rv = get_account(url, token, marker, limit, prefix, http_conn)
        listing = rv[1]
        while listing:
            marker = listing[-1]['name']
            listing = \
                get_account(url, token, marker, limit, prefix, http_conn)[1]
            if listing:
                rv[1].extend(listing)
        return rv
    parsed, conn = http_conn
    qs = 'format=json'
    if marker:
        qs += '&marker=%s' % quote(marker)
    if limit:
        qs += '&limit=%d' % limit
    if prefix:
        qs += '&prefix=%s' % quote(prefix)
    conn.request('GET', '%s?%s' % (parsed.path, qs), '',
                 {'X-Auth-Token': token})
    resp = conn.getresponse()
    resp_headers = {}
    for header, value in resp.getheaders():
        resp_headers[header.lower()] = value
    if resp.status < 200 or resp.status >= 300:
        resp.read()
        raise ClientException('Account GET failed', http_scheme=parsed.scheme,
                http_host=conn.host, http_port=conn.port,
                http_path=parsed.path, http_query=qs, http_status=resp.status,
                http_reason=resp.reason)
    if resp.status == 204:
        resp.read()
        return resp_headers, []
    return resp_headers, json_loads(resp.read())


def head_account(url, token, http_conn=None):
    """
    Get account stats.

    :param url: storage URL
    :param token: auth token
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :returns: a dict containing the response's headers (all header names will
              be lowercase)
    :raises ClientException: HTTP HEAD request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url)
    conn.request('HEAD', parsed.path, '', {'X-Auth-Token': token})
    resp = conn.getresponse()
    resp.read()
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Account HEAD failed', http_scheme=parsed.scheme,
                http_host=conn.host, http_port=conn.port,
                http_path=parsed.path, http_status=resp.status,
                http_reason=resp.reason)
    resp_headers = {}
    for header, value in resp.getheaders():
        resp_headers[header.lower()] = value
    return resp_headers


def post_account(url, token, headers, http_conn=None):
    """
    Update an account's metadata.

    :param url: storage URL
    :param token: auth token
    :param headers: additional headers to include in the request
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :raises ClientException: HTTP POST request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url)
    headers['X-Auth-Token'] = token
    conn.request('POST', parsed.path, '', headers)
    resp = conn.getresponse()
    resp.read()
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Account POST failed',
                              http_scheme=parsed.scheme,
                              http_host=conn.host,
                              http_port=conn.port,
                              http_path=parsed.path,
                              http_status=resp.status,
                              http_reason=resp.reason)


def get_container(url, token, container, marker=None, limit=None,
                  prefix=None, delimiter=None, http_conn=None,
                  full_listing=False):
    """
    Get a listing of objects for the container.

    :param url: storage URL
    :param token: auth token
    :param container: container name to get a listing for
    :param marker: marker query
    :param limit: limit query
    :param prefix: prefix query
    :param delimeter: string to delimit the queries on
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :param full_listing: if True, return a full listing, else returns a max
                         of 10000 listings
    :returns: a tuple of (response headers, a list of objects) The response
              headers will be a dict and all header names will be lowercase.
    :raises ClientException: HTTP GET request failed
    """
    if not http_conn:
        http_conn = http_connection(url)
    if full_listing:
        rv = get_container(url, token, container, marker, limit, prefix,
                           delimiter, http_conn)
        listing = rv[1]
        while listing:
            if not delimiter:
                marker = listing[-1]['name']
            else:
                marker = listing[-1].get('name', listing[-1].get('subdir'))
            listing = get_container(url, token, container, marker, limit,
                                    prefix, delimiter, http_conn)[1]
            if listing:
                rv[1].extend(listing)
        return rv
    parsed, conn = http_conn
    path = '%s/%s' % (parsed.path, quote(container))
    qs = 'format=json'
    if marker:
        qs += '&marker=%s' % quote(marker)
    if limit:
        qs += '&limit=%d' % limit
    if prefix:
        qs += '&prefix=%s' % quote(prefix)
    if delimiter:
        qs += '&delimiter=%s' % quote(delimiter)
    conn.request('GET', '%s?%s' % (path, qs), '', {'X-Auth-Token': token})
    resp = conn.getresponse()
    if resp.status < 200 or resp.status >= 300:
        resp.read()
        raise ClientException('Container GET failed',
                http_scheme=parsed.scheme, http_host=conn.host,
                http_port=conn.port, http_path=path, http_query=qs,
                http_status=resp.status, http_reason=resp.reason)
    resp_headers = {}
    for header, value in resp.getheaders():
        resp_headers[header.lower()] = value
    if resp.status == 204:
        resp.read()
        return resp_headers, []
    return resp_headers, json_loads(resp.read())


def head_container(url, token, container, http_conn=None):
    """
    Get container stats.

    :param url: storage URL
    :param token: auth token
    :param container: container name to get stats for
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :returns: a dict containing the response's headers (all header names will
              be lowercase)
    :raises ClientException: HTTP HEAD request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url)
    path = '%s/%s' % (parsed.path, quote(container))
    conn.request('HEAD', path, '', {'X-Auth-Token': token})
    resp = conn.getresponse()
    resp.read()
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Container HEAD failed',
                http_scheme=parsed.scheme, http_host=conn.host,
                http_port=conn.port, http_path=path, http_status=resp.status,
                http_reason=resp.reason)
    resp_headers = {}
    for header, value in resp.getheaders():
        resp_headers[header.lower()] = value
    return resp_headers


def put_container(url, token, container, headers=None, http_conn=None):
    """
    Create a container

    :param url: storage URL
    :param token: auth token
    :param container: container name to create
    :param headers: additional headers to include in the request
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :raises ClientException: HTTP PUT request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url)
    path = '%s/%s' % (parsed.path, quote(container))
    if not headers:
        headers = {}
    headers['X-Auth-Token'] = token
    conn.request('PUT', path, '', headers)
    resp = conn.getresponse()
    resp.read()
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Container PUT failed',
                http_scheme=parsed.scheme, http_host=conn.host,
                http_port=conn.port, http_path=path, http_status=resp.status,
                http_reason=resp.reason)


def post_container(url, token, container, headers, http_conn=None):
    """
    Update a container's metadata.

    :param url: storage URL
    :param token: auth token
    :param container: container name to update
    :param headers: additional headers to include in the request
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :raises ClientException: HTTP POST request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url)
    path = '%s/%s' % (parsed.path, quote(container))
    headers['X-Auth-Token'] = token
    conn.request('POST', path, '', headers)
    resp = conn.getresponse()
    resp.read()
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Container POST failed',
                http_scheme=parsed.scheme, http_host=conn.host,
                http_port=conn.port, http_path=path, http_status=resp.status,
                http_reason=resp.reason)


def delete_container(url, token, container, http_conn=None):
    """
    Delete a container

    :param url: storage URL
    :param token: auth token
    :param container: container name to delete
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :raises ClientException: HTTP DELETE request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url)
    path = '%s/%s' % (parsed.path, quote(container))
    conn.request('DELETE', path, '', {'X-Auth-Token': token})
    resp = conn.getresponse()
    resp.read()
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Container DELETE failed',
                http_scheme=parsed.scheme, http_host=conn.host,
                http_port=conn.port, http_path=path, http_status=resp.status,
                http_reason=resp.reason)


def get_object(url, token, container, name, http_conn=None,
               resp_chunk_size=None):
    """
    Get an object

    :param url: storage URL
    :param token: auth token
    :param container: container name that the object is in
    :param name: object name to get
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :param resp_chunk_size: if defined, chunk size of data to read. NOTE: If
                            you specify a resp_chunk_size you must fully read
                            the object's contents before making another
                            request.
    :returns: a tuple of (response headers, the object's contents) The response
              headers will be a dict and all header names will be lowercase.
    :raises ClientException: HTTP GET request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url)
    path = '%s/%s/%s' % (parsed.path, quote(container), quote(name))
    conn.request('GET', path, '', {'X-Auth-Token': token})
    resp = conn.getresponse()
    if resp.status < 200 or resp.status >= 300:
        resp.read()
        raise ClientException('Object GET failed', http_scheme=parsed.scheme,
                http_host=conn.host, http_port=conn.port, http_path=path,
                http_status=resp.status, http_reason=resp.reason)
    if resp_chunk_size:

        def _object_body():
            buf = resp.read(resp_chunk_size)
            while buf:
                yield buf
                buf = resp.read(resp_chunk_size)
        object_body = _object_body()
    else:
        object_body = resp.read()
    resp_headers = {}
    for header, value in resp.getheaders():
        resp_headers[header.lower()] = value
    return resp_headers, object_body


def head_object(url, token, container, name, http_conn=None):
    """
    Get object info

    :param url: storage URL
    :param token: auth token
    :param container: container name that the object is in
    :param name: object name to get info for
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :returns: a dict containing the response's headers (all header names will
              be lowercase)
    :raises ClientException: HTTP HEAD request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url)
    path = '%s/%s/%s' % (parsed.path, quote(container), quote(name))
    conn.request('HEAD', path, '', {'X-Auth-Token': token})
    resp = conn.getresponse()
    resp.read()
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Object HEAD failed', http_scheme=parsed.scheme,
                http_host=conn.host, http_port=conn.port, http_path=path,
                http_status=resp.status, http_reason=resp.reason)
    resp_headers = {}
    for header, value in resp.getheaders():
        resp_headers[header.lower()] = value
    return resp_headers


def put_object(url, token=None, container=None, name=None, contents=None,
               content_length=None, etag=None, chunk_size=65536,
               content_type=None, headers=None, http_conn=None, proxy=None):
    """
    Put an object

    :param url: storage URL
    :param token: auth token; if None, no token will be sent
    :param container: container name that the object is in; if None, the
                      container name is expected to be part of the url
    :param name: object name to put; if None, the object name is expected to be
                 part of the url
    :param contents: a string or a file like object to read object data from;
                     if None, a zero-byte put will be done
    :param content_length: value to send as content-length header; also limits
                           the amount read from contents; if None, it will be
                           computed via the contents or chunked transfer
                           encoding will be used
    :param etag: etag of contents; if None, no etag will be sent
    :param chunk_size: chunk size of data to write; default 65536
    :param content_type: value to send as content-type header; if None, no
                         content-type will be set (remote end will likely try
                         to auto-detect it)
    :param headers: additional headers to include in the request, if any
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :param proxy: proxy to connect through, if any; None by default; str of the
                  format 'http://127.0.0.1:8888' to set one
    :returns: etag from server response
    :raises ClientException: HTTP PUT request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url, proxy=proxy)
    path = parsed.path
    if container:
        path = '%s/%s' % (path.rstrip('/'), quote(container))
    if name:
        path = '%s/%s' % (path.rstrip('/'), quote(name))
    if headers:
        headers = dict(headers)
    else:
        headers = {}
    if token:
        headers['X-Auth-Token'] = token
    if etag:
        headers['ETag'] = etag.strip('"')
    if content_length is not None:
        headers['Content-Length'] = str(content_length)
    else:
        for n, v in headers.iteritems():
            if n.lower() == 'content-length':
                content_length = int(v)
    if content_type is not None:
        headers['Content-Type'] = content_type
    if not contents:
        headers['Content-Length'] = '0'
    if hasattr(contents, 'read'):
        conn.putrequest('PUT', path)
        for header, value in headers.iteritems():
            conn.putheader(header, value)
        if content_length is None:
            conn.putheader('Transfer-Encoding', 'chunked')
            conn.endheaders()
            chunk = contents.read(chunk_size)
            while chunk:
                conn.send('%x\r\n%s\r\n' % (len(chunk), chunk))
                chunk = contents.read(chunk_size)
            conn.send('0\r\n\r\n')
        else:
            conn.endheaders()
            left = content_length
            while left > 0:
                size = chunk_size
                if size > left:
                    size = left
                chunk = contents.read(size)
                conn.send(chunk)
                left -= len(chunk)
    else:
        conn.request('PUT', path, contents, headers)
    resp = conn.getresponse()
    resp.read()
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Object PUT failed', http_scheme=parsed.scheme,
                http_host=conn.host, http_port=conn.port, http_path=path,
                http_status=resp.status, http_reason=resp.reason)
    return resp.getheader('etag', '').strip('"')


def post_object(url, token, container, name, headers, http_conn=None):
    """
    Update object metadata

    :param url: storage URL
    :param token: auth token
    :param container: container name that the object is in
    :param name: name of the object to update
    :param headers: additional headers to include in the request
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :raises ClientException: HTTP POST request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url)
    path = '%s/%s/%s' % (parsed.path, quote(container), quote(name))
    headers['X-Auth-Token'] = token
    conn.request('POST', path, '', headers)
    resp = conn.getresponse()
    resp.read()
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Object POST failed', http_scheme=parsed.scheme,
                http_host=conn.host, http_port=conn.port, http_path=path,
                http_status=resp.status, http_reason=resp.reason)


def delete_object(url, token=None, container=None, name=None, http_conn=None,
                  headers=None, proxy=None):
    """
    Delete object

    :param url: storage URL
    :param token: auth token; if None, no token will be sent
    :param container: container name that the object is in; if None, the
                      container name is expected to be part of the url
    :param name: object name to delete; if None, the object name is expected to
                 be part of the url
    :param http_conn: HTTP connection object (If None, it will create the
                      conn object)
    :param headers: additional headers to include in the request
    :param proxy: proxy to connect through, if any; None by default; str of the
                  format 'http://127.0.0.1:8888' to set one
    :raises ClientException: HTTP DELETE request failed
    """
    if http_conn:
        parsed, conn = http_conn
    else:
        parsed, conn = http_connection(url, proxy=proxy)
    path = parsed.path
    if container:
        path = '%s/%s' % (path.rstrip('/'), quote(container))
    if name:
        path = '%s/%s' % (path.rstrip('/'), quote(name))
    if headers:
        headers = dict(headers)
    else:
        headers = {}
    if token:
        headers['X-Auth-Token'] = token
    conn.request('DELETE', path, '', headers)
    resp = conn.getresponse()
    resp.read()
    if resp.status < 200 or resp.status >= 300:
        raise ClientException('Object DELETE failed',
                http_scheme=parsed.scheme, http_host=conn.host,
                http_port=conn.port, http_path=path, http_status=resp.status,
                http_reason=resp.reason)


class Connection(object):
    """Convenience class to make requests that will also retry the request"""

    def __init__(self, authurl, user, key, retries=5, preauthurl=None,
                 preauthtoken=None, snet=False, starting_backoff=1,
                 auth_version=1):
        """
        :param authurl: authenitcation URL
        :param user: user name to authenticate as
        :param key: key/password to authenticate with
        :param retries: Number of times to retry the request before failing
        :param preauthurl: storage URL (if you have already authenticated)
        :param preauthtoken: authentication token (if you have already
                             authenticated)
        :param snet: use SERVICENET internal network default is False
        :param auth_version: Openstack auth version.
        """
        self.authurl = authurl
        self.user = user
        self.key = key
        self.retries = retries
        self.http_conn = None
        self.url = preauthurl
        self.token = preauthtoken
        self.attempts = 0
        self.snet = snet
        self.starting_backoff = starting_backoff
        self.auth_version = auth_version

    def get_auth(self):
        return get_auth(self.authurl, self.user, self.key, snet=self.snet,
                        auth_version=self.auth_version)

    def http_connection(self):
        return http_connection(self.url)

    def _retry(self, reset_func, func, *args, **kwargs):
        self.attempts = 0
        backoff = self.starting_backoff
        while self.attempts <= self.retries:
            self.attempts += 1
            try:
                if not self.url or not self.token:
                    self.url, self.token = self.get_auth()
                    self.http_conn = None
                if not self.http_conn:
                    self.http_conn = self.http_connection()
                kwargs['http_conn'] = self.http_conn
                rv = func(self.url, self.token, *args, **kwargs)
                return rv
            except (socket.error, HTTPException):
                if self.attempts > self.retries:
                    raise
                self.http_conn = None
            except ClientException, err:
                if self.attempts > self.retries:
                    raise
                if err.http_status == 401:
                    self.url = self.token = None
                    if self.attempts > 1:
                        raise
                elif err.http_status == 408:
                    self.http_conn = None
                elif 500 <= err.http_status <= 599:
                    pass
                else:
                    raise
            sleep(backoff)
            backoff *= 2
            if reset_func:
                reset_func(func, *args, **kwargs)

    def head_account(self):
        """Wrapper for :func:`head_account`"""
        return self._retry(None, head_account)

    def get_account(self, marker=None, limit=None, prefix=None,
                    full_listing=False):
        """Wrapper for :func:`get_account`"""
        # TODO(unknown): With full_listing=True this will restart the entire
        # listing with each retry. Need to make a better version that just
        # retries where it left off.
        return self._retry(None, get_account, marker=marker, limit=limit,
                           prefix=prefix, full_listing=full_listing)

    def post_account(self, headers):
        """Wrapper for :func:`post_account`"""
        return self._retry(None, post_account, headers)

    def head_container(self, container):
        """Wrapper for :func:`head_container`"""
        return self._retry(None, head_container, container)

    def get_container(self, container, marker=None, limit=None, prefix=None,
                      delimiter=None, full_listing=False):
        """Wrapper for :func:`get_container`"""
        # TODO(unknown): With full_listing=True this will restart the entire
        # listing with each retry. Need to make a better version that just
        # retries where it left off.
        return self._retry(None, get_container, container, marker=marker,
                           limit=limit, prefix=prefix, delimiter=delimiter,
                           full_listing=full_listing)

    def put_container(self, container, headers=None):
        """Wrapper for :func:`put_container`"""
        return self._retry(None, put_container, container, headers=headers)

    def post_container(self, container, headers):
        """Wrapper for :func:`post_container`"""
        return self._retry(None, post_container, container, headers)

    def delete_container(self, container):
        """Wrapper for :func:`delete_container`"""
        return self._retry(None, delete_container, container)

    def head_object(self, container, obj):
        """Wrapper for :func:`head_object`"""
        return self._retry(None, head_object, container, obj)

    def get_object(self, container, obj, resp_chunk_size=None):
        """Wrapper for :func:`get_object`"""
        return self._retry(None, get_object, container, obj,
                           resp_chunk_size=resp_chunk_size)

    def put_object(self, container, obj, contents, content_length=None,
                   etag=None, chunk_size=65536, content_type=None,
                   headers=None):
        """Wrapper for :func:`put_object`"""

        def _default_reset(*args, **kwargs):
            raise ClientException('put_object(%r, %r, ...) failure and no '
                'ability to reset contents for reupload.' % (container, obj))

        reset_func = _default_reset
        tell = getattr(contents, 'tell', None)
        seek = getattr(contents, 'seek', None)
        if tell and seek:
            orig_pos = tell()
            reset_func = lambda *a, **k: seek(orig_pos)
        elif not contents:
            reset_func = lambda *a, **k: None

        return self._retry(reset_func, put_object, container, obj, contents,
            content_length=content_length, etag=etag, chunk_size=chunk_size,
            content_type=content_type, headers=headers)

    def post_object(self, container, obj, headers):
        """Wrapper for :func:`post_object`"""
        return self._retry(None, post_object, container, obj, headers)

    def delete_object(self, container, obj):
        """Wrapper for :func:`delete_object`"""
        return self._retry(None, delete_object, container, obj)

# End inclusion of swift.common.client
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


def mkdirs(path):
    try:
        makedirs(path)
    except OSError, err:
        if err.errno != EEXIST:
            raise


def put_errors_from_threads(threads, error_queue):
    """
    Places any errors from the threads into error_queue.
    :param threads: A list of QueueFunctionThread instances.
    :param error_queue: A queue to put error strings into.
    :returns: True if any errors were found.
    """
    was_error = False
    for thread in threads:
        for info in thread.exc_infos:
            was_error = True
            if isinstance(info[1], ClientException):
                error_queue.put(str(info[1]))
            else:
                error_queue.put(''.join(format_exception(*info)))
    return was_error


class QueueFunctionThread(Thread):

    def __init__(self, queue, func, *args, **kwargs):
        """ Calls func for each item in queue; func is called with a queued
            item as the first arg followed by *args and **kwargs. Use the abort
            attribute to have the thread empty the queue (without processing)
            and exit. """
        Thread.__init__(self)
        self.abort = False
        self.queue = queue
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.exc_infos = []

    def run(self, *args, **kwargs):
        try:
            while True:
                try:
                    item = self.queue.get_nowait()
                    if not self.abort:
                        self.func(item, *self.args, **self.kwargs)
                    self.queue.task_done()
                except Empty:
                    if self.abort:
                        break
                    sleep(0.01)
        except Exception:
            self.exc_infos.append(exc_info())


def st_get_container(options, container):
    conn = get_conn(options)

    objects = {}
    try:
        objects = conn.get_container(container, marker='')
    except ClientException, err:
        if err.http_status != 404:
            print 'Container %s not found' % container
            raise

    return objects

def st_delete(options, container, obj):
    """ delete an object in a given container """
    # TODO: add better arg, error handling
    conn = get_conn(options)
    try:
        conn.delete_object(container, obj)
    except ClientException, err:
        if err.http_status != 404:
            print "Object %s not found" % repr('%s/%s' % (container, obj))
            raise

def st_delete_container(options, container):
    """ delete a given container """
    # TODO: add better arg, error handling
    conn = get_conn(options)
    try:
        conn.delete_container(container)
    except ClientException, err:
        if err.http_status != 404:
            print 'Container %s not found' % repr(container)
            raise
        if err.http_status != 409:
            raise

def st_download(options, container, obj, out_file=None):
    """ st_download """
    conn = get_conn(options)
    try:
        headers, body = \
            conn.get_object(container, obj, resp_chunk_size=65536)
        content_type = headers.get('content-type')
        if 'content-length' in headers:
            content_length = int(headers.get('content-length'))
        else:
            content_length = None
        etag = headers.get('etag')
        # TODO: fix this headache - downloading to ./container/object not desired
        path = join(container, obj) or obj
        if not out_file:
          out_file = obj
          if path[:1] in ('/', '\\'):
              path = path[1:]
              out_file = path
        md5sum = None
        make_dir = out_file != "-"
        if content_type.split(';', 1)[0] == 'text/directory':
            if make_dir and not isdir(path):
                mkdirs(path)
            read_length = 0
            if 'x-object-manifest' not in headers:
                md5sum = md5()
            for chunk in body:
                read_length += len(chunk)
                if md5sum:
                    md5sum.update(chunk)
        else:
            # TODO: fix or remove
            #dirpath = dirname(path)
            #if make_dir and dirpath and not isdir(dirpath):
            #    mkdirs(dirpath)
            if out_file == "-":
                fp = stdout
            elif out_file:
                fp = open(out_file, 'wb')
            # TODO: fix or remove
            #else:
            #    fp = open(path, 'wb')
            read_length = 0
            if 'x-object-manifest' not in headers:
                md5sum = md5()
            for chunk in body:
                fp.write(chunk)
                read_length += len(chunk)
                if md5sum:
                    md5sum.update(chunk)
            fp.close()
        if md5sum and md5sum.hexdigest() != etag:
            print '%s: md5sum != etag, %s != %s' % \
                (path, md5sum.hexdigest(), etag)
        if content_length is not None and read_length != content_length:
            print '%s: read_length != content_length, %d != %d' % \
                (path, read_length, content_length)
        if 'x-object-meta-mtime' in headers:
            mtime = float(headers['x-object-meta-mtime'])
            if out_file:
                utime(out_file, (mtime, mtime))
            else:
                utime(path, (mtime, mtime))
    except ClientException, err:
        if err.http_status != 404:
            raise
        print 'Object %s not found' % \
            repr('%s/%s' % (container, obj))

def st_create_container(options, container):

    conn = get_conn(options)
    try:
        conn.put_container(container)
    except ClientException, err:
        if err.http_status != 404:
            print 'unable to create container %s' % container
            raise

def st_list(options, container):

    conn = get_conn(options)
    try:
        marker = ''
        if not container:
            items = \
                conn.get_account(marker=marker, prefix=options['prefix'])[1]
        else:
            items = conn.get_container(container, marker=marker)[1]
        if not items:
            print 'items for container %s not found' % container

    except ClientException, err:
        if err.http_status != 404:
            print 'Container %s not found' % (container)
            raise
    return items

def st_upload(options, container, path):
    """ upload a file to swift """
    try:
        conn = get_conn(options)
        try:
            obj = path
            if obj.startswith('./') or obj.startswith('.\\'):
                obj = obj[2:]
            if obj.startswith('/'):
                obj = obj[1:]
            put_headers = {'x-object-meta-mtime': str(getmtime(path))}
            # We need to HEAD all objects now in case we're overwriting a
            # manifest object and need to delete the old segments
            # ourselves.
            old_manifest = None
            try:
                headers = conn.head_object(container, obj)
                cl = int(headers.get('content-length'))
                mt = headers.get('x-object-meta-mtime')
                if cl == getsize(path) and \
                    mt == put_headers['x-object-meta-mtime']:
                    return
                if not options.leave_segments:
                    old_manifest = headers.get('x-object-manifest')
            except ClientException, err:
                if err.http_status != 404:
                    raise
            conn.put_object(container, obj, open(path, 'rb'),
                            content_length=getsize(path), headers=put_headers)
        except OSError, err:
            if err.errno != ENOENT:
                raise
            print 'Local file %s not found' % repr(path)
            raise

    except ClientException, err:
        if err.http_status != 404:
            raise
        print 'Account not found'
