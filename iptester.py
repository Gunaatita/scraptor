import httplib
import urlparse
import socks
import urllib2
import urllib
import random
import time
from bs4 import BeautifulSoup
from string import ascii_lowercase
import stem
import stem.connection
from stem import Signal
from stem.control import Controller

class SocksiPyConnection(httplib.HTTPConnection):
    def __init__(self, proxytype, proxyaddr, proxyport=None, rdns=True, username=None, password=None, *args, **kwargs):
        self.proxyargs = (proxytype, proxyaddr, proxyport, rdns, username, password)
        httplib.HTTPConnection.__init__(self, *args, **kwargs)

    def connect(self):
        self.sock = socks.socksocket()
        self.sock.setproxy(*self.proxyargs)
        if isinstance(self.timeout, float):
            self.sock.settimeout(self.timeout)
        self.sock.connect((self.host, self.port))

class SocksiPyHandler(urllib2.HTTPHandler):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kw = kwargs
        urllib2.HTTPHandler.__init__(self)

    def http_open(self, req):
        def build(host, port=None, strict=None, timeout=0):
            conn = SocksiPyConnection(*self.args, host=host, port=port, strict=strict, timeout=timeout, **self.kw)
            return conn
        return self.do_open(build, req)

def renew_connection():
    with Controller.from_port(port = 8119,address="127.0.0.1") as controller:
        controller.authenticate()
        controller.signal(Signal.NEWNYM)
        controller.close()
    
opener = urllib2.build_opener(SocksiPyHandler(socks.PROXY_TYPE_SOCKS5,'localhost', 9051))   
opener.addheaders = [('User-agent', random.choice("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"))]
oldIP = opener.open("http://icanhazip.com/").read()
newIP = oldIP
print ("oldIP: %s" % oldIP)
renew_connection()
seconds=0
while oldIP == newIP:
    # sleep this thread
    # for the specified duration
    time.sleep(0.05)
    # track the elapsed seconds
    seconds += 1
    # obtain the current IP address
    newIP = opener.open("http://icanhazip.com/").read()
    # signal that the program is still awaiting a different IP address
    print ("%d seconds elapsed awaiting a different IP address." % seconds)
print ("newIP: %s" % newIP)


