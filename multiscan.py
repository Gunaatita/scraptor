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
import math
from torsock import SocksiPyHandler
import constants
from Queue import Queue
from threading import Thread, Condition, Lock
from threading import active_count as threading_active_count
from pymongo import MongoClient
import pymongo
    
#renew require opener only


class opener(object):
    
    def __init__(self,i):
            self.index=i
            self.base_port=constants.base_port+i
            self.control_port=constants.control_port+i
            self.open_socket=urllib2.build_opener(SocksiPyHandler(socks.PROXY_TYPE_SOCKS5,'localhost', self.base_port))
                                             
    def renew(self,force,count):
        try:
            if not force:
                #zeta bluff
                if random.randint(0,math.floor(math.sqrt(constants.coupons))) == 1:
                    return
            #poisson normal load balancing
            with Controller.from_port(port = self.control_port,address="127.0.0.1") as controller:
                controller.authenticate()
                controller.signal(Signal.NEWNYM)
                controller.close()
                time.sleep(0.1)
                #print "RENEW SUCCEDED ON PORT: "+ str(self.control_port)
        except Exception, e:
            time.sleep(0.1)
            if count <5:
                self.renew(force,count+1)
            else:
                print "RENEW FAILED ON PORT: " + str(self.control_port)
                print "attempt: "+ str(count)
                print " error: " +str(e)
            
class Worker(Thread):
    log_con=  MongoClient().scraping.logs
    q=[constants.start.format(alphabet) for alphabet in ascii_lowercase]
    def __init__(self,w_opener):
        Thread.__init__(self)
        self.conn = MongoClient()
        self.db = self.conn.scraping
        self.coll = self.db.linkedin
        self.w_opener=w_opener
    
    @staticmethod
    def url_fix(s, charset='utf-8'):
        if isinstance(s, unicode):
            s = s.encode(charset, 'ignore')
        scheme, netloc, path, qs, anchor = urlparse.urlsplit(s)
        path = urllib.quote(path, '/%')
        qs = urllib.quote_plus(qs, ':&=')
        return urlparse.urlunsplit((scheme, netloc, path, qs, anchor))  
        
    def mongowrite(self,url):
        soup = self.randomopener(url)
        if soup != -1:
            [elem.extract() for elem in soup.findAll(['script', 'style'])]
            self.coll.insert_one({"url":url,"soup":unicode(soup)})    
     
    def randomopener(self,myurl):
        cur_opener=self.w_opener.open_socket
        cur_opener.addheaders = [('User-agent', random.choice(constants.user_agent_list))]
        try:
            f = cur_opener.open(Worker.url_fix(myurl))
            if random.randint(1,constants.coupon_collection_rate)==1:
                self.w_opener.renew(False,0)
            return BeautifulSoup(f)
        except Exception, e:
            print(e)
            self.w_opener.renew(True,0)
            Worker.log_con.insert_one({"error":str(e),"url":myurl})
            return -1
        
    def dir_finale(self,link):
        last = link.split('/')[-1].lower()
        if any(s in last for s in ('!', '*', "^","%","$","@","`","~","+","=")):
            return
        soup = self.randomopener(link)
        if soup != -1:
            all_links = [tag['href'] for tag in soup.select('a.profile-photo')] + [tag['href'] for tag in soup.select('a.profile-img')]
            for item in all_links:
                self.mongowrite(item)
            
    def run(self):
        while True:
            #pop random
            if len(Worker.q) == 0:
                continue
            url = random.choice(Worker.q)
            Worker.q.remove(url)
            if "/directory/" in url:
                soup = self.randomopener(url)
                if soup != -1:
                    all_links = [tag['href'] for tag in soup.select('li.content a[href]')]
                    Worker.q=Worker.q+all_links
            elif "https" in url:
                self.mongowrite(url)
            else :
                self.dir_finale("https://in.linkedin.com/" + url)
            if self.coll.count()>constants.max_urls:
                break  

def main():
    workers=[Worker(opener(i)) for i in range(constants.coupons)]
    for w in workers : w.start()
    for w in workers: w.join() 

if __name__ == '__main__':
    main()

    
