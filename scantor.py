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

#this number is one more than ports in shell script
coupons = 4
max_urls = 50
members =0 
openers = []
ips=[]
start = "https://in.linkedin.com/directory/people-{0}"
max_load=10
coupon_collection_rate=math.floor(max_load/math.log(coupons))
base_port=9050
control_port=8118
file=None
logs=open("logs.csv", 'a')

user_agent_list = [\
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"\
        "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",\
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",\
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",\
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",\
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",\
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",\
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",\
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",\
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",\
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",\
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",\
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",\
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",\
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",\
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",\
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",\
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"]

for i in range(0,coupons):
    opener = urllib2.build_opener(SocksiPyHandler(socks.PROXY_TYPE_SOCKS5,'localhost', base_port+i))
    #opener = urllib2.build_opener()
    openers.append(opener)

def url_fix(s, charset='utf-8'):
    if isinstance(s, unicode):
        s = s.encode(charset, 'ignore')
    scheme, netloc, path, qs, anchor = urlparse.urlsplit(s)
    path = urllib.quote(path, '/%')
    qs = urllib.quote_plus(qs, ':&=')
    return urlparse.urlunsplit((scheme, netloc, path, qs, anchor))

def renewconnection():
    num=control_port+random.randint(0,coupons-1)
    #zeta bluff
    if random.randint(0,math.floor(math.sqrt(coupons))) == 1:
        return
    #poisson normal load balancing
    with Controller.from_port(port = num,address="127.0.0.1") as controller:
        controller.authenticate()
        controller.signal(Signal.NEWNYM)
        controller.close()
    
def randomopener(myurl):
    global ips
    cur_opener = random.choice(openers)
    cur_opener.addheaders = [('User-agent', random.choice(user_agent_list))]
    try:
        f = cur_opener.open(url_fix(myurl))
        if random.randint(1,coupon_collection_rate)==1:
            renewconnection()
            time.sleep(0.02)
        return BeautifulSoup(f)
    except Exception, e:
        print(e)
        ip=opener.open("http://icanhazip.com/").read()
        logs.write(str(e) +"," + myurl+"," + ip +"\n")
        if ip in ips:
            renewconnection()
        else:
            ips.append(ip)
        return -1

def dir_finale(link):
    global members
    last = link.split('/')[-1].lower()
    if any(s in last for s in ('!', '*', "^","%","$","@","`","~","+","=")):
        return
    if members > max_urls:
        return
    soup = randomopener(link)
    if soup != -1:
        all_links = [tag['href'] for tag in soup.select('a.profile-photo')] + [tag['href'] for tag in soup.select('a.profile-img')]
        for item in all_links:
            file.write(item+"\n" )
            members+=1
    
def finale(links):
    global members
    for link  in links:
        if "https" in link:
            file.write(link+"\n")
            members+=1
        else :
            dir_finale("https://in.linkedin.com/" + link)
                    
def recurser(links):
    if "/directory/" in random.choice(links):
        for link in links:
            if members > max_urls :
                break
            soup = randomopener(link)
            if soup != -1:
                all_links = [tag['href'] for tag in soup.select('li.content a[href]')]
                recurser(all_links)
    else :
        finale(links)

def main():
    for char in ascii_lowercase:
        global file
        file=open("members_"+char +".csv", 'a')
        alphaurl = start.format(char)
        if members > max_urls :
            break
        soup = randomopener(alphaurl)
        if soup != -1:
            all_links = [tag['href'] for tag in soup.select('li.content a[href]')]
            recurser(all_links) 
        file.close()
    logs.close()
    
if __name__ == "__main__": main()   
