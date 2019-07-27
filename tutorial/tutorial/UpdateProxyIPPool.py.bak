import requests
from bs4 import BeautifulSoup
import threading
import queue
from fake_useragent import UserAgent

class UpdateProxyIPPool(object):
    '''获得西祠网代理ip'''

    def __init__(self, page):
        self.ips = []
        self.urls = []
        for i in range(page):
            self.urls.append("http://www.xicidaili.com/nn/" + str(i))
        self.header = {"User-Agent": 'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0'}
        self.useragent = UserAgent()
        # self.file=open("ips",'w')
        self.q = queue.Queue()
        self.Lock = threading.Lock()

    def update_useragent(self):
        self.header['User-Agent'] = self.useragent.random
        # print("Update Useragent {}".format(self.header['User-Agent']))

    def get_ips(self):
        '''爬IP'''
        for url in self.urls:
            self.update_useragent()
            res = requests.get(url, headers=self.header)
            soup = BeautifulSoup(res.text, 'lxml')
            ips = soup.find_all('tr')
            for i in range(1, len(ips)):
                ip = ips[i]
                tds = ip.find_all("td")
                ip_temp = "http://" + tds[1].contents[0] + ":" + tds[2].contents[0]
                # print(str(ip_temp))
                self.q.put(str(ip_temp))

    def review_ips(self):
        '''测刚刚爬的IP能不能用'''
        while not self.q.empty():
            ip = self.q.get()
            try:
                self.update_useragent()
                proxy = {"http": ip}
                res = requests.get("http://www.baidu.com", proxies=proxy, timeout=5, headers=self.header)

                self.Lock.acquire()
                if res.status_code == 200:
                    self.ips.append(ip)
                    # print(ip)
                self.Lock.release()
            except Exception:
                pass
                # print 'error'

    def main(self):
        '''采用多线程测试IP'''
        self.get_ips()
        threads = []
        for i in range(40):
            threads.append(threading.Thread(target=self.review_ips, args=[]))
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return self.ips

    def savetxt(self, filename):
        with open(filename, 'w') as f:
            for line in self.ips:
                f.write(line + '\n')