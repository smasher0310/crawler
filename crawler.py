import requests
from urllib.parse import urlparse,urljoin
from bs4 import BeautifulSoup
import cfg
import persistence  
import time, os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

class crawler():
    """A simple Crawler"""
    def __init__(self,url):
        self.url =  url
        self.prevDocCount  = 0
        self.CurrDocCount = 0
    
    # do http request
    def makeGetRequest(self,url):
        try:
            response = requests.get(url)
            return response
        except:
            return None

    # intial crawl to populate the database
    def setup(self):
        persistence.setup()

        #create folder for crawled files
        if not os.path.exists(os.path.join(os.getcwd(),cfg.local_folder)):
            os.makedirs(cfg.local_folder)

        # add the base url to DB 
        persistence.addDocument(self.url,self.url)

    def findExtension(self,contentType):
        ext = ''
        if 'htm' in contentType:
            ext = 'html'
        elif 'json' in contentType:
            ext = 'json'
        elif 'javascript' in contentType:
            ext = 'js'
        elif 'css' in contentType:
            ext = 'css'
        elif 'png' in contentType:
            ext = 'png'
        elif 'jpeg' in contentType or 'jpg' in contentType :
            ext = 'jpeg'
        elif 'pdf' in contentType:
            ext = 'pdf'
        elif 'zip' in contentType:
            ext = 'zip'
        else:
            ext = 'unknown'
        return ext

    def checkLimitReached(self):
        self.CurrDocCount = persistence.documentCount()

        if (self.CurrDocCount - self.prevDocCount) < cfg.link_limit:
           return False
        else:
            self.prevDocCount = self.CurrDocCount
            return True


    def do_crawl(self,item):

        if self.checkLimitReached():
            print('Thread stopped')
            return
        
        # print('Requesting for the page {}'.format(item['link']))
        res = self.makeGetRequest(item['link'])
        if res is None:
            return
        # if filepath exist, overwrite it
        if not item['filePath'] == '':
            filepath = item['filePath']
        else:
            # find the suitable extension 
            filetype = self.findExtension(res.headers['content-type'])
            filename = '{}.{}'.format(str(time.time_ns()),filetype)
            filepath = os.path.join(cfg.local_folder,filename)

        file = open(filepath,'w')
        file.write(res.text)
        file.close()

        #update the info related with this link
        # split(';')[0].split('/')[-1]
        persistence.updateDocument(item['_id'],res.status_code,filepath,res.headers['content-type'],len(res.text),True)
        
        # crawl and save links to DB
        soup = BeautifulSoup(res.text,'html.parser')
        docs = []
        for link in soup.find_all('a'): 

            # link in DB is >=5000, defer to crawl 
            if self.checkLimitReached():
                #mark the page uncrawled and return
                persistence.updateDocument(item['_id'],res.status_code,filepath,res.headers['content-type'],len(res.text),False)
                print('Thread stopped after writting')
                return

            newLink = ''
            if isAbsolute(link.get('href')):
                newLink = link.get('href')
            else:
                # create absolute path
                newLink = urljoin(item['link'],link.get('href'))
                if not isAbsolute(newLink):
                    continue

            docs.append({
                            'link':                 newLink,
                            'srcLink':              item['link'],
                            'isCrawled':            False,
                            'lastCrawlDate':        None,
                            'responseStatus':       404,
                            'contentType':          None,
                            'contentLen':            0,
                            'filePath':             '',
                            'createdAt':            datetime.now()
            })
        persistence.add_bulk(docs)
                
    # crawl the links       
    def crawl(self):
        cycleCount = 0
        while(1):
            try:

                cycleCount =  cycleCount + 1
                print('Cycle {} started'.format(cycleCount))

                # get the uncrawled url from DB
                items = persistence.getLinks()
                if len(items) == 0:
                    print('All links crawled')
                    print('sleeping for 5 sec.....................\n\n')
                    time.sleep(5)
                    continue
                
                futures = []
                executor = ThreadPoolExecutor(cfg.thread_count)
                for item in items:
                    if self.checkLimitReached():
                        # terminate the threads
                        print("Maximum Limit Reached")
                        for f in futures:
                            f.cancel()
                        executor.shutdown(wait=False)
                        break
                    futures.append(executor.submit(self.do_crawl,item))
                    
                executor.shutdown(wait=True)

                print('Pages Scraped: {}\t Total Link count: {}'.format(persistence.scrapedDocumentCount(),persistence.documentCount()))
                
                # resetting
                

                print('sleeping for 5 sec.....................\n\n')
                time.sleep(5)
            except Exception as e:
                print('crawler.py:{}'.format(e))


# check if url is absolute URL
def isAbsolute(url):
    if bool(urlparse(url).scheme) and bool(urlparse(url).netloc):
        return True
    return False        

def main():
    url = input('Enter url to crawl: ')

    # check if url is FQ URL
    if not isAbsolute(url):
        print('Invalid URL')
        exit(1)

    c =  crawler(url)
    c.setup()
    c.crawl()
    


if __name__ == '__main__':
    main()
