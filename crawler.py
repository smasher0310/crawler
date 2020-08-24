import requests
from urllib.parse import urlparse,urljoin
from bs4 import BeautifulSoup
import cfg
import persistence  
import time, os
from concurrent.futures import ThreadPoolExecutor

class crawler():
    """A simple Crawler"""
    def __init__(self,url):
        self.url =  url
    
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

        #add the base url to DB 
        persistence.addDocument(self.url,self.url)

    def do_crawl(self,item):

        if cfg.link_count >= cfg.link_limit:
            return
        
        print('Requesting for the page {}'.format(item['link']))
        res = self.makeGetRequest(item['link'])
        if res is None:
            return

        if not res.headers['content-type'] in cfg.file_ext.keys():
            filetype = 'unknown'
        else:
            filetype = cfg.file_ext[res.headers['content-type']]

        filename = '{}.{}'.format(str(time.time_ns()),filetype)
        # write the content to new file
        filepath = os.path.join(cfg.html_folder_path,filename)
        file = open(filepath,'w')
        file.write(res.text)
        file.close()

        #update the info related with this link
        # split(';')[0].split('/')[-1]
        persistence.updateDocument(item['_id'],res.status_code,filepath,res.headers['content-type'],len(res.text),True)
        
        # crawl and save links to DB
        soup = BeautifulSoup(res.text,'html.parser')
        for link in soup.find_all('a'): 

            # link in DB is >=5000, defer to crawl 
            if cfg.link_count >= cfg.link_limit:
                #mark the page uncrawled and return
                persistence.updateDocument(item['_id'],res.status_code,filepath,res.headers['content-type'],len(res.text),False)
                return

            # print(link.get('href'))
            if isAbsolute(link.get('href')):
                persistence.addDocument(link.get('href'),item['link'])
            else:
                # create absolute path
                newPath = urljoin(item['link'],link.get('href'))
                if isAbsolute(newPath):
                    persistence.addDocument(newPath,item['link'])
                
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

                executor = ThreadPoolExecutor(cfg.thread_count)

                for item in items:
                    if cfg.link_count >= cfg.link_limit:
                        # terminate the threads and reset the counter for next cycle
                        executor.shutdown(wait=True)
                        print("Maximum Limit Reached\nResetting the limit to 0")
                        cfg.link_count = 0
                        break
                    executor.submit(self.do_crawl,item)
                    
                executor.shutdown(wait=True)
                # sleep for 5s between cycles
                crawledDocumentsCount = persistence.countDocuments({'isCrawled' : True})
                notCrawledDocumentsCount = persistence.countDocuments({'isCrawled': False})
                print('Pages Scraped: {}\t Total Link count: {}'.format(crawledDocumentsCount,crawledDocumentsCount+notCrawledDocumentsCount))
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
