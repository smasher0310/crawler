import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import cfg
import persistence  
import time, os

class crawler():
    """A simple Crawler"""
    def __init__(self,url):
        self.threadcount = 1
        self.url =  url
        self.sleeptime = 0
    
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


    # crawl the links       
    def crawl(self):
        cycleCount = 0
        while(1):
            try:
                # get the uncrawled url from DB
                items = persistence.getLinks()

                if items is None:
                    print('No link to crawl! Exit')
                    break
                cycleCount =  cycleCount + 1
                print('Cycle {} started'.format(cycleCount))
                for item in items:

                    # link in DB is 5000
                    if cfg.link_count == cfg.link_limit:
                        print('Maximum limit reached')
                        break

                    res = self.makeGetRequest(item['link'])
                    if res is None:
                        continue

                    # write the content to new file
                    filename = os.path.join(os.getcwd(),cfg.html_folder,str(time.time_ns())+".html")
                    file = open(filename,'w')
                    file.write(res.text)
                    file.close()

                    #update the info related with this link
                    # split(';')[0].split('/')[-1]
                    persistence.updateDocument(item['_id'],res.status_code,filename,res.headers['content-type'],len(res.text))
                    
                    # crawl and save links to DB
                    print('crawling the page: {}'.format(item['link']))
                    soup = BeautifulSoup(res.text,'html.parser')
                    for link in soup.find_all('a'): 
                        # print(link.get('href'))
                        if isAbsolute(link.get('href')):
                            persistence.addDocument(link.get('href'),item['link'])
                        else:
                            persistence.addDocument(self.url + link.get('href'),item['link'])
                
                # sleep for 5s between cycles
                print('total link in DB {}'.format(cfg.link_count))
                print('sleeping for 5 sec.....................')
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
