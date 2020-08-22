import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup

class crawler():
    """A simple Crawler"""
    def __init__(self,url):
        self.threadcount = 1
        self.url =  url
        self.sleeptime = 0
        self.links = []
    
    # do http request
    def makeGetRequest(self,url):
        try:
            response = requests.get(url)
            return response
        except:
            print('Something terrible happend! Cannot do http request {}'.format(url))
            return None

    # read configuration
    def readConfiguration(self):
        print('Read configuration')

    # intial crawl to populate the database
    def setup(self):
        print('intial crawling: url(' + self.url + ')')
        response = self.makeGetRequest(self.url)
        if  response is not None:
            print(response.status_code)
            soup = BeautifulSoup(response.content,'html.parser')
            for link in soup.find_all('a'):
                # print(link.get('href'))
                self.links.append(link.get('href'))
            # print(self.links)
        else:
            print('Initial Url failed')


    # crawl the links       
    def crawl(self):
        while(1):
            #loop over the collection to crawl 
            try:
                r = requests.get(self.url)
                print(len(r.text))
            except:
                print('Something terrible happend! Cannot do http request')


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
    


if __name__ == '__main__':
    main()
