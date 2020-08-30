from pymongo import MongoClient
import cfg
import time
from datetime import datetime
from datetime import timedelta  

links = None
connected = False
linkCount = 0

def setup():
    global connected, links
    try:
        client = MongoClient(cfg.DB_host,cfg.DB_port)
        db = client[cfg.DB_Name]
        links = db.links
        connected =  True
        print('Connected to {} DB {}\n'.format(cfg.DB_Name,db))
        links.create_index(str("link"),unique=True)
    except:
        print('Error in connecting to DB')
        

def add_bulk(data):
    global linkCount
    try:
        prev = documentCount()
        links.insert_many(data,ordered=False)
        curr = documentCount()
        linkCount = linkCount +  curr - prev
    except: 
        pass

def addDocument(url,rootUrl):
    global linkCount
    try:
        prev = documentCount()
        doc = {
            'link':                 url,
            'srcLink':              rootUrl,
            'isCrawled':            False,
            'lastCrawlDate':        None,
            'responseStatus':       404,
            'contentType':          None,
            'contentLen':            0,
            'filePath':             '',
            'createdAt':            datetime.now()
        }
        links.insert_one(doc)
        curr = documentCount()
        linkCount = linkCount + curr-prev

    except: 
        pass

# get the links that has not been crawled yet
def getLinks():
    try:
        items = links.find({ '$or': [
                                        {"isCrawled"     : False},
                                        {'lastCrawlDate' : { '$lt' : datetime.now() - timedelta(days=cfg.days)}}
                                    ]
                            })   
    except :
        pass
    itemList = []
    for item in items:
        itemList.append(item)
    return itemList

# # update status of the crawl
def updateDocument(oid,status,filePath,contentType,contentLen,isCrawled):
    if not connected:
        exit('DB not connectd')
        return

    try:
        links.update_one(
            {'_id': oid},
            { '$set' : {
                    'isCrawled':            isCrawled,
                    'lastCrawlDate':        datetime.now(),
                    'responseStatus':       status,
                    'contentType':          contentType,
                    'contentLen':           contentLen,
                    'filePath':             filePath
                 } 
            }
        )
    except Exception as e:
        print(e)


def documentCount():
    return links.count_documents({'isCrawled':True}) + links.count_documents({'isCrawled':False})

def scrapedDocumentCount():
    return links.count_documents({'isCrawled':True})