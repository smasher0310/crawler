from pymongo import MongoClient
import cfg
import time
import datetime

links = None
connected = False

def setup():
    global connected, links
    try:
        client = MongoClient(cfg.DB_host,cfg.DB_port)
        db = client[cfg.DB_Name]
        links = db.links
        connected =  True
        print('Connected to {} DB {}'.format(cfg.DB_Name,db))
    except:
        print('Error in connecting to DB')

def addDocument(url,rootUrl):
    ret = links.find_one({
        'link': url
    })
    if not ret is None:
        # print('{} already in DB'.format(url))
        return 
    cfg.link_count = cfg.link_count + 1
    doc = {
        'link':                 url,
        'srcLink':              rootUrl,
        'isCrawled':            False,
        'lastCrawlDate':        None,
        'responseStatus':       404,
        'contentType':          None,
        'contentLen':            0,
        'filePath':             '',
        'createdAt':            datetime.datetime.utcnow()
    }
    links.insert_one(doc)

# get the links that has not been crawled yet
def getLinks():
    items = links.find({"isCrawled":False})
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
            {'_id':oid},
            { '$set' : {
                    'isCrawled':            isCrawled,
                    'lastCrawlDate':        datetime.datetime.utcnow(),
                    'responseStatus':       status,
                    'contentType':          contentType,
                    'contentLen':           contentLen,
                    'filePath':             filePath
                 } 
            }
        )
    except Exception as e:
        print(e)


def countDocuments(cond):
    return links.count_documents(cond)