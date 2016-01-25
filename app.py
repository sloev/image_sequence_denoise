import logging
import motor
from wsrpc import rpc, serve_forever, ws_open, ws_close
import uuid
from tornado.ioloop import IOLoop
from pymongo import ASCENDING, DESCENDING

@ws_open
def create_namespaces(self):
    self.tiles_jobs = {}
    self.reduced_tiles_jobs = {}
    self.db = self.settings['db']
    self.window_size = self.settings['window_size']


@ws_close
def free_jobs(self):
    logging.info("freeing jobs")
    def callback(result, error):
        if error:
            raise error
    if self.tiles_jobs:
        tiles_col = self.db.tiles
        tiles_col.update(
                {'$isolated':1,'_id':{'$in':self.tiles_jobs.values()}},
                {'$set':{'lock':False}},{'multi':True}, callback=callback)
    if self.reduced_tiles_jobs:
        reduced_tiles_col = self.db.reduced_tiles
        reduced_tiles_col.update(
                {'$isolated':1,'_id':{'$in':self.reduced_tiles_jobs.values()}},
                {'$set':{'lock':False}}, {'multi':True}, callback=callback)

@rpc
def get_tiles(self, args, callback_id, FUNC_NAME):
    logging.info("get tiles")
    tiles_col = self.db.tiles
    
    #[1]: get first tile and dertermine which tile number we are looking for
    query = {'lock':False}
    cursor = tiles_col.find(query).sort({'index':ASCENDING, 'number':ASCENDING})
    docs = yield cursor.to_list(length=1)
    doc = docs[0]
    number = doc['number']
    index = doc['index']
    #[2]: query for next window size index(>=) tiles of number 
    query = {'lock':False, 'number':number, 'index':{'$gte':index}}
    cursor = tiles_col.find(query).sort({'index':ASCENDING, 'number':ASCENDING})
    docs = yield cursor.to_list(length=self.window_size)

    #[3]: lock them, if not succes a race condition occured, try again!
    yield tiles_col.update({'$isolated':1,'_id':{'$in':[doc['_id'] for doc in docs]}},
        {'$set':{'lock':True}}, {'multi':True})
    docs, ids = zip(*[(doc, doc.pop('_id')) for doc in docs])
    args = {'id':job_id,'docs':docs}
    self.tiles_jobs[job_id] = ids
    self.push(args, callback_id, FUNC_NAME)

@rpc
def post_reduced_tile(self, args, callback_id, FUNC_NAME):
    logging.info("post_reduced_tile")

@rpc
def get_reduced_tiles(self, args, callback_id, FUNC_NAME):
    logging.info("get reduced_tiles")

@rpc
def post_compiled_image(self, args, callback_id, FUNC_NAME):
    logging.info("post_compiled_image")

def main():
    settings = {}
    port = 8888
    serve_forever(settings, port)
if __name__ == "__main__":
    main()
