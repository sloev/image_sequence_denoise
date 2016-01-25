import logging
import motor
from wsrpc import rpc, serve_forever, ws_open, ws_close
import uuid
from tornado.ioloop import IOLoop
from pymongo import ASCENDING, DESCENDING


@ws_open
def test_data(self):
    tiles_jobs = [
            {
                'index':index,
                'number':number,
                'lock':False
                } for number in range(12) for index in range(10)]

    db = self.settings['db']
    db.drop_collection("tiles_jobs")
    db.drop_collection("reduced_tiles_jobs")
    res = yield db.tiles_jobs.insert(tiles_jobs)
    logging.info(str(res))
@ws_open
def create_namespaces(self):
    self.tiles_jobs = {}
    self.reduced_tiles_jobs = {}
    self.db = self.settings['db']
    self.window_size = self.settings['window_size']


@ws_close
def free_jobs(self):
    logging.info("freeing jobs")
    def callback(col,ids):
        def inner(result, error):
            if error:
                logging.info("error in freeing up from {}:{}".format(col,ids))
                raise error
            logging.info("free'd up from {}:{}".format(col,ids))
        return inner
    for col in ['tiles_jobs', 'reduced_tiles_jobs']:
        jobs = getattr(self, col)
        if jobs:
            ids = jobs.values()
            tiles_col = self.db[col]
            tiles_col.update(
                    {'$isolated':1,'_id':{'$in':ids}},
                    {'$set':{'lock':False}},multi=True, callback=callback(col, ids))

@rpc
def get_tiles(self, args, callback_id, FUNC_NAME):
    logging.info("get tiles")
    tiles_col = self.db.tiles_jobs
    
    #[1]: get first tile and dertermine which tile number we are looking for
    query = {'lock':False}
    cursor = tiles_col.find(query).sort([('index',ASCENDING), ('number',ASCENDING)])
    docs = yield cursor.to_list(length=1)
    if not docs:
        logging.warning("no more tiles")
        args = None
        self.push(args, callback_id, FUNC_NAME)
        return
    doc = docs[0]
    number = doc['number']
    index = doc['index']
    job_id = str(doc['_id'])
    logging.info("index:{}, number:{}".format(number, index))
    #[2]: query for next window size index(>=) tiles of number 
    query = {'lock':False, 'number':number, 'index':{'$gte':index}}
    cursor = tiles_col.find(query).sort([('index',ASCENDING), ('number',ASCENDING)])
    docs = yield cursor.to_list(length=self.window_size)
    if not docs:
        logging.warning("no more tiles")
        args = None
        self.push(args, callback_id, FUNC_NAME)
        return
    #[3]: lock them, if not succes a race condition occured, try again!
    yield tiles_col.update({'$isolated':1,'_id':{'$in':[doc['_id'] for doc in docs]}},
        {'$set':{'lock':True}}, multi=True)
    docs, ids = zip(*[(doc, doc.pop('_id')) for doc in docs])
    args = {'id':job_id,'docs':docs}
    self.tiles_jobs[job_id] = ids
    self.push(args, callback_id, FUNC_NAME)

@rpc
def post_reduced_tile(self, args, callback_id, FUNC_NAME):
    logging.info("post_reduced_tile")
    try:
        job_id = args['id']
        doc = {'index':'lolcat'}#{key, args[key] for key in ['image','index','number','position','coords']}
        self.tiles_jobs.pop(job_id)
        logging.info("removed tiles_job: %s from list"%str(job_id))
        res = yield self.db.reduced_tiles_jobs.insert(doc)
        logging.info("inserted into reduced_tiles_job:{}".format(res))
    except KeyError:
        args = {'error':"bad arguments or inexisting id"}
        self.push(args, callback_id, FUNC_NAME)
@rpc
def get_reduced_tiles(self, args, callback_id, FUNC_NAME):
    logging.info("get reduced_tiles")

@rpc
def post_compiled_image(self, args, callback_id, FUNC_NAME):
    logging.info("post_compiled_image")

def main():
    db = motor.motor_tornado.MotorClient().test
    window_size = 10
    settings = {'db':db,
            'window_size':window_size
            }
    port = 8888
    serve_forever(settings, port)
if __name__ == "__main__":
    main()
