


import sys, multiprocessing, itertools
import os
from contextlib import closing
from PIL import Image
import numpy
from scipy.misc import imread
from bson.binary import Binary
import bson
import pickle
import json
ending = ".JPG"
prefix="p"

import itertools as itr
import image_slicer
from io import BytesIO
import base64

window_size = 20
slice_num = 10
def producer(args):
    process_name = multiprocessing.current_process().name
    filename_prefix = ""
    originals = []
    l = {}
    gen =((int(file[len(prefix):-len(ending)]), image_slicer.slice(os.path.join(root, file), slice_num, save=False)) for root, file in args)
    for index, tiles in gen:
        if not filename_prefix:
            filename_prefix = index
        if not originals:
            originals = tiles
        for i, tile in enumerate(tiles):
            if i not in l:
                l[i] = []
            l[i] += [tile]
    for index, tiles in l.iteritems():
        w = sum(i.image.size[0] for i in tiles)
        mh = max(i.image.size[1] for i in tiles)
        result = Image.new("RGBA", (w, mh))
        x = 0
        for i in tiles:
            result.paste(i.image, (x,0))
            x+= i.image.size[0]
        name = "batch_%d-%d_%d.jpg" %(filename_prefix,(filename_prefix + window_size), index)
        orig_tile = originals[index]
        byte_io= BytesIO()
        result.save(byte_io, 'JPEG')
        image_string = base64.b64encode(byte_io.getvalue()).decode()
        obj =  (process_name,
                index,
                image_string,
                orig_tile.number,
                orig_tile.position,
                orig_tile.coords
                )
        producer.queue.put(obj)
    producer.queue.put("minus")

def consumer(queue, images_left):
    images_len = len(str(images_left))
    while True:
        msg = queue.get()
        if msg == None:
            break
        elif msg == "minus":
            images_left -=1 
        else:
            process_name,index,image_string,number,position,coords = msg
            s =  "[%s]\tleft: %0"+str(images_len)+"d\tindex=%0"+str(images_len)+"d\tnumber=%d\tposition=%s\tcoords=%s"
            print s %(process_name, images_left, index, number, str(position), str(coords))

def producer_init(queue):
    producer.queue = queue

def main(path):
    q = multiprocessing.Queue()
    def walk():
        for root, dirs, files in os.walk(path):
            dirs.sort()
            files.sort()
            yield root,dirs,files
    def get_count():
        files = 0
        for _,_,filenames in os.walk(path):
            files += len(filenames)
        return (files - window_size) + 1

    gen = itertools.chain.from_iterable(((root, file) for file in files) for root,dirs,files in walk())

    def window(it):
        itrs = itr.tee(it, window_size)
        return itr.izip(*[itr.islice(anitr, s, None) for s, anitr in enumerate(itrs)])
    try:
        consumer_p = multiprocessing.Process(target=consumer, args=((q),(get_count()),))
        consumer_p.daemon=True
        consumer_p.start()
        producer_pool = multiprocessing.Pool(8, producer_init, [q])
        producer_pool.map_async(producer, window(gen))
        producer_pool.close()
        producer_pool.join()
    except KeyboardInterrupt:
        pass
    q.put(None)
    consumer_p.join()
        #pool.map
if __name__ =="__main__":
    path = sys.argv[1]
    main(path)
