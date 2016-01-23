


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
window_size = 20
slice_num = 10
def producer(args):
    filename_prefix = ""
    originals = []
    l = {}
    gen =((int(file[len(prefix):-len(ending)]), image_slicer.slice(os.path.join(root, file), slice_num, save=False)) for root, file in args)
    for index, tiles in gen:
        if not filename_prefix:
            filename_prefix = str(index)
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
        filename = "/tmp/composite/%s_%d.jpg" %(filename_prefix, index)
        result.save(filename)
    producer.queue.put(filename_prefix)
    return
    #for making tiles in one image with original placing, not interesting
    #instead keep meta data in other container
    for index, tile in enumerate(originals):
        try:
            tile.image = l[0][index].image
        except IndexError:
            tile.image.paste(0,None)
    img = image_slicer.join(originals)
    img.save("/tmp/composite.jpg")
    return ""

def consumer(queue, images_left):
    while True:
        msg = queue.get()
        if msg == None:
            break
        images_left -=1 
        print "consumer received: %s now only left: %d" %(msg, images_left)

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
        with closing(multiprocessing.Pool(8, producer_init, [q])) as pool:
            pool.map(producer, window(gen))
    except KeyboardInterrupt:
        pass
    q.put(None)
    consumer_p.join()
        #pool.map
if __name__ =="__main__":
    path = sys.argv[1]
    main(path)
