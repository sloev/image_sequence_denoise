import tornado
import motor
import time

from tornado.gen import coroutine, Return, sleep

@coroutine
def image_to_numpy(image):
    s = "%d %d" %(image,(2*image))
    yield sleep(10-image)
    print s

    raise Return(s)

@coroutine
def reduce(l):
    l = yield(l)
    print "reduced", l

@coroutine
def do():
    c = []
    for i in range(30):
        c += [image_to_numpy(i)]
        if len(c) > 9:
            x = yield c
            print x
            c = []

tornado.ioloop.IOLoop.current().run_sync(do)
