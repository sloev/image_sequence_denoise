import os

path = "images"
def walk():
    for root, dirs, files in os.walk(path):
        dirs.sort()
        files.sort()
        yield root, dirs, files
def gen():
    return (file for file in (files for root, dirs, files in walk()))
if __name__ =="__main__":
    for i in gen():
        print i
