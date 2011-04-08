#from app import settings
#import os
#import stat
#import time
#import datetime
#import thread
import time
from django.conf import settings
import datetime
import os
import stat

def cleanGarbageFiles():
    thresholdage = settings.CACHE_MIDDLEWARE_SECONDS
    intervalinseconds = settings.GARBAGE_FILES_CLEANINIG_INTERVAL
    while(True):
        deleteOldImageFiles(thresholdage)
        deleteOldStatusFiles(thresholdage)
        time.sleep(intervalinseconds)

def deleteOldImageFiles(seconds):
    imagedir = settings.LOCAL_APACHE_DICT+settings.REL_TREEMAP_DICT
    files = os.listdir(imagedir)
    for file in files:
        accessed = os.stat(imagedir + '/' + file)[stat.ST_ATIME]
        accessedtimetuple = time.localtime(accessed)
        timeaccessed = datetime.datetime(accessedtimetuple[0], accessedtimetuple[1],accessedtimetuple[2],accessedtimetuple[3],accessedtimetuple[4], accessedtimetuple[5], accessedtimetuple[6])
        timediff = datetime.datetime.now() - timeaccessed
        ageinseconds = timediff.seconds + timediff.days * 24*60*60
        if ageinseconds > seconds:
            os.remove(imagedir + '/' + file)
            
def deleteOldStatusFiles(seconds):
    statusdir = settings.LOCAL_APACHE_DICT+settings.REL_STATUS_DICT
    files = os.listdir(statusdir)
    for file in files:
        accessed = os.stat(statusdir + '/' + file)[stat.ST_ATIME]
        accessedtimetuple = time.localtime(accessed)
        timeaccessed = datetime.datetime(accessedtimetuple[0], accessedtimetuple[1],accessedtimetuple[2],accessedtimetuple[3],accessedtimetuple[4], accessedtimetuple[5], accessedtimetuple[6])
        timediff = datetime.datetime.now() - timeaccessed
        ageinseconds = timediff.seconds + timediff.days * 24*60*60
        if ageinseconds > seconds:
            os.remove(statusdir + '/' + file)
    
