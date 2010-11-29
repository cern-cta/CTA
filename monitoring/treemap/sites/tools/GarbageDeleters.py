from sites import settings
import os
import stat
import time
import datetime


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
    
