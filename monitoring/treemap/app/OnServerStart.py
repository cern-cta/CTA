import thread
from app.tools.GarbageDeleters import cleanGarbageFiles

def run():
    print "starting thread that cleans old files"
    try:
        thread.start_new_thread(cleanGarbageFiles, ())
    except:
        print "Error: unable to start garbage cleaning"