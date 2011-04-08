import thread
from app.tools.GarbageDeleters import cleanGarbageFiles

def run():
    #this thread is starting for each process!
    #todo: determine if other processes are already running???
    print "starting thread that cleans old files"
    try:
        thread.start_new_thread(cleanGarbageFiles, ())
    except:
        print "Error: unable to start garbage cleaning"