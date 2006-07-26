import threading
import os
import sys
import time

def getTicket():
    newTime=time.gmtime()
    return str(((newTime[7]*24+newTime[3])*60 + newTime[4])*60 +newTime[5]) 

# function called if one of the request takes too much time

def timeOut(myCmd):
    print "TIME OUT !!!"
    print "Castor is not working :("
    listProc=os.popen("ps").read().split("\n")

    # I retrieve and  kill the process that created problems
    procName=(myCmd.split(" "))[0]
    for item in listProc:
        if item.find(procName) != -1:
            os.system("kill -9 "+item[0:5])
            print procName+" was blocked"
    os._exit(0)  


def saveOnFile(namefile,cmdS):
    t=threading.Timer(120.0,timeOut,[cmdS])
    t.start()
    fin=open(namefile,"wb")
    myOut=os.popen4(cmdS)
    fin.write((myOut[1]).read())
    fin.close()
    t.cancel()
      
def runOnShell(cmdS):
    t=threading.Timer(120.0,timeOut,[cmdS])
    t.start()
    os.popen4(cmdS)
    t.cancel()
    
    
def logUser():
    
#function to find the right directory on castor 
    myUid= os.geteuid()
   
    strUid=0
   
    fin=open("/etc/passwd",'r')
    for line in fin:
        elem=line.split(":")
        if elem[2] == str(myUid):
            strUid=elem[0]
    fin.close()
    return strUid


# prepare string for castor file

def prepareCastorString():
    userId=logUser()
    if userId==-1:
	     return -1
    return "/castor/cern.ch/user/"+userId[0]+"/"+userId+"/"
	    
# check if the user of the program is a castor user

def checkUser():
    userId=logUser()
    myCmd=prepareString("","")
    myCmd="nsls "+myCmd
    ret=runSafe(myCmd)
    if ret.find("No such file or directory") != -1:
        print "You are "+userId+" and you are not a Castor user. Go away!"
        return -1
    return userId





