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


def saveOnFile(namefile,cmdS,scen=None):
    count=0
    for singleCmd in cmdS:

        t=threading.Timer(120.0,timeOut,[singleCmd])
        t.start()
        if count == 0:
            fin=open(namefile,"wb")
        else:
            fin=open(namefile+("%d"%count),"wb")
        if scen != None:
            singleCmd=scen+singleCmd
        
        myOut=os.popen4(singleCmd)
        fin.write((myOut[1]).read())
        fin.close()
        count=count+1
        t.cancel()
      
def runOnShell(cmdS,scen=None):
    for singleCmd in cmdS:
        t=threading.Timer(120.0,timeOut,[singleCmd])
        t.start()
        if scen != None:
            singleCmd=scen+singleCmd
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

def checkUser(filePath):
    myCmd=prepareCastorString()
    myCmd="nsls "+myCmd
    ret=os.popen(myCmd).read()
    userId=logUser()
    if ret.find("No such file or directory") != -1:
        print "You are "+userId+" and you are not a Castor user. Go away!"
        return -1
    return userId

 
#different scenarium for env

def createScenarium(host,port,serviceClass,version,opt=None):
	myShell=os.popen('ls -l /bin/sh').read()
	myScen=""
	if myShell.find("bash") != -1:
		if host !=-1:
			myScen+="export STAGE_HOST="+host+";"
		else:
			myScen+="unset STAGE_HOST;"
		if port !=-1:
			myScen+="export STAGE_PORT="+port+";"
		else:
			myScen+="unset STAGE_PORT;"	
		if serviceClass !=-1:
			myScen+="export STAGE_SVCCLASS="+serviceClass+";"
		else:
			myScen+="unset STAGE_SVCCLASS;"
		if version !=-1:
			myScen+="export RFIO_USE_CASTOR_V2="+version+";"
		else:
			myScen+="unset RFIO_USE_CASTOR_V2;"
                if opt != None:
                    for envVar in opt:
                        if envVar[1] != -1:
                            myScen+="export "+envVar[0]+"="+envVar[1]+";"
                        else:
                            myScen+="unset "+envVar[0]+";"
		return myScen

	if myShell.find("tcsh"):
	        if host !=-1:
			myScen+="setenv STAGE_HOST "+host+";"
		else:
			myScen+="unsetenv STAGE_HOST;"
		if port !=-1:
			myScen+="setenv  STAGE_PORT  "+port+";"
		else:
			myScen+="unsetenv STAGE_PORT;"	
		if serviceClass !=-1:
			myScen+="setenv  STAGE_SVCCLASS "+serviceClass+";"
		else:
			myScen+="unsetenv STAGE_SVCCLASS;"
	        if version !=-1:
			myScen+="setenv RFIO_USE_CASTOR_V2 "+version+";"
		else:
			myScen+="unsetenv RFIO_USE_CASTOR_V2;"
                if opt != None:
                    for envVar in opt:
                        if envVar[1] != -1:
                            myScen+="setenv "+envVar[0]+" "+envVar[1]+";"
                        else:
                            myScen+="unsetenv "+envVar[0]+";"
                
	        return myScen
	return myScen
