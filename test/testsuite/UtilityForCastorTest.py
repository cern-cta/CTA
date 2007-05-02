import threading
import os
import sys
import time



def getListOfFiles(myDir):
        tmpList=os.popen4("nsls "+myDir)[1].read().split("\n")
        defList=[]
	if tmpList[0].find("No such file or directory"):
		return []
        for file in tmpList:
            defList.append(myDir+file)
            return defList


def getTimeOut():
    f=open("./CASTORTESTCONFIG","r")
    configFileInfo=f.read()
    f.close
    index= configFileInfo.find("*** Generic parameters ***")
    configFileInfo=configFileInfo[index:]
    index=configFileInfo.find("***")
    if index != -1:
        index=index-1
        configFileInfo=configFileInfo[:index]
    timeOutVal=configFileInfo[configFileInfo.find("TIMEOUT"):].split()[1]
    return int(timeOutVal)

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
        myTime=getTimeOut()
        t=threading.Timer(myTime,timeOut,[singleCmd])
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
        myTime=getTimeOut()
        t=threading.Timer(myTime,timeOut,[singleCmd])
        t.start()
        if scen != None:
            singleCmd=scen+singleCmd
        os.system(singleCmd)
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
    return 0

def logGroup():
    #function to find the right directory on castor 
    myGid= os.getegid()
   
    strGid=0
   
    fin=open("/etc/group",'r')
    for line in fin:
        elem=line.split(":")
        if elem[2] == str(myGid):
            strGid=elem[0]
            fin.close()
            return strGid
    return 0


# prepare string for castor file

def prepareCastorString():
    userId=logUser()
    if userId==-1:
	     return -1
    return "/castor/cern.ch/user/"+userId[0]+"/"+userId+"/"
	    
# check if the user of the program is a castor user

def checkUser():
    myCmd=prepareCastorString()
    myCmd="nsls "+myCmd
    ret=os.popen(myCmd).read()
    userId=logUser()
    if ret.find("No such file or directory") != -1:
        print "You are "+userId+" and you are not a Castor user. Go away!"
        return -1
    return userId

 
#different scenarium for env

def createScenarium(host,port,serviceClass,version,useEnv=None,opt=None):
	myShell=os.popen('ls -l /bin/sh').read()
	myScen=""
	if myShell.find("bash") != -1:
		if host !=-1:
			myScen+="export STAGE_HOST="+host+";"
		if port !=-1:
			myScen+="export STAGE_PORT="+port+";"	
		if serviceClass !=-1:
			myScen+="export STAGE_SVCCLASS="+serviceClass+";"
		if version !=-1:
			myScen+="export RFIO_USE_CASTOR_V2="+version+";"
                if opt != None:
                    for envVar in opt:
                        if envVar[1] != -1:
                            myScen+="export "+envVar[0]+"="+envVar[1]+";"

                if useEnv == None or useEnv == "no":
                    if host == -1:
			myScen+="unset STAGE_HOST;"
                    if port ==-1:
			myScen+="unset STAGE_PORT;"	
                    if serviceClass ==-1:
			myScen+="unset STAGE_SVCCLASS;"
                    if version ==-1:
			myScen+="unset RFIO_USE_CASTOR_V2;"
                    if opt != None:
                        for envVar in opt:
                            if envVar[1] == -1:
                                myScen+="unset "+envVar[0]+";"

                return myScen

	if myShell.find("tcsh"):
	        if host !=-1:
			myScen+="setenv STAGE_HOST "+host+";"
		if port !=-1:
			myScen+="setenv  STAGE_PORT  "+port+";"
		if serviceClass !=-1:
			myScen+="setenv  STAGE_SVCCLASS "+serviceClass+";"
	        if version !=-1:
			myScen+="setenv RFIO_USE_CASTOR_V2 "+version+";"
                if opt != None:
                    for envVar in opt:
                        if envVar[1] != -1:
                            myScen+="setenv "+envVar[0]+" "+envVar[1]+";"
                            
                if useEnv == None or useEnv == "no":
                    if host == -1:
                        myScen+="unsetenv STAGE_HOST;"
                    if port == -1:
                        myScen+="unsetenv STAGE_PORT;"	
                    if serviceClass == -1:
                        myScen+="unsetenv STAGE_SVCCLASS;"
                    if version == -1:
                        myScen+="unsetenv RFIO_USE_CASTOR_V2;"
                    if opt != None:
                        for envVar in opt:
                            if envVar[1] == -1:
                                myScen+="unsetenv "+envVar[0]+";"
        print myScen        
	return myScen


def getCastorParameters():
	try:
            f=open("./CASTORTESTCONFIG")
            configFileInfo=f.read()
            f.close
            index= configFileInfo.find("*** Generic parameters ***")
            configFileInfo=configFileInfo[index:]
            index=configFileInfo.find("***")
            if index != -1:
                index=index-1
                configFileInfo=configFileInfo[:index]
            
            stagerHost=(configFileInfo[configFileInfo.find("STAGE_HOST"):]).split()[1]
            stagerPort=(configFileInfo[configFileInfo.find("STAGE_PORT"):]).split()[1]
            stagerSvcClass=(configFileInfo[configFileInfo.find("STAGE_SVCCLASS"):]).split()[1]
            stagerVersion=(configFileInfo[configFileInfo.find("CASTOR_V2"):]).split()[1]
            return (stagerHost,stagerPort,stagerSvcClass,stagerVersion)
        except IOError:
            return (0,0,0,0)

# to test if the request handler and the stager are responding and the rfcp working
 
def testCastorBasicFunctionality(inputFile,castorFile,localOut,scenario):
        cmd=["stager_qry -M "+castorFile]
        saveOnFile(localOut+"Qry",cmd,scenario)
        fi=open(localOut+"Qry","r")
        outBuff=fi.read()
        fi.close()
        if outBuff.rfind("Unknown host") != -1:
            return [-1,"the stager host is unknown"]
        if outBuff.rfind("Internal error")!= -1:
            return [-1, "the stager gives Internal errors"]
        
	cmd=["rfcp "+inputFile+" "+castorFile+"RfioFine1","rfcp "+castorFile+"RfioFine1  "+castorFile+"RfioFine2","rfcp "+castorFile+"RfioFine1  "+localOut+"fileRfioFineCopy","rfcp "+castorFile+"RfioFine2  "+localOut+"fileRfioFine2Copy","diff "+inputFile+" "+localOut+"fileRfioFineCopy","diff "+inputFile+" "+localOut+"fileRfioFine2Copy"]
		
        saveOnFile(localOut+"RfioFine",cmd,scenario)
        if os.stat(localOut+"fileRfioFineCopy")[6] == 0:
            return [-1,"Rfcp doesn't work"]
        if os.stat(localOut+"fileRfioFine2Copy")[6] == 0:
            return [-1,"Rfcp doesn't work"]
        if os.stat(localOut+"RfioFine4")[6] != 0:
            return [-1,"Rfcp doesn't work"]
        if os.stat(localOut+"RfioFine5")[6] != 0:
            return [-1,"Rfcp doesn't work"]
        
        runOnShell(["rm "+localOut+"fileRfioFineCopy","rm "+localOut+"fileRfioFine2Copy"],scenario)
        return [0,"Everything fine"]
