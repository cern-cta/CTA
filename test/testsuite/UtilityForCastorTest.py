import threading
import os
import sys
import time
import getopt
import re

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

#########################
# get castor parameters #
#########################

class configuration:
        """ a singleton, initializing global parameters on loading """
        def parseConfigFile(self, section):
                f=open(self.configFile,"r")
                configFileInfo=f.read()
                f.close
                index= configFileInfo.find("*** " + section)
                configFileInfo=configFileInfo[index:]
                index=configFileInfo.find("***")
                if index != -1:
                        index=index-1
                        configFileInfo=configFileInfo[:index]
                lines = configFileInfo.split("\n")[1:]
                res = {}
                regexp = re.compile("^([a-zA-Z_]+)[ \t]+(.*)$")
                for l in lines:
                        if l.find("***") != -1: break
                        if l.strip() != "":
                                g = regexp.match(l)
                                if g != None:
                                        res[g.group(1)] = g.group(2)
                return res
        def gatherParameters(self):
                # parse command line
                myArg={"-s":"","-p":"","-d":"","-e":"","-v":"","-c":"","-q":"False","-o":""}
                optionCmdLine = getopt.getopt(sys.argv[1:],'s:d:e:v:p:c:qo:')
                optionCmdLine = optionCmdLine[0]
                for elemLine in optionCmdLine:
                        if elemLine != []:
                                myArg[elemLine[0]]=elemLine[1]
                # check for the right configFile
                if myArg["-c"] != "":
                        # note : that is a global variable
                        self.configFile=myArg["-c"]
                else:
                        self.configFile = "./CASTORTESTCONFIG"
                # Default values
                self.stagerHost=""
                self.stagerPort=""
                self.stagerSvcClass=""
                self.stagerVersion=""
                self.timeOut=0
                self.stagerExtraSvcClass=""
                self.stagerDiskOnlySvcClass=""
                self.stagerForcedFileClass=""
                self.quietMode = False
                self.outputDir = ""
                # Parse config file content
                params = self.parseConfigFile("Generic")
                if params.has_key("STAGE_HOST"): self.stagerHost=params["STAGE_HOST"]
                if params.has_key("STAGE_PORT"): self.stagerPort=params["STAGE_PORT"]
                if params.has_key("STAGE_SVCCLASS"): self.stagerSvcClass=params["STAGE_SVCCLASS"]
                if params.has_key("CASTOR_V2"): self.stagerVersion=params["CASTOR_V2"]
                if params.has_key("TIMEOUT"): self.timeOut=int(params["TIMEOUT"])
                if params.has_key("EXTRA_SVCCLASS"): self.stagerExtraSvcClass=params["EXTRA_SVCCLASS"]        
                if params.has_key("DISKONLY_SVCCLASS"): self.stagerDiskOnlySvcClass=params["DISKONLY_SVCCLASS"]
                if params.has_key("FORCED_FILECLASS"): self.stagerForcedFileClass=params["FORCED_FILECLASS"]
                if params.has_key("QUIET_MODE") and params["QUIET_MODE"].lower() == "true":self.quietMode = True
                if params.has_key("OUTPUT_DIR"): self.outputDir = params["OUTPUT_DIR"]
                # Overwrite with command line values when needed
                if myArg["-s"] !="":
                        self.stagerHost=myArg["-s"]
                if myArg["-p"] != "":
                        self.stagerPort=myArg["-p"]
                if myArg["-d"] != "":
                        self.stagerSvcClass=myArg["-d"]
                if myArg["-v"] != "":
                        if myArg["-v"] == "2":
                                self.stagerVersion="yes"
                        else:
                                self.stagerVersion="no"
                if myArg["-e"] != "":
                        self.stagerExtraSvcClass=myArg["-e"]
                if myArg["-q"] == "":
                        self.quietMode=True
                if myArg["-o"] != "":
                        self.outputDir=myArg["-o"]
                else:
                        if self.outputDir == "":
                                userId=logUser()
                                if userId != -1:
                                        self.outputDir = "/castor/cern.ch/user/"+userId[0]+"/"+userId+"/"
        isInitialized = False
        def __init__(self):
                global stagerHost, stagerPort, stagerSvcClass, stagerVersion, stagerExtraSvcClass, stagerDiskOnlySvcClass, stagerForcedFileClass, configFile, quietMode, outputDir
                # gather parameters if needed
                if not self.isInitialized:
                        self.isInitialized = True
                        self.gatherParameters()
                        if not self.quietMode:
                                print 'Configuration file used is "'+ self.configFile + '", leading to :'
                                print "        stagerHost : ", self.stagerHost
                                print "        output directory : ", self.outputDir                                
                                print "        stagerSvcClass : ", self.stagerSvcClass
                                print "        stagerExtraSvcClass : ", self.stagerExtraSvcClass
                                print "        stagerDiskOnlySvcClass : ", self.stagerDiskOnlySvcClass
                                print "        stagerForcedFileClass : ", self.stagerForcedFileClass
                # expose them in global variables
                stagerHost = self.stagerHost
                stagerPort = self.stagerPort
                stagerSvcClass = self.stagerSvcClass
                stagerVersion = self.stagerVersion
                stagerTimeOut = self.timeOut
                stagerExtraSvcClass = self.stagerExtraSvcClass
                stagerDiskOnlySvcClass = self.stagerDiskOnlySvcClass
                stagerForcedFileClass = self.stagerForcedFileClass
                configFile = self.configFile
                quietMode = self.quietMode
                outputDir = self.outputDir

# force to initialize the configuration
configuration()

####################
# Useful functions #
####################

def getListOfFiles(myDir):
        tmpList=os.popen4("nsls "+myDir)[1].read().split("\n")
        defList=[]
        if tmpList[0].find("No such file or directory"):
                return []
        for file in tmpList:
            defList.append(myDir+file)
        return defList

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
        t=threading.Timer(stagerTimeOut,timeOut,[singleCmd])
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
    buff=[]
    for singleCmd in cmdS:
        t=threading.Timer(stagerTimeOut,timeOut,[singleCmd])
        t.start()
        if scen != None:
            singleCmd=scen+singleCmd
        buff.append(os.popen4(singleCmd)[1].read())
        t.cancel()
    return buff  
    
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


# check if the user of the program is a castor user

def checkUser():
    myCmd="nsls " + outputDir
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

        if myShell.find("tcsh") != -1:
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
        return myScen

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
