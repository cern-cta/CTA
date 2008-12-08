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

def parseConfigFile(configFile, section):
    f=open(configFile,"r")
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
    regexp = re.compile("^([a-zA-Z_0-9]+)[ \t]+(.*)$")
    for l in lines:
        if l.find("***") != -1: break
        if l.strip() != "":
            g = regexp.match(l)
            if g != None:
                res[g.group(1)] = g.group(2).strip()
    return res

#########################
# get castor parameters #
#########################

class configuration:
    """ a singleton, initializing global parameters on loading """
    def gatherParameters(self):
        # parse command line
        myArg={"-s":"","-p":"","-S":"","-e":"","-v":"","-c":"","-q":"False","-o":"","-h":""}
        optionCmdLine = getopt.getopt(sys.argv[1:],'s:S:e:v:p:c:qo:h:')
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
        self.outputDirTape = ""
        self.testCastorV1 = True
        self.testDefaultEnv = True
        # Parse config file content
        params = parseConfigFile(self.configFile, "Generic")
        if params.has_key("STAGE_HOST"): self.stagerHost=params["STAGE_HOST"]
        if params.has_key("STAGE_PORT"): self.stagerPort=params["STAGE_PORT"]
        if params.has_key("STAGE_SVCCLASS"): self.stagerSvcClass=params["STAGE_SVCCLASS"]
        if params.has_key("CASTOR_V2"):
            self.stagerVersion = -1;
            if params["CASTOR_V2"].lower() == "yes":
                self.stagerVersion = 2;
        if params.has_key("TIMEOUT"): self.timeOut=int(params["TIMEOUT"])
        if params.has_key("EXTRA_SVCCLASS"): self.stagerExtraSvcClass=params["EXTRA_SVCCLASS"]
        if params.has_key("DISKONLY_SVCCLASS"): self.stagerDiskOnlySvcClass=params["DISKONLY_SVCCLASS"]
        if params.has_key("FORCED_FILECLASS"): self.stagerForcedFileClass=params["FORCED_FILECLASS"]
        if params.has_key("QUIET_MODE") and params["QUIET_MODE"].lower() == "yes": self.quietMode = True
        if params.has_key("OUTPUT_DIR"):
            self.outputDir = params["OUTPUT_DIR"]
        self.outputDirTape = self.outputDir
        if params.has_key("OUTPUT_DIR_TAPE"): self.outputDirTape = params["OUTPUT_DIR_TAPE"]
        if params.has_key("TEST_DEFAULT_ENV") and params["TEST_DEFAULT_ENV"].lower() == "no": self.testDefaultEnv = False
        if params.has_key("TEST_CASTOR_V1") and params["TEST_CASTOR_V1"].lower() == "no": self.testCastorV1 = False
        # Overwrite with command line values when needed
        if myArg["-s"] !="":
            self.stagerHost=myArg["-s"]
        if myArg["-p"] != "":
            self.stagerPort=myArg["-p"]
        if myArg["-S"] != "":
            self.stagerSvcClass=myArg["-S"]
        if myArg["-v"] != "":
            if myArg["-v"] == "2":
                self.stagerVersion=2
            else:
                self.stagerVersion=-1
        if myArg["-e"] != "":
            self.stagerExtraSvcClass=myArg["-e"]
        if myArg["-q"] == "":
            self.quietMode=True
        if myArg["-o"] != "":
            self.outputDir=myArg["-o"]
        if myArg["-h"] != "":
            self.stagerHost=myArg["-h"]
    isInitialized = False
    def __init__(self):
        global stagerHost,stagerPort,stagerSvcClass,stagerVersion,stagerTimeOut,stagerExtraSvcClass,stagerDiskOnlySvcClass,stagerForcedFileClass,configFile,quietMode,outputDir,outputDirTape,testCastorV1,testDefaultEnv
        # gather parameters if needed
        if not self.isInitialized:
            self.isInitialized = True
            self.gatherParameters()
            if not self.quietMode:
                print 'Configuration file used is "'+ self.configFile + '", leading to :'
                print "    stagerHost : ", self.stagerHost
                print "    outputDirectory : ", self.outputDir
                print "    tapeOutputDirectory : ", self.outputDirTape
                print "    testDefaultEnv : ", self.testDefaultEnv
                print "    testCastorV1 : ", self.testCastorV1
                print "    stagerSvcClass : ", self.stagerSvcClass
                print "    stagerExtraSvcClass : ", self.stagerExtraSvcClass
                print "    stagerDiskOnlySvcClass : ", self.stagerDiskOnlySvcClass
                print "    stagerForcedFileClass : ", self.stagerForcedFileClass
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
        outputDirTape = self.outputDirTape
        testCastorV1 = self.testCastorV1
        testDefaultEnv = self.testDefaultEnv

# force to initialize the configuration
config = configuration()

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
    myGid=os.getegid()
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
    if ret.find("No such file or directory") != -1:
        return -1
    return 0

# check if an rfcp output is correct, i.e. it states the transfer went through
# and the number of bytes written/read is equal to the remote file size if not
# an update
def checkRfcpOutput(buf, put, update=0):
    if put:
        s = re.compile('(\d+) bytes in [0-9]+ seconds through local \(in\) and eth[0-1] \(out\).*\n(\d+) bytes in remote file').search(buf)
    else:
        s = re.compile('(\d+) bytes in [0-9]+ seconds through eth[0-1] \(in\) and local \(out\).*\n(\d+) bytes in remote file').search(buf)
    if not s:
        return 0
    elif update:
        return 1
    else:
        return (s.group(1) == s.group(2))

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
            myScen+="export RFIO_USE_CASTOR_V2=yes;"
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
            myScen+="setenv STAGE_PORT  "+port+";"
        if serviceClass !=-1:
            myScen+="setenv STAGE_SVCCLASS "+serviceClass+";"
        if version !=-1:
            myScen+="setenv RFIO_USE_CASTOR_V2 yes;"
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

# creates a file as least as big as sizeMB MBs
# may reuse old files bigger than what was asked
def getBigFile(sizeMB, ticket):
    fileName = "/tmp/bigFile" + ticket
    # check whether to reuse existing file
    reuse = True
    try:
        size = os.stat(fileName).st_size / 0x10000
        if size < sizeMB:
            reuse = False
    except OSError:
        reuse = False
    if not reuse:
        # create it with random data
        fout = open(fileName,"wb")
        fin = open("/dev/urandom")
        for i in range(sizeMB) :
            fout.write(fin.read(0x100000)) # 1 MB
        fin.close()
        fout.close()
    return fileName
