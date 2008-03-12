import unittest
import UtilityForCastorTest
import os
import re
import sys
import time
import threading
import UtilityForCastorTest
from UtilityForCastorTest import stagerHost,stagerPort,stagerSvcClass,stagerVersion,stagerTimeOut,stagerExtraSvcClass,stagerDiskOnlySvcClass,stagerForcedFileClass,quietMode,outputDir,configFile

# parameters
ticket= UtilityForCastorTest.getTicket()
myScen=""

# files and directories
inputFile=""
updInputFile=""
localDir=""
dirCastor=outputDir+"/tmpClientTest"+ticket+"/"

def makeBigFile(fileStart):
    size=0
    fout=open("/tmp/bigFile"+ticket,"wb")
    fin=open(fileStart,"r")
    while size < 2000 :
        outBuf=fin.read()
        size+=len(outBuf)
        fout.write(outBuf)
        fin.seek(0)
    fin.close()
    fout.close()
    return "/tmp/bigFile"+ticket

def checkSvcClass(output, svcClass):
    if output.find("Using "+svcClass) != -1 or output.find("Opt SVCCLASS="+svcClass) != -1:
        return True
    else:
        return False

class PreRequisitesCase(unittest.TestCase):
    def mainScenarium(self):
        assert (UtilityForCastorTest.checkUser() != -1), "you don't have acccess to directory \"" + outputDir + "\" where you wanted to run the test"
        try:
            global localDir, inputFile, myScen, updInputFile
            params = UtilityForCastorTest.parseConfigFile(configFile, "Client")
            localDir = params["LOG_DIR"]
            localDir=localDir+ticket+"/"
            os.system("mkdir "+localDir)
            inputFile = params["INPUT_FILE"]
            updInputFile = localDir+"rfcpupdFile"
	    os.system("cat "+inputFile+" > "+updInputFile)
	    myScen=UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerSvcClass,stagerVersion,None,[["STAGER_TRACE","3"]])
            UtilityForCastorTest.runOnShell(["nsmkdir "+dirCastor],myScen)
        except IOError:
            assert 0==-1, "An error in the preparation of the main setting occurred ... test is not valid"
    def stagerRfioFine(self):
        [ret,out] = UtilityForCastorTest.testCastorBasicFunctionality(inputFile,dirCastor+"ClientPreReq"+ticket,localDir,myScen)
        assert ret==0, out
    
class StagerPutCase(unittest.TestCase):
    def basicPut(self):
        cmd=["stager_put -M "+dirCastor+"fileClientBasicPut"+ticket,"stager_qry -M "+dirCastor+"fileClientBasicPut"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientBasicPut",cmd,myScen)
        fi=open(localDir+"ClientBasicPut","r")
        buffOut=fi.read()
        fi.close()

        assert buffOut.rfind("SUBREQUEST_READY") != -1, "stager_put doesn't work"
        fi=open(localDir+"ClientBasicPut1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.rfind("STAGEOUT") != -1, "stager_qry doesn't work after a simple put"
        
        
    def putAndRfcp(self):
        cmd=["stager_put -M "+dirCastor+"fileClientPutRfcp"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientPutRfcp"+ticket,"stager_qry -M "+dirCastor+"fileClientPutRfcp"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientPutAndRfcp",cmd,myScen)
        fi=open(localDir+"ClientPutAndRfcp2","r")
        buffOut=fi.read()
        fi.close()
        
        assert buffOut.find("STAGEOUT") != -1, "stager_qry doesn't work after stager_put and rfcp"
        
        
    def putManyFiles(self):
        strMultiple=""
        cmdRfcp=[]
        for index in range(10):
          strMultiple=strMultiple+" -M "+dirCastor+"fileClientManyPut"+str(index)+ticket
          cmdRfcp.append("rfcp "+inputFile+" "+dirCastor+"fileClientManyPut"+str(index)+ticket)
        
        cmd=["stager_put "+strMultiple]+cmdRfcp+["stager_qry "+strMultiple]

        UtilityForCastorTest.saveOnFile(localDir+"ClientPutManyFiles",cmd,myScen)

        fi=open(localDir+"ClientPutManyFiles","r")
        buffOut=fi.read()
        fi.close()
        
        assert buffOut.count("SUBREQUEST_READY") == 10, "stager_put doesn't work with two files"
        

        fi=open(localDir+"ClientPutManyFiles11","r")
        buffOut=fi.read()
        fi.close()
        
        assert buffOut.count("STAGEOUT") == 10, "stager_qry doesn't work after stager_put with many files and rfcp"
        
        
    def putTag(self):
        cmd=["stager_put -M "+dirCastor+"fileClientPutTag"+ticket+" -U tagPut"+ticket,"stager_qry -M "+dirCastor+"fileClientPutTag"+ticket,"stager_qry -U tagPut"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientPutTag"+ticket,"stager_qry -M "+dirCastor+"fileClientPutTag"+ticket,"stager_qry -U tagPut"+ticket]
        
        UtilityForCastorTest.saveOnFile(localDir+"ClientPutTag",cmd,myScen)
    
        fi=open(localDir+"ClientPutTag","r")
        buffOut=fi.read()
        fi.close()
        
        assert buffOut.find("SUBREQUEST_READY") != -1, "put doesn't work with tag"

        fi=open(localDir+"ClientPutTag1","r")
        buffOut=fi.read()
        fi.close()
        
        assert buffOut.find("STAGEOUT") != -1, "stager_qry doesn't work after a put with tag"
    
        fi=open(localDir+"ClientPutTag2","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.find("STAGEOUT") != -1, "stager_qry doesn't work after a put with tag"

        fi=open(localDir+"ClientPutTag4","r")
        buffOut=fi.read()
        fi.close()
        
        assert buffOut.find("STAGEOUT") != -1, "stager_qry doesn't work with tag"
        
        fi=open(localDir+"ClientPutTag5","r")
        buffOut=fi.read()
        fi.close()
        
        assert buffOut.find("STAGEOUT") != -1, "stager_qry doesn't work with tag"
        
        
    def putSvcClass(self):

        cmd=["stager_put -M "+dirCastor+"fileClientPutSvc1"+ticket+" -S "+stagerSvcClass,"stager_qry -M "+dirCastor+"fileClientPutSvc1"+ticket+" -S "+stagerExtraSvcClass,"stager_put -M "+dirCastor+"fileClientPutSvc2"+ticket+" -S "+stagerExtraSvcClass,"stager_qry -M "+dirCastor+"fileClientPutSvc2"+ticket+" -S "+stagerExtraSvcClass]
        
        UtilityForCastorTest.saveOnFile(localDir+"ClientPutSvc",cmd,myScen)
        fi=open(localDir+"ClientPutSvc","r")
        buffOut=fi.read()
        fi.close()
        assert checkSvcClass(buffOut, stagerSvcClass), "stager_put doesn't work with svc class option -S"
        
        
        fi=open(localDir+"ClientPutSvc1","r")
        outBuf=fi.read()
        fi.close()
        assert checkSvcClass(outBuf, stagerExtraSvcClass), "stager_qry doesn't work with svc class option -S"
        assert outBuf.find("No such file") != -1, "stager_qry doesn't work after a simple put"
        
        fi=open(localDir+"ClientPutSvc2","r")
        buffOut=fi.read()
        fi.close()
        assert checkSvcClass(outBuf, stagerExtraSvcClass), "stager_qry doesn't work with svc class option -S"


        fi=open(localDir+"ClientPutSvc3","r")
        buffOut=fi.read()
        fi.close()
        assert checkSvcClass(outBuf, stagerExtraSvcClass), "stager_qry doesn't work with svc class option -S"
        assert buffOut.rfind("STAGEOUT") != -1, "stager_qry doesn't work with svc class option -S"
    
        
    def putR(self):
        cmd=["stager_put -M "+dirCastor+"fileClientPutR"+ticket+" -r","stager_qry -M "+dirCastor+"fileClientPutR"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientPutR",cmd,myScen)
        
        fi=open(localDir+"ClientPutR","r")
        buffOut=fi.read()
        fi.close()

        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_put doesn't work with svc class the option -r"
        line=buffOut.split("\n")
        reqId=line[len(line)-3]

        assert buffOut.count(reqId) != 3, "stager_put doesn't work with svc class the option -r"
        fi=open(localDir+"ClientPutR1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.rfind("STAGEOUT") != -1, "stager_qry doesn't work with the  option -r"
        

class StagerPutDoneCase(unittest.TestCase):
    def basicPutDone(self):
        cmd=["stager_put -M "+dirCastor+"fileClientBasicPutDone"+ticket,"stager_putdone -M "+dirCastor+"fileClientBasicPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClientBasicPutDone"+ticket]
        
        UtilityForCastorTest.saveOnFile(localDir+"ClientBasicPutDone",cmd,myScen)
        
        fi=open(localDir+"ClientBasicPutDone1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("putDone without a put") != -1, "putDone doesn't work"

        fi=open(localDir+"ClientBasicPutDone2","r")
        buffOut=fi.read()
        fi.close()
        
        assert buffOut.find("STAGEOUT") != -1, "stager_qry doesn't work after a putDone"
        
        
    def putDoneAndRfcp(self):
        cmd=["stager_put -M "+dirCastor+"fileClientPutDoneRfcp"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientPutDoneRfcp"+ticket,"stager_putdone -M "+dirCastor+"fileClientPutDoneRfcp"+ticket,"stager_qry -M "+dirCastor+"fileClientPutDoneRfcp"+ticket]

        UtilityForCastorTest.saveOnFile(localDir+"ClientPutDoneAndRfcp",cmd,myScen)
       
        fi=open(localDir+"ClientPutDoneAndRfcp2","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "putDone doesn't work with rfcp"
    
        fi=open(localDir+"ClientPutDoneAndRfcp3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("CANBEMIGR") != -1 or buffOut.find("STAGED") != -1, "stager_qry doesn't work after a putDone with rfcp"
        
    def putDoneManyFiles(self):

        strMultiple=""
        cmdRfcp=[]
        for index in range(10):
          strMultiple=strMultiple+" -M "+dirCastor+"fileClientManyPutDone"+str(index)+ticket
          cmdRfcp.append("rfcp "+inputFile+" "+dirCastor+"fileClientManyPutDone"+str(index)+ticket)
        
        cmd=["stager_put "+strMultiple]+cmdRfcp+["stager_putdone "+strMultiple] 

        UtilityForCastorTest.saveOnFile(localDir+"ClientPutDoneManyFiles",cmd,myScen)
        
        time.sleep(30) # to wait before the query to the stager
        
        cmd=["stager_qry "+strMultiple]
        UtilityForCastorTest.saveOnFile(localDir+"ClientPutDoneManyFilesBis",cmd,myScen)
        
        fi=open(localDir+"ClientPutDoneManyFiles11","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.find("SUBREQUEST_READY") != -1, "putDone doesn't work with many files"
        
    
        fi=open(localDir+"ClientPutDoneManyFilesBis","r")
        buffOut=fi.read()
        fi.close()        
        assert (buffOut.count("CANBEMIGR") + buffOut.count("STAGED")) == 10 , "stager_qry doesn't work after a putDone with many files"+buffOut
        
        
    def putDoneR(self):    
        
        cmd=["stager_put -M "+dirCastor+"fileClientPutDoneR"+ticket+" -r","rfcp "+inputFile+" "+dirCastor+"fileClientPutDoneR"+ticket]
        cmd2=["stager_putdone -M "+dirCastor+"fileClientPutDoneR"+ticket+" -r ","stager_qry -M "+dirCastor+"fileClientPutDoneR"+ticket]
        
        UtilityForCastorTest.saveOnFile(localDir+"ClientPutDoneR",cmd,myScen)
        fi=open(localDir+"ClientPutDoneR","r")
        buffOut=fi.read() 
        fi.close()
        line=buffOut.split("\n")
        reqId=line[len(line)-3]
        
        cmd2=["stager_putdone -M "+dirCastor+"fileClientPutDoneR"+ticket+" -r "+reqId,"stager_qry -M "+dirCastor+"fileClientPutDoneR"+ticket]
        
        UtilityForCastorTest.saveOnFile(localDir+"ClientPutDoneRBis",cmd2,myScen)
        fi=open(localDir+"ClientPutDoneRBis","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_putdone doesn't work with -r"
        assert buffOut.find(reqId)  != -1, "stager_put doesn't work with svc class the option -r"

        fi=open(localDir+"ClientPutDoneRBis1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("CANBEMIGR") != -1 or  buffOut.find("STAGED") != -1, "stager_putdone doesn't work after a putDone -r"
    
class StagerGetCase(unittest.TestCase):
        
    def getAndRfcp(self):
        cmd=["stager_put -M "+dirCastor+"fileClientGetRfcp"+ticket,"stager_get -M "+dirCastor+"fileClientGetRfcp"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientGetRfcp"+ticket,"stager_get -M "+dirCastor+"fileClientGetRfcp"+ticket,"stager_putdone -M "+dirCastor+"fileClientGetRfcp"+ticket,"stager_get -M "+dirCastor+"fileClientGetRfcp"+ticket,"stager_qry -M "+dirCastor+"fileClientGetRfcp"+ticket]

        UtilityForCastorTest.saveOnFile(localDir+"ClientGetAndRfcp",cmd,myScen)
    
        fi=open(localDir+"ClientGetAndRfcp1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_FAILED") != -1, "stager_get doesn't work after prepare to put"

        fi=open(localDir+"ClientGetAndRfcp3","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_get doesn't work after stager_put and rfcp"
    
        fi=open(localDir+"ClientGetAndRfcp4","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_putdone doesn't work after put, rfcp and get"
        
    
        fi=open(localDir+"ClientGetAndRfcp5","r")
        assert fi.read().find("SUBREQUEST_READY") != -1, "stager_get doesn't work after a put, rfcp and putDone"
        fi.close()
        
        fi=open(localDir+"ClientGetAndRfcp6","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("CANBEMIGR") != -1 or  buffOut.find("STAGED"), "stager_qry doesn't work after a put, rfcp and putDone"
        
        
    def getManyFiles(self):
        strMultiple=""
        cmdRfcp=[]
        for index in range(10):
          strMultiple=strMultiple+" -M "+dirCastor+"fileClientManyPutDone"+str(index)+ticket
          cmdRfcp.append("rfcp "+inputFile+" "+dirCastor+"fileClientManyPutDone"+str(index)+ticket)
        
        cmd=["stager_put "+strMultiple]+cmdRfcp+["stager_get "+strMultiple,"stager_putdone "+strMultiple,"stager_get "+strMultiple]
        
        UtilityForCastorTest.saveOnFile(localDir+"ClientGetManyFiles",cmd,myScen)
    
        fi=open(localDir+"ClientGetManyFiles11","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.count("SUBREQUEST_READY") == 10, "stager_get doesn't work after stager_put with many files and rfcp"
        
        
        fi=open(localDir+"ClientGetManyFiles13","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.count("SUBREQUEST_READY") == 10, "stager_get doesn't work after put, rfcp and putdone with many files"
        
    
    def getTag(self):
        cmd=["stager_put -M "+dirCastor+"fileClientGetTag"+ticket+" -U tagGet"+ticket,"stager_get -M "+dirCastor+"fileClientGetTag"+ticket+" -U tagGet"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientGetTag"+ticket,"stager_get  -M "+dirCastor+"fileClientGetTag"+ticket+" -U tagGet"+ticket,"stager_putdone -M "+dirCastor+"fileClientGetTag"+ticket,"stager_get -M "+dirCastor+"fileClientGetTag"+ticket+" -U tagGet"+ticket ]

        UtilityForCastorTest.saveOnFile(localDir+"ClientGetTag",cmd,myScen)
    
        fi=open(localDir+"ClientGetTag1","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.find("SUBREQUEST_FAILED") != -1, "stager_get doesn't work with tag after put"
      
        fi=open(localDir+"ClientGetTag3","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_get doesn't work with tag after put and rfcp"
        

        fi=open(localDir+"ClientGetTag5","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_get doesn't work with tag after put, rfcp and putdone"
        
        
    def getSvcClass(self):
        cmd=["stager_put -M "+dirCastor+"fileClientGetSvc"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientGetSvc"+ticket,"stager_qry -M "+dirCastor+"fileClientGetSvc"+ticket+" -S "+stagerExtraSvcClass,"stager_get -M "+dirCastor+"fileClientGetSvc"+ticket+" -S "+stagerExtraSvcClass,"stager_put -M "+dirCastor+"fileClientGetSvcBis"+ticket+" -S "+stagerExtraSvcClass,"rfcp "+inputFile+" "+dirCastor+"fileClientGetSvcBis"+ticket,"stager_qry -M "+dirCastor+"fileClientGetSvcBis"+ticket+" -S "+stagerExtraSvcClass,"stager_get -M "+dirCastor+"fileClientGetSvcBis"+ticket+" -S "+stagerExtraSvcClass,"stager_qry -M "+dirCastor+"fileClientGetSvcBis"+ticket+" -S "+stagerSvcClass]

        UtilityForCastorTest.saveOnFile(localDir+"ClientGetSvc",cmd,myScen)
        
        fi=open(localDir+"ClientGetSvc2","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.rfind("No such file") != -1, "stager_get doesn't work with svc class option -S"
        assert checkSvcClass(buffOut, stagerExtraSvcClass), "stager_get doesn't work with svc class option -S"
        
        fi=open(localDir+"ClientGetSvc3","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.rfind("File is being written to in another SvcClass") != -1, "stager_get doesn't work with svc class option -S"
        assert checkSvcClass(buffOut, stagerExtraSvcClass), "stager_get doesn't work with svc class option -S"

        fi=open(localDir+"ClientGetSvc5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("Device or resource busy") != -1, "stager_get doesn't work with svc class option -S"
        
        fi=open(localDir+"ClientGetSvc7","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("File is being (re)created right now") != -1, "stager_get doesn't work with svc class option -S"
        assert checkSvcClass(buffOut, stagerExtraSvcClass), "stager_get doesn't work with svc class option -S"
        
        fi=open(localDir+"ClientGetSvc8","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("No such file") != -1, "stager_get doesn't work with svc class option -S"
        assert checkSvcClass(buffOut, stagerSvcClass), "stager_get doesn't work with svc class option -S"
        
    def getR(self):        
        cmd=["stager_put -M "+dirCastor+"fileClientGetR"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientGetR"+ticket,"stager_get -M "+dirCastor+"fileClientGetR"+ticket+" -r","stager_qry -M "+dirCastor+"fileClientGetR"+ticket]

        UtilityForCastorTest.saveOnFile(localDir+"ClientGetR",cmd,myScen)

        fi=open(localDir+"ClientGetR2","r")
        buffOut=fi.read()
        fi.close()
        
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_get doesn't work after put"

        line=buffOut.split("\n")

        reqId=line[len(line)-3]
        assert buffOut.count(reqId) != 3, "stager_put doesn't work with svc class the option -r"
        
    
        fi=open(localDir+"ClientGetR3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1, "stager_get doesn't work after put"
        
    def getStageOut(self):
        cmd=["stager_put -M "+dirCastor+"fileClientGetStageOut"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientGetStageOut"+ticket,"stager_qry -M "+dirCastor+"fileClientGetStageOut"+ticket,"stager_get -M "+dirCastor+"fileClientGetStageOut"+ticket,"stager_get -M "+dirCastor+"fileClientGetStageOut"+ticket+" -S "+stagerExtraSvcClass]

        UtilityForCastorTest.saveOnFile(localDir+"ClientGetStageOut",cmd,myScen)
        
        fi=open(localDir+"ClientGetStageOut2","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.find("STAGEOUT") != -1, "stager_put doesn't work"
        
        fi=open(localDir+"ClientGetStageOut3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_get doesn't work on a STAGEOUT file"

        fi=open(localDir+"ClientGetStageOut4","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.rfind("SUBREQUEST_FAILED 16 File is being written to in another SvcClass") != -1, "stager_get on a STAGEOUT file in another SvcClass did not work as expected"
        assert checkSvcClass(buffOut, stagerExtraSvcClass), "stager_get doesn't work with svc class option -S"

class StagerUpdCase(unittest.TestCase):
    def pupdExistingFile(self):
        cmd=["rfcp "+inputFile+" "+dirCastor+"fileClientpupdExistingFile"+ticket,"stager_update -M "+dirCastor+"fileClientpupdExistingFile"+ticket,"stager_qry -M "+dirCastor+"fileClientpupdExistingFile"+ticket,"stager_update -M "+dirCastor+"fileClientpupdExistingFile"+ticket,"stager_qry -M "+dirCastor+"fileClientpupdExistingFile"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientpupdExistingFile",cmd,myScen)

        fi=open(localDir+"ClientpupdExistingFile1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_update doesn't work on existing files"

        fi=open(localDir+"ClientpupdExistingFile2","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or  buffOut.find("CANBEMIGR") != -1 , "stager_update doesn't work on existing files"

        fi=open(localDir+"ClientpupdExistingFile3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_update doesn't work on existing files"

        fi=open(localDir+"ClientpupdExistingFile4","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or  buffOut.find("CANBEMIGR") != -1 , "stager_update doesn't work on existing files"

    def pupdNonExistingFile(self):
        cmd=["stager_update -M "+dirCastor+"fileClientpupdNonExistingFile"+ticket,"stager_qry -M "+dirCastor+"fileClientpupdNonExistingFile"+ticket,"stager_update -M "+dirCastor+"fileClientpupdNonExistingFile"+ticket,"stager_qry -M "+dirCastor+"fileClientpupdNonExistingFile"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientpupdNonExistingFile",cmd,myScen)

        fi=open(localDir+"ClientpupdNonExistingFile","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_update doesn't work on non existing files"

        fi=open(localDir+"ClientpupdNonExistingFile1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "stager_update doesn't work on non existing files"
        
        fi=open(localDir+"ClientpupdNonExistingFile2","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_FAILED") != -1, "stager_update doesn't work on non existing files"
        assert buffOut.find("File is being (re)created") != -1, "stager_update doesn't work on non existing files"

        fi=open(localDir+"ClientpupdNonExistingFile3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "stager_update doesn't work on non existing files"
        
    def pupdExistingFileReplicated(self):
        replEnv = UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerExtraSvcClass,stagerVersion)
        cmd=["rfcp "+inputFile+" "+dirCastor+"fileClientpupdExistingFileReplicated"+ticket,"stager_qry -M "+dirCastor+"fileClientpupdExistingFileReplicated"+ticket,replEnv+"rfcp "+dirCastor+"fileClientpupdExistingFileReplicated"+ticket+" "+localDir+"fileClientpupdExistingFileReplicated","stager_qry -M "+dirCastor+"fileClientpupdExistingFileReplicated"+ticket+ " -S "+stagerExtraSvcClass,"stager_update -M "+dirCastor+"fileClientpupdExistingFileReplicated"+ticket,"stager_qry -M "+dirCastor+"fileClientpupdExistingFileReplicated"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientpupdExistingFileReplicated",cmd,myScen)

        fi=open(localDir+"ClientpupdExistingFileReplicated","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through local \(in\) and eth[0-1] \(out\)',buffOut) != None, "stager_update doesn't work on replicated files"

        fi=open(localDir+"ClientpupdExistingFileReplicated1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "stager_update doesn't work on replicated files"

        fi=open(localDir+"ClientpupdExistingFileReplicated2","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through eth[0-1] \(in\) and local \(out\)',buffOut) != None, "stager_update doesn't work on replicated files"

        fi=open(localDir+"ClientpupdExistingFileReplicated3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "stager_update doesn't work on replicated files"

        fi=open(localDir+"ClientpupdExistingFileReplicated4","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_update doesn't work on replicated files"

        fi=open(localDir+"ClientpupdExistingFileReplicated5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "stager_update doesn't work on replicated files"

    def updExistingFile(self):
        cmd=["rfcp "+inputFile+" "+dirCastor+"fileClientupdExistingFile"+ticket,"rfcpupd "+updInputFile+" "+dirCastor+"fileClientupdExistingFile"+ticket,"stager_qry -M "+dirCastor+"fileClientupdExistingFile"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientupdExistingFile",cmd,myScen)

        fi=open(localDir+"ClientupdExistingFile1","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through local \(in\) and eth[0-1] \(out\)',buffOut) != None, "update doesn't work on an existing file"

        fi=open(localDir+"ClientupdExistingFile2","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or  buffOut.find("CANBEMIGR") != -1 , "update doesn't work on an existing file"

    def updNonExistingFile(self):
        cmd=["rfcpupd "+updInputFile+" "+dirCastor+"fileClientupdNonExistingFile"+ticket,"stager_qry -M "+dirCastor+"fileClientupdNonExistingFile"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientupdNonExistingFile",cmd,myScen)

        fi=open(localDir+"ClientupdNonExistingFile","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through local \(in\) and eth[0-1] \(out\)',buffOut) != None, "update doesn't work on a non existing file"

        fi=open(localDir+"ClientupdNonExistingFile1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or  buffOut.find("CANBEMIGR") != -1 , "update doesn't work on a non existing file"

    def updExistingFileReplicated(self):
        replEnv = UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerExtraSvcClass,stagerVersion)
        cmd=["rfcp "+inputFile+" "+dirCastor+"fileClientupdExistingFileReplicated"+ticket,"stager_qry -M "+dirCastor+"fileClientupdExistingFileReplicated"+ticket,replEnv+"rfcp "+dirCastor+"fileClientupdExistingFileReplicated"+ticket+" "+localDir+"fileClientupdExistingFileReplicated","stager_qry -M "+dirCastor+"fileClientupdExistingFileReplicated"+ticket+" -S "+stagerExtraSvcClass,"rfcpupd "+updInputFile+" "+dirCastor+"fileClientupdExistingFileReplicated"+ticket,"stager_qry -M "+dirCastor+"fileClientupdExistingFileReplicated"+ticket,"stager_qry -M "+dirCastor+"fileClientupdExistingFileReplicated"+ticket+" -S "+stagerExtraSvcClass]
        UtilityForCastorTest.saveOnFile(localDir+"ClientupdExistingFileReplicated",cmd,myScen)

        fi=open(localDir+"ClientupdExistingFileReplicated","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through local \(in\) and eth[0-1] \(out\)',buffOut) != None, "update doesn't work on a replicated file"

        fi=open(localDir+"ClientupdExistingFileReplicated1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "update doesn't work on a replicated file"

        fi=open(localDir+"ClientupdExistingFileReplicated2","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through eth[0-1] \(in\) and local \(out\)',buffOut) != None, "update doesn't work on a replicated file"

        fi=open(localDir+"ClientupdExistingFileReplicated3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "update doesn't work on a replicated file"

        fi=open(localDir+"ClientupdExistingFileReplicated4","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through local \(in\) and eth[0-1] \(out\)',buffOut) != None, "update doesn't work on a replicated file"

        fi=open(localDir+"ClientupdExistingFileReplicated5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or  buffOut.find("CANBEMIGR") != -1 , "update doesn't work on a replicated file"

        fi=open(localDir+"ClientupdExistingFileReplicated6","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("INVALID") != -1 , "update doesn't work on a replicated file"

    def pupdUpdPutDone(self):
        cmd=["stager_update -M "+dirCastor+"fileClientpupdUpdPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClientpupdUpdPutDone"+ticket,"rfcpupd "+updInputFile+" "+dirCastor+"fileClientpupdUpdPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClientpupdUpdPutDone"+ticket,"rfcpupd "+updInputFile+" "+dirCastor+"fileClientpupdUpdPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClientpupdUpdPutDone"+ticket,"stager_putdone -M "+dirCastor+"fileClientpupdUpdPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClientpupdUpdPutDone"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientpupdUpdPutDone",cmd,myScen)
        
        fi=open(localDir+"ClientpupdUpdPutDone","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_update cycle doesn't work"

        fi=open(localDir+"ClientpupdUpdPutDone1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "stager_update cycle doesn't work"

        fi=open(localDir+"ClientpupdUpdPutDone2","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through local \(in\) and eth[0-1] \(out\)',buffOut) != None, "stager_update cycle doesn't work"

        fi=open(localDir+"ClientpupdUpdPutDone3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "stager_update cycle doesn't work"

        fi=open(localDir+"ClientpupdUpdPutDone4","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through local \(in\) and eth[0-1] \(out\)',buffOut) != None, "stager_update cycle doesn't work"

        fi=open(localDir+"ClientpupdUpdPutDone5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "stager_update cycle doesn't work"

        fi=open(localDir+"ClientpupdUpdPutDone6","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_update cycle doesn't work"

        fi=open(localDir+"ClientpupdUpdPutDone7","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or  buffOut.find("CANBEMIGR") != -1 , "stager_update cycle doesn't work"

    def pputUpdPutDone(self):
        cmd=["stager_put -M "+dirCastor+"fileClientpputUpdPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClientpputUpdPutDone"+ticket,"rfcpupd "+updInputFile+" "+dirCastor+"fileClientpputUpdPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClientpputUpdPutDone"+ticket,"rfcpupd "+updInputFile+" "+dirCastor+"fileClientpputUpdPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClientpputUpdPutDone"+ticket,"stager_putdone -M "+dirCastor+"fileClientpputUpdPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClientpputUpdPutDone"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientpputUpdPutDone",cmd,myScen)
        
        fi=open(localDir+"ClientpputUpdPutDone","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "updates on prepareToPut do not work"

        fi=open(localDir+"ClientpputUpdPutDone1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "updates on prepareToPut do not work"

        fi=open(localDir+"ClientpputUpdPutDone2","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through local \(in\) and eth[0-1] \(out\)',buffOut) != None, "updates on prepareToPut do not work"

        fi=open(localDir+"ClientpputUpdPutDone3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "updates on prepareToPut do not work"

        fi=open(localDir+"ClientpputUpdPutDone4","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through local \(in\) and eth[0-1] \(out\)',buffOut) != None, "updates on prepareToPut do not work"

        fi=open(localDir+"ClientpputUpdPutDone5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "updates on prepareToPut do not work"

        fi=open(localDir+"ClientpputUpdPutDone6","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "updates on prepareToPut do not work"

        fi=open(localDir+"ClientpputUpdPutDone7","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or  buffOut.find("CANBEMIGR") != -1 , "updates on prepareToPut do not work"

    def pupdPutPutDone(self):
        cmd=["stager_update -M "+dirCastor+"fileClientpupdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClientpupdPutPutDone"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientpupdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClientpupdPutPutDone"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientpupdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClientpupdPutPutDone"+ticket,"stager_putdone -M "+dirCastor+"fileClientpupdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClientpupdPutPutDone"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientpupdPutPutDone",cmd,myScen)
        
        fi=open(localDir+"ClientpupdPutPutDone","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "puts on prepareToUpdate do not work"

        fi=open(localDir+"ClientpupdPutPutDone1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "puts on prepareToUpdate do not work"

        fi=open(localDir+"ClientpupdPutPutDone2","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through local \(in\) and eth[0-1] \(out\)',buffOut) != None, "puts on prepareToUpdate do not work"

        fi=open(localDir+"ClientpupdPutPutDone3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "puts on prepareToUpdate do not work"

        fi=open(localDir+"ClientpupdPutPutDone4","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through local \(in\) and eth[0-1] \(out\)',buffOut) != None, "puts on prepareToUpdate do not work"

        fi=open(localDir+"ClientpupdPutPutDone5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "puts on prepareToUpdate do not work"

        fi=open(localDir+"ClientpupdPutPutDone6","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "puts on prepareToUpdate do not work"

        fi=open(localDir+"ClientpupdPutPutDone7","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or  buffOut.find("CANBEMIGR") != -1 , "puts on prepareToUpdate do not work"

    def pPutUpdPutPutDone(self):
        cmd=["stager_put -M "+dirCastor+"fileClientpPutUpdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClientpPutUpdPutPutDone"+ticket,"rfcpupd "+updInputFile+" "+dirCastor+"fileClientpPutUpdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClientpPutUpdPutPutDone"+ticket,"rfcpupd "+updInputFile+" "+dirCastor+"fileClientpPutUpdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClientpPutUpdPutPutDone"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientpPutUpdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClientpPutUpdPutPutDone"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientpPutUpdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClientpPutUpdPutPutDone"+ticket,"stager_putdone -M "+dirCastor+"fileClientpPutUpdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClientpPutUpdPutPutDone"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientpPutUpdPutPutDone",cmd,myScen)
        
        fi=open(localDir+"ClientpPutUpdPutPutDone","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "mixing puts and updates does not work"

        fi=open(localDir+"ClientpPutUpdPutPutDone1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "mixing puts and updates does not work"

        fi=open(localDir+"ClientpPutUpdPutPutDone2","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through local \(in\) and eth[0-1] \(out\)',buffOut) != None, "mixing puts and updates does not work"

        fi=open(localDir+"ClientpPutUpdPutPutDone3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "mixing puts and updates does not work"

        fi=open(localDir+"ClientpPutUpdPutPutDone4","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through local \(in\) and eth[0-1] \(out\)',buffOut) != None, "mixing puts and updates does not work"

        fi=open(localDir+"ClientpPutUpdPutPutDone5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "mixing puts and updates does not work"

        fi=open(localDir+"ClientpPutUpdPutPutDone6","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through local \(in\) and eth[0-1] \(out\)',buffOut) != None, "mixing puts and updates does not work"

        fi=open(localDir+"ClientpPutUpdPutPutDone7","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "mixing puts and updates does not work"

        fi=open(localDir+"ClientpPutUpdPutPutDone8","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through local \(in\) and eth[0-1] \(out\)',buffOut) != None, "mixing puts and updates does not work"

        fi=open(localDir+"ClientpPutUpdPutPutDone9","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "mixing puts and updates does not work"

        fi=open(localDir+"ClientpPutUpdPutPutDone10","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "mixing puts and updates does not work"

        fi=open(localDir+"ClientpPutUpdPutPutDone11","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or  buffOut.find("CANBEMIGR") != -1 , "mixing puts and updates does not work"

    def pPutPUpd(self):
        cmd=["stager_put -M "+dirCastor+"fileClientpPutPUpd"+ticket,"stager_qry -M "+dirCastor+"fileClientpPutPUpd"+ticket,"stager_update -M "+dirCastor+"fileClientpPutPUpd"+ticket,"stager_qry -M "+dirCastor+"fileClientpPutPUpd"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientpPutPUpd"+ticket,"stager_qry -M "+dirCastor+"fileClientpPutPUpd"+ticket,"stager_update -M "+dirCastor+"fileClientpPutPUpd"+ticket,"stager_qry -M "+dirCastor+"fileClientpPutPUpd"+ticket,"stager_putdone -M "+dirCastor+"fileClientpPutPUpd"+ticket,"stager_qry -M "+dirCastor+"fileClientpPutPUpd"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientpPutPUpd",cmd,myScen)
        
        fi=open(localDir+"ClientpPutPUpd","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "prepareToUpdate on prepareToPut does not work"

        fi=open(localDir+"ClientpPutPUpd1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "prepareToUpdate on prepareToPut does not work"

        fi=open(localDir+"ClientpPutPUpd2","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_FAILED") != -1, "prepareToUpdate on prepareToPut does not work"
        assert buffOut.find("File is being (re)created right now") != -1, "prepareToUpdate on prepareToPut does not work"

        fi=open(localDir+"ClientpPutPUpd3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "prepareToUpdate on prepareToPut does not work"

        fi=open(localDir+"ClientpPutPUpd4","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through local \(in\) and eth[0-1] \(out\)',buffOut) != None, "prepareToUpdate on prepareToPut does not work"

        fi=open(localDir+"ClientpPutPUpd5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "prepareToUpdate on prepareToPut does not work"

        fi=open(localDir+"ClientpPutPUpd6","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "prepareToUpdate on prepareToPut does not work"

        fi=open(localDir+"ClientpPutPUpd7","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "prepareToUpdate on prepareToPut does not work"

        fi=open(localDir+"ClientpPutPUpd8","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "prepareToUpdate on prepareToPut does not work"

        fi=open(localDir+"ClientpPutPUpd9","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or  buffOut.find("CANBEMIGR") != -1 , "prepareToUpdate on prepareToPut does not work"

    def pUpdpPut(self):
        cmd=["rfcp "+inputFile+" "+dirCastor+"fileClientpUpdpPut"+ticket,"stager_update -M "+dirCastor+"fileClientpUpdpPut"+ticket,"stager_qry -M "+dirCastor+"fileClientpUpdpPut"+ticket,"stager_put -M "+dirCastor+"fileClientpUpdpPut"+ticket,"stager_qry -M "+dirCastor+"fileClientpUpdpPut"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientpUpdpPut"+ticket,"stager_qry -M "+dirCastor+"fileClientpUpdpPut"+ticket,"stager_putdone -M "+dirCastor+"fileClientpUpdpPut"+ticket,"stager_qry -M "+dirCastor+"fileClientpUpdpPut"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientpUpdpPut",cmd,myScen)
        
        fi=open(localDir+"ClientpUpdpPut","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through local \(in\) and eth[0-1] \(out\)',buffOut) != None, "mixing puts and updates does not work"

        fi=open(localDir+"ClientpUpdpPut1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"ClientpUpdpPut2","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or  buffOut.find("CANBEMIGR") != -1 , "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"ClientpUpdpPut3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"ClientpUpdpPut4","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 or  buffOut.find("CANBEMIGR") != -1 , "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"ClientpUpdpPut5","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through local \(in\) and eth[0-1] \(out\)',buffOut) != None, "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"ClientpUpdpPut6","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"ClientpUpdpPut7","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"ClientpUpdpPut8","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or  buffOut.find("CANBEMIGR") != -1 , "prepareToPut on prepareToUpdate does not work"

    def pUpdpPutNonExist(self):
        cmd=["stager_update -M "+dirCastor+"fileClientpUpdpPutNonExist"+ticket,"stager_qry -M "+dirCastor+"fileClientpUpdpPutNonExist"+ticket,"stager_put -M "+dirCastor+"fileClientpUpdpPutNonExist"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientpUpdpPutNonExist"+ticket,"stager_qry -M "+dirCastor+"fileClientpUpdpPutNonExist"+ticket,"stager_putdone -M "+dirCastor+"fileClientpUpdpPutNonExist"+ticket,"stager_qry -M "+dirCastor+"fileClientpUpdpPutNonExist"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientpUpdpPutNonExist",cmd,myScen)
        
        fi=open(localDir+"ClientpUpdpPutNonExist","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"ClientpUpdpPutNonExist1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"ClientpUpdpPutNonExist2","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_FAILED") != -1, "prepareToPut on prepareToUpdate does not work"
        assert buffOut.find("Another prepareToPut/Update is ongoing") != -1, "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"ClientpUpdpPutNonExist3","r")
        buffOut=fi.read()
        fi.close()
        assert re.search('through local \(in\) and eth[0-1] \(out\)',buffOut) != None, "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"ClientpUpdpPutNonExist4","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"ClientpUpdpPutNonExist5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"ClientpUpdpPutNonExist6","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or  buffOut.find("CANBEMIGR") != -1 , "prepareToPut on prepareToUpdate does not work"


class StagerRmCase(unittest.TestCase):
    def basicRm(self):
        cmd=["stager_put -M "+dirCastor+"fileClientBasicRm"+ticket,"stager_rm -M "+dirCastor+"fileClientBasicRm"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientBasicRm",cmd,myScen)
         
        fi=open(localDir+"ClientBasicRm1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_rm doesn't work after put"
        
        
    def rmAndRfcp(self):
        cmd=["stager_put -M "+dirCastor+"fileClientRmRfcp"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientRmRfcp"+ticket,"stager_rm -M "+dirCastor+"fileClientRmRfcp"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientRmAndRfcp",cmd,myScen)
    
        fi=open(localDir+"ClientRmAndRfcp2","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_rm doesn't work after put and rfcp"

        cmd=["stager_put -M "+dirCastor+"fileClientRmRfcpBis"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientRmRfcpBis"+ticket,"stager_putdone -M "+dirCastor+"fileClientRmRfcpBis"+ticket,"stager_qry -M "+dirCastor+"fileClientRmRfcpBis"+ticket,"stager_rm -M "+dirCastor+"fileClientRmRfcpBis"+ticket]

        UtilityForCastorTest.saveOnFile(localDir+"ClientRmAndRfcpBis",cmd,myScen)

        fi=open(localDir+"ClientRmAndRfcpBis3","r")
        buffOut=fi.read()
        fi.close()

        fi=open(localDir+"ClientRmAndRfcpBis4","r")
        buffOut2=fi.read()
        fi.close()
        
        assert (buffOut2.find("SUBREQUEST_FAILED") != -1 and  buffOut.find("CANBEMIGR") != -1) or (buffOut2.find("SUBREQUEST_READY") != -1 and  buffOut.find("STAGED") != -1), "stager_rm doesn't work after put, rfcp and putdone"
        # not migrated yet ....  
        
    def rmManyFiles(self):
        strMultiple=""
        cmdRfcp=[]
        for index in range(10):
          strMultiple=strMultiple+" -M "+dirCastor+"fileClientManyRm"+str(index)+ticket
          cmdRfcp.append("rfcp "+inputFile+" "+dirCastor+"fileClientManyRm"+str(index)+ticket)
        
        cmd=["stager_put "+strMultiple]+cmdRfcp+["stager_rm "+strMultiple]
        
        UtilityForCastorTest.saveOnFile(localDir+"ClientRmManyFiles",cmd,myScen)

        fi=open(localDir+"ClientRmManyFiles11","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.count("SUBREQUEST_READY") == 10, "stager_rm doesn't work with many files"
        
    def rmSvcClass(self):
        
        cmd=["stager_put -M "+dirCastor+"fileClientRmSvc"+ticket+" -S "+stagerSvcClass,"stager_rm -M "+dirCastor+"fileClientRmSvc"+ticket+" -S "+stagerExtraSvcClass,"stager_qry -M "+dirCastor+"fileClientRmSvc"+ticket]
        
        UtilityForCastorTest.saveOnFile(localDir+"ClientRmSvc",cmd,myScen)
        
        fi=open(localDir+"ClientRmSvc1","r")
        buffOut=fi.read()
        fi.close()        
        assert checkSvcClass(buffOut, stagerExtraSvcClass), "stager_rm doesn't work with svc class option -S"
        
        fi=open(localDir+"ClientRmSvc2","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.rfind("STAGEOUT") != -1, "stager_rm doesn't work with svc class option -S"

    def rmDualSvcClass(self):
        cmd=["STAGE_SVCCLASS="+ stagerDiskOnlySvcClass+" rfcp "+inputFile+" "+dirCastor+"fileClientRmDualSvc"+ticket,"stager_get -M "+dirCastor+"fileClientRmDualSvc"+ticket+" -S "+stagerExtraSvcClass]
        
        UtilityForCastorTest.saveOnFile(localDir+"ClientRmDualSvcP1",cmd,myScen)
        
        fi=open(localDir+"ClientRmDualSvcP11","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.rfind("SUBREQUEST_READY") != -1, "stager_get doesn't work with svc class option -S"
        
        buffOut=UtilityForCastorTest.runOnShell(["stager_qry -M "+dirCastor+"fileClientRmDualSvc"+ticket+" -S "+stagerExtraSvcClass],myScen)
        while buffOut[0].find("CANBEMIGR") == -1 and buffOut[0].find("STAGED") == -1:
          time.sleep(10)
          buffOut=UtilityForCastorTest.runOnShell(["stager_qry -M "+dirCastor+"fileClientRmDualSvc"+ticket+" -S "+stagerExtraSvcClass],myScen)
        
        cmd=["stager_rm -M "+dirCastor+"fileClientRmDualSvc"+ticket+" -S "+stagerDiskOnlySvcClass,"stager_qry -M "+dirCastor+"fileClientRmDualSvc"+ticket+" -S "+stagerExtraSvcClass,"stager_rm -M "+dirCastor+"fileClientRmDualSvc"+ticket+" -S "+stagerExtraSvcClass,"stager_qry -M "+dirCastor+"fileClientGetDualSvc"+ticket+" -S '*'"]
        UtilityForCastorTest.saveOnFile(localDir+"ClientRmDualSvcP2",cmd,myScen)

        fi=open(localDir+"ClientRmDualSvcP2","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.rfind("SUBREQUEST_READY") != -1, "stager_rm doesn't work with svc class option -S"

        fi=open(localDir+"ClientRmDualSvcP21","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1, "stager_qry doesn't work after stager_rm with svc class option -S"

        fi=open(localDir+"ClientRmDualSvcP22","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.rfind("SUBREQUEST_READY") != -1 or buffOut.find("SUBREQUEST_FAILED 16 The file is not yet migrated") != -1, "stager_rm doesn't work with svc class option -S"

        fi=open(localDir+"ClientRmDualSvcP23","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.rfind("No such file or directory") != -1, "stager_qry doesn't work after stager_rm with svc class option -S"
        
class StagerSpecialQueryCase(unittest.TestCase):
    def queryS(self):
        cmd=["stager_qry -s"]
        UtilityForCastorTest.saveOnFile(localDir+"ClientQueryS",cmd,myScen)
        
        fi=open(localDir+"ClientQueryS","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.find("Error") == -1, "stager_qry -s doesn't work"
        
        
    def queryE(self):
        cmd=["stager_put -M "+dirCastor+"fileClientQueryE"+ticket+"TEXT1","stager_put -M "+dirCastor+"fileClientQueryE"+ticket+"TEXT2","stager_qry -E "+dirCastor+"fileClientQueryE"+ticket+"*"]

        myScenE=UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerSvcClass,stagerVersion,[["STAGER_TRACE","3"]])
        
        UtilityForCastorTest.saveOnFile(localDir+"ClientQueryE",cmd,myScenE)
        fi=open(localDir+"ClientQueryE2","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.find("STAGEOUT") != -1, "stager_qry -E doesn't work"
        
class StagerDiskOnlyCase(unittest.TestCase):
    def forceFileClass(self):
        cmd=["stager_put -M "+dirCastor+"fileForceFileClass"+ticket+" -S "+stagerDiskOnlySvcClass,"nsls --class "+dirCastor+"fileForceFileClass"+ticket,"nslistclass --name "+stagerForcedFileClass+" | grep CLASS_ID"]
        UtilityForCastorTest.saveOnFile(localDir+"ClientForceFileClass",cmd,myScen)
        
        fi=open(localDir+"ClientForceFileClass1","r")
        buffOut=fi.read()
        fileClassId = int(buffOut.split()[0])
        fi.close()
        
        fi=open(localDir+"ClientForceFileClass2","r")
        buffOut=fi.read()
        expectedFileClassId = int(buffOut.split()[1])
        fi.close()
        assert fileClassId == expectedFileClassId, "Forcing of fileClass in Disk only pools does not work"
        
        
class StagerExtraTestCase(unittest.TestCase):
    def putDoneAndLongFile(self):
        fileBig=makeBigFile(inputFile)
    
        cmd1=["stager_put -M "+dirCastor+"fileClientPutDoneLongFile"+ticket,"rfcp "+fileBig+" "+dirCastor+"fileClientPutDoneLongFile"+ticket]
        cmd2=["stager_rm -M "+dirCastor+"fileClientPutDoneLongFile"+ticket,"stager_putdone -M "+dirCastor+"fileClientPutDoneLongFile"+ticket]

        UtilityForCastorTest.runOnShell(cmd1,myScen)
        UtilityForCastorTest.saveOnFile(localDir+"ClientPutDoneAndLongFile",cmd2,myScen)
        os.system("rm "+fileBig) 
        fi=open(localDir+"ClientPutDoneAndLongFile1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_FAILED") != -1, "putDone doesn't work with big files"
    
    def srmSimulation(self):
        cmd=["stager_put -M "+dirCastor+"fileClientSrm"+ticket,"touch /tmp/fileSrm"+ticket,"rfcp  /tmp/fileSrm"+ticket+" "+dirCastor+"fileClientSrm"+ticket,"stager_putdone -M "+dirCastor+"fileClientSrm"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientSrmSim",cmd,myScen)
        fi=open(localDir+"ClientSrmSim","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "srm simulation failed"
        
        fi=open(localDir+"ClientSrmSim2","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("0 bytes transferred") != -1, "srm simulation failed"

        fi=open(localDir+"ClientSrmSim3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "srm simulation failed"
      
    def putSizeCheck(self):
        cmd=["stager_put -M "+dirCastor+"fileSizeCheck"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileSizeCheck"+ticket,"nsls -l "+dirCastor+"fileSizeCheck"+ticket,"stager_putdone -M "+dirCastor+"fileSizeCheck"+ticket,"nsls -l "+dirCastor+"fileSizeCheck"+ticket,"stager_qry -M "+dirCastor+"fileSizeCheck"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientSizeCheck",cmd,myScen)
        fileSize=os.stat(inputFile)[6]
        fi=open(localDir+"ClientSizeCheck5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or  buffOut.find("CANBEMIGR") != -1 , "the putdone failed"
        
        fi=open(localDir+"ClientSizeCheck2","r")
        nsSize=fi.read().split()[4]
        fi.close()
        assert int(nsSize) == int(fileSize), "The size of a file is not the right one after the putdone"
        
        fi=open(localDir+"ClientSizeCheck4","r")
        nsSize=fi.read().split()[4]
        fi.close()
        assert int(nsSize) == int(fileSize), "The size of a file is not the right one after the putdone"
        
        cmd2=["rfcp "+inputFile+" "+dirCastor+"fileSizeCheckBis"+ticket,"nsls -l "+dirCastor+"fileSizeCheckBis"+ticket,"stager_qry -M "+dirCastor+"fileSizeCheckBis"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"ClientSizeCheckBis",cmd2,myScen)
        
        fi=open(localDir+"ClientSizeCheckBis2","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or  buffOut.find("CANBEMIGR") != -1 , "the putdone failed failed"
        
        fi=open(localDir+"ClientSizeCheckBis1","r")
        nsSize=fi.read().split()[4]
        fi.close()
        assert int(nsSize) == int(fileSize), "The size of a file is not the right one after the putdone"
    

casesPreClient=("mainScenarium","stagerRfioFine")
casesPut=("basicPut","putAndRfcp","putManyFiles","putTag","putSvcClass","putR")
casesPutDone=("basicPutDone","putDoneAndRfcp","putDoneManyFiles","putDoneR")
casesGet=("getAndRfcp","getManyFiles","getTag","getSvcClass","getR","getStageOut")
casesUpd=("pupdExistingFile", "pupdNonExistingFile", "pupdExistingFileReplicated", "updExistingFile", "updNonExistingFile", "updExistingFileReplicated", "pupdUpdPutDone", "pputUpdPutDone", "pupdPutPutDone", "pPutUpdPutPutDone", "pPutPUpd", "pUpdpPut", "pUpdpPutNonExist")
casesRm=("basicRm","rmAndRfcp","rmManyFiles","rmSvcClass","rmDualSvcClass")
casesQuery=("queryS","queryE")
casesDiskOnly=("forceFileClass",)
casesExtraTest=("putDoneAndLongFile","srmSimulation","putSizeCheck")

class StagerPreClientSuite(unittest.TestSuite):
    def __init__(self):
      unittest.TestSuite.__init__(self,map(PreRequisitesCase,casesPreClient))

class StagerPutSuite(unittest.TestSuite):
    def __init__(self):
      unittest.TestSuite.__init__(self,map(StagerPutCase,casesPut))

class StagerPutDoneSuite(unittest.TestSuite):
    def __init__(self):
      unittest.TestSuite.__init__(self,map(StagerPutDoneCase,casesPutDone))

class StagerGetSuite(unittest.TestSuite):
    def __init__(self):
      unittest.TestSuite.__init__(self,map(StagerGetCase,casesGet))

class StagerUpdSuite(unittest.TestSuite):
    def __init__(self):
      unittest.TestSuite.__init__(self,map(StagerUpdCase,casesUpd))

class StagerRmSuite(unittest.TestSuite):
    def __init__(self):
      unittest.TestSuite.__init__(self,map(StagerRmCase,casesRm))
    
class StagerQuerySpecialSuite(unittest.TestSuite):
    def __init__(self):
      unittest.TestSuite.__init__(self,map(StagerSpecialQueryCase,casesQuery))

class StagerDiskOnlySuite(unittest.TestSuite):
    def __init__(self):
      unittest.TestSuite.__init__(self,map(StagerDiskOnlyCase,casesDiskOnly))

class StagerExtraTestSuite(unittest.TestSuite):
    def __init__(self):
      unittest.TestSuite.__init__(self,map(StagerExtraTestCase,casesExtraTest))
