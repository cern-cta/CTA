import unittest
import UtilityForCastorTest
import os
import re
import sys
import time
import threading
import UtilityForCastorTest
from UtilityForCastorTest import stagerHost,stagerPort,stagerSvcClass,stagerTimeOut,stagerExtraSvcClass,stagerDiskOnlySvcClass,stagerForcedFileClass,quietMode,outputDir,configFile,ticket

# parameters
myScen=""

# files and directories
inputFile=""
updInputFile=""
localDir=""
dirCastor=outputDir+"/tmpClientTest"+ticket+"/"

def checkSvcClass(output, svcClass):
    if output.find("Using "+svcClass) != -1 or output.find("Opt SVCCLASS="+svcClass) != -1:
        return True
    else:
        return False

class PreRequisitesCase(unittest.TestCase):
    def mainScenarium(self):
        assert (UtilityForCastorTest.checkUser() != -1), "you don't have acccess to directory \"" + outputDir + "\" where you wanted to run the test"
        try:
            global localDir, inputFile, myScen, updInputFile, notEnoughSvcClasses
            params = UtilityForCastorTest.parseConfigFile(configFile, "Client")
            localDir = params["LOG_DIR"]
            localDir=localDir+ticket+"/"
            os.system("mkdir "+localDir)
            inputFile = params["INPUT_FILE"]
            updInputFile = localDir+"rfcpupdFile"
            os.system("echo 'extra data' > "+updInputFile)
            myScen=UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerSvcClass,None,[["STAGER_TRACE","3"]])
            UtilityForCastorTest.runOnShell(["nsmkdir "+dirCastor],myScen)

            notEnoughSvcClasses=0
            if stagerSvcClass.lower() == stagerExtraSvcClass.lower():
                print '\n  The following test cases will be skipped as not enough services classes have been defined.'
                print '  In order to have these tests run the STAGE_SVCCLASS and EXTRA_SVCCLASS options in the'
                print '  configuration file must be different.'
                print '     putSvcClass                (ClientTest.StagerPutCase)'
                print '     getSvcClass                (ClientTest.StagerGetCase)'
                print '     getStageOut                (ClientTest.StagerGetCase)'
                print '     rmSvcClass                 (ClientTest.StagerRmCase)'
                print '     rmDualSvcClass             (ClientTest.StagerRmCase)'
                print '     pupdExistingFileReplicated (ClientTest.StagerUpdRfiov2Case)'
                print '     pupdExistingFileReplicated (ClientTest.StagerUpdRfiov3Case)'
                print '     updExistingFileReplicated  (ClientTest.StagerUpdRfiov2Case)'
                print '     updExistingFileReplicated  (ClientTest.StagerUpdRfiov3Case) ... ',
                notEnoughSvcClasses=1

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
        if notEnoughSvcClasses:
            return

        cmd=["stager_put -M "+dirCastor+"fileClientPutSvc1"+ticket+" -S "+stagerSvcClass,"stager_qry -M "+dirCastor+"fileClientPutSvc1"+ticket+" -S "+stagerExtraSvcClass,"stager_put -M "+dirCastor+"fileClientPutSvc2"+ticket+" -S "+stagerExtraSvcClass,"stager_qry -M "+dirCastor+"fileClientPutSvc2"+ticket+" -S "+stagerExtraSvcClass]
        UtilityForCastorTest.saveOnFile(localDir+"ClientPutSvc",cmd,myScen)

        fi=open(localDir+"ClientPutSvc","r")
        buffOut=fi.read()
        fi.close()
        assert checkSvcClass(buffOut, stagerSvcClass), "stager_put doesn't work with svc class option -S"

        fi=open(localDir+"ClientPutSvc1","r")
        buffOut=fi.read()
        fi.close()
        assert checkSvcClass(buffOut, stagerExtraSvcClass), "stager_qry doesn't work with svc class option -S"
        assert buffOut.find("No such file") != -1, "stager_qry doesn't work after a simple put"

        fi=open(localDir+"ClientPutSvc2","r")
        buffOut=fi.read()
        fi.close()
        assert checkSvcClass(buffOut, stagerExtraSvcClass), "stager_qry doesn't work with svc class option -S"

        fi=open(localDir+"ClientPutSvc3","r")
        buffOut=fi.read()
        fi.close()
        assert checkSvcClass(buffOut, stagerExtraSvcClass), "stager_qry doesn't work with svc class option -S"
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
        assert buffOut.rfind("STAGEOUT") != -1, "stager_qry doesn't work with the option -r"


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
        assert buffOut.find(reqId) != -1, "stager_put doesn't work with svc class the option -r"

        fi=open(localDir+"ClientPutDoneRBis1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("CANBEMIGR") != -1 or buffOut.find("STAGED") != -1, "stager_putdone doesn't work after a putDone -r"


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
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_get doesn't work after a put, rfcp and putDone"

        fi=open(localDir+"ClientGetAndRfcp6","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("CANBEMIGR") != -1 or buffOut.find("STAGED"), "stager_qry doesn't work after a put, rfcp and putDone"

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
        if notEnoughSvcClasses:
            return

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
        assert buffOut.rfind("File is being written to in another service class") != -1, "stager_get doesn't work with svc class option -S"
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
        if notEnoughSvcClasses:
            return

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
        assert buffOut.rfind("SUBREQUEST_FAILED 16 File is being written to in another service class") != -1, "stager_get on a STAGEOUT file in another SvcClass did not work as expected"
        assert checkSvcClass(buffOut, stagerExtraSvcClass), "stager_get doesn't work with svc class option -S"


class StagerUpdCase(unittest.TestCase):
    def pupdExistingFile(self):
        cmd=["rfcp "+inputFile+" "+dirCastor+"fileClient"+rfiover+"pupdExistingFile"+ticket,"stager_update -M "+dirCastor+"fileClient"+rfiover+"pupdExistingFile"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pupdExistingFile"+ticket,"stager_update -M "+dirCastor+"fileClient"+rfiover+"pupdExistingFile"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pupdExistingFile"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"Client"+rfiover+"pupdExistingFile",cmd,myScen)

        fi=open(localDir+"Client"+rfiover+"pupdExistingFile1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_update doesn't work on existing files"

        fi=open(localDir+"Client"+rfiover+"pupdExistingFile2","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "stager_update doesn't work on existing files"

        fi=open(localDir+"Client"+rfiover+"pupdExistingFile3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_update doesn't work on existing files"

        fi=open(localDir+"Client"+rfiover+"pupdExistingFile4","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "stager_update doesn't work on existing files"

    def pupdNonExistingFile(self):
        cmd=["stager_update -M "+dirCastor+"fileClient"+rfiover+"pupdNonExistingFile"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pupdNonExistingFile"+ticket,"stager_update -M "+dirCastor+"fileClient"+rfiover+"pupdNonExistingFile"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pupdNonExistingFile"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"Client"+rfiover+"pupdNonExistingFile",cmd,myScen)

        fi=open(localDir+"Client"+rfiover+"pupdNonExistingFile","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_update doesn't work on non existing files"

        fi=open(localDir+"Client"+rfiover+"pupdNonExistingFile1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "stager_update doesn't work on non existing files"

        fi=open(localDir+"Client"+rfiover+"pupdNonExistingFile2","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_FAILED") != -1, "stager_update doesn't work on non existing files"
        assert buffOut.find("File is being (re)created") != -1, "stager_update doesn't work on non existing files"

        fi=open(localDir+"Client"+rfiover+"pupdNonExistingFile3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "stager_update doesn't work on non existing files"

    def pupdExistingFileReplicated(self):
        if notEnoughSvcClasses:
            return

        replEnv = UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerExtraSvcClass)
        cmd=["rfcp "+inputFile+" "+dirCastor+"fileClient"+rfiover+"pupdExistingFileReplicated"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pupdExistingFileReplicated"+ticket,replEnv+"rfcp "+dirCastor+"fileClient"+rfiover+"pupdExistingFileReplicated"+ticket+" "+localDir+"fileClient"+rfiover+"pupdExistingFileReplicated","stager_qry -M "+dirCastor+"fileClient"+rfiover+"pupdExistingFileReplicated"+ticket+ " -S "+stagerExtraSvcClass,"stager_update -M "+dirCastor+"fileClient"+rfiover+"pupdExistingFileReplicated"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pupdExistingFileReplicated"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"Client"+rfiover+"pupdExistingFileReplicated",cmd,myScen)

        fi=open(localDir+"Client"+rfiover+"pupdExistingFileReplicated","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 1), "stager_update doesn't work on replicated files"

        fi=open(localDir+"Client"+rfiover+"pupdExistingFileReplicated1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "stager_update doesn't work on replicated files"

        fi=open(localDir+"Client"+rfiover+"pupdExistingFileReplicated2","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 0), "stager_update doesn't work on replicated files"

        fi=open(localDir+"Client"+rfiover+"pupdExistingFileReplicated3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "stager_update doesn't work on replicated files"

        fi=open(localDir+"Client"+rfiover+"pupdExistingFileReplicated4","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_update doesn't work on replicated files"

        fi=open(localDir+"Client"+rfiover+"pupdExistingFileReplicated5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "stager_update doesn't work on replicated files"

    def updExistingFile(self):
        cmd=["rfcp "+inputFile+" "+dirCastor+"fileClient"+rfiover+"updExistingFile"+ticket,"rfcpupd "+rfiover+" "+updInputFile+" "+dirCastor+"fileClient"+rfiover+"updExistingFile"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"updExistingFile"+ticket,"rfcp "+dirCastor+"fileClient"+rfiover+"updExistingFile"+ticket+" "+localDir+"Read"+rfiover+"updExistingFile"]
        UtilityForCastorTest.saveOnFile(localDir+"Client"+rfiover+"updExistingFile",cmd,myScen)

        fi=open(localDir+"Client"+rfiover+"updExistingFile1","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 1, 1), "update doesn't work on an existing file"

        fi=open(localDir+"Client"+rfiover+"updExistingFile2","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "update doesn't work on an existing file"

        fi=open(localDir+"Client"+rfiover+"updExistingFile3","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 0), "update doesn't work on an existing file"

    def updNonExistingFile(self):
        cmd=["rfcpupd "+rfiover+" "+updInputFile+" "+dirCastor+"fileClient"+rfiover+"updNonExistingFile"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"updNonExistingFile"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"Client"+rfiover+"updNonExistingFile",cmd,myScen)

        fi=open(localDir+"Client"+rfiover+"updNonExistingFile","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 1), "update doesn't work on a non existing file"

        fi=open(localDir+"Client"+rfiover+"updNonExistingFile1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "update doesn't work on a non existing file"

    def updExistingFileReplicated(self):
        if notEnoughSvcClasses:
            return

        replEnv = UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerExtraSvcClass)
        cmd=["rfcp "+inputFile+" "+dirCastor+"fileClient"+rfiover+"updExistingFileReplicated"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"updExistingFileReplicated"+ticket,replEnv+"rfcp "+dirCastor+"fileClient"+rfiover+"updExistingFileReplicated"+ticket+" "+localDir+"fileClient"+rfiover+"updExistingFileReplicated","stager_qry -M "+dirCastor+"fileClient"+rfiover+"updExistingFileReplicated"+ticket+" -S "+stagerExtraSvcClass,"rfcpupd "+rfiover+" "+updInputFile+" "+dirCastor+"fileClient"+rfiover+"updExistingFileReplicated"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"updExistingFileReplicated"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"updExistingFileReplicated"+ticket+" -S "+stagerExtraSvcClass,"rfcp "+dirCastor+"fileClient"+rfiover+"updExistingFileReplicated"+ticket+" "+localDir+"Read"+rfiover+"updExistingFileReplicated"]
        UtilityForCastorTest.saveOnFile(localDir+"Client"+rfiover+"updExistingFileReplicated",cmd,myScen)

        fi=open(localDir+"Client"+rfiover+"updExistingFileReplicated","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 1), "update doesn't work on a replicated file"

        fi=open(localDir+"Client"+rfiover+"updExistingFileReplicated1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "update doesn't work on a replicated file"

        fi=open(localDir+"Client"+rfiover+"updExistingFileReplicated2","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 0), "update doesn't work on a replicated file"

        fi=open(localDir+"Client"+rfiover+"updExistingFileReplicated3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "update doesn't work on a replicated file"

        fi=open(localDir+"Client"+rfiover+"updExistingFileReplicated4","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 1, 1), "update doesn't work on a replicated file"

        fi=open(localDir+"Client"+rfiover+"updExistingFileReplicated5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "update doesn't work on a replicated file"

        fi=open(localDir+"Client"+rfiover+"updExistingFileReplicated6","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("INVALID") != -1 , "update doesn't work on a replicated file"

        fi=open(localDir+"Client"+rfiover+"updExistingFileReplicated7","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 0), "update doesn't work on a replicated file"

    def pupdUpdPutDone(self):
        cmd=["stager_update -M "+dirCastor+"fileClient"+rfiover+"pupdUpdPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pupdUpdPutDone"+ticket,"rfcpupd "+rfiover+" "+updInputFile+" "+dirCastor+"fileClient"+rfiover+"pupdUpdPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pupdUpdPutDone"+ticket,"rfcpupd "+rfiover+" "+updInputFile+" "+dirCastor+"fileClient"+rfiover+"pupdUpdPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pupdUpdPutDone"+ticket,"stager_putdone -M "+dirCastor+"fileClient"+rfiover+"pupdUpdPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pupdUpdPutDone"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"Client"+rfiover+"pupdUpdPutDone",cmd,myScen)

        fi=open(localDir+"Client"+rfiover+"pupdUpdPutDone","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_update cycle doesn't work"

        fi=open(localDir+"Client"+rfiover+"pupdUpdPutDone1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "stager_update cycle doesn't work"

        fi=open(localDir+"Client"+rfiover+"pupdUpdPutDone2","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 1), "stager_update cycle doesn't work"

        fi=open(localDir+"Client"+rfiover+"pupdUpdPutDone3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "stager_update cycle doesn't work"

        fi=open(localDir+"Client"+rfiover+"pupdUpdPutDone4","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 1, 1), "stager_update cycle doesn't work"

        fi=open(localDir+"Client"+rfiover+"pupdUpdPutDone5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "stager_update cycle doesn't work"

        fi=open(localDir+"Client"+rfiover+"pupdUpdPutDone6","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_update cycle doesn't work"

        fi=open(localDir+"Client"+rfiover+"pupdUpdPutDone7","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "stager_update cycle doesn't work"

    def pputUpdPutDone(self):
        cmd=["stager_put -M "+dirCastor+"fileClient"+rfiover+"pputUpdPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pputUpdPutDone"+ticket,"rfcpupd "+rfiover+" "+updInputFile+" "+dirCastor+"fileClient"+rfiover+"pputUpdPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pputUpdPutDone"+ticket,"rfcpupd "+rfiover+" "+updInputFile+" "+dirCastor+"fileClient"+rfiover+"pputUpdPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pputUpdPutDone"+ticket,"stager_putdone -M "+dirCastor+"fileClient"+rfiover+"pputUpdPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pputUpdPutDone"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"Client"+rfiover+"pputUpdPutDone",cmd,myScen)

        fi=open(localDir+"Client"+rfiover+"pputUpdPutDone","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "updates on prepareToPut do not work"

        fi=open(localDir+"Client"+rfiover+"pputUpdPutDone1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "updates on prepareToPut do not work"

        fi=open(localDir+"Client"+rfiover+"pputUpdPutDone2","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 1), "updates on prepareToPut do not work"

        fi=open(localDir+"Client"+rfiover+"pputUpdPutDone3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "updates on prepareToPut do not work"

        fi=open(localDir+"Client"+rfiover+"pputUpdPutDone4","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 1, 1), "updates on prepareToPut do not work"

        fi=open(localDir+"Client"+rfiover+"pputUpdPutDone5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "updates on prepareToPut do not work"

        fi=open(localDir+"Client"+rfiover+"pputUpdPutDone6","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "updates on prepareToPut do not work"

        fi=open(localDir+"Client"+rfiover+"pputUpdPutDone7","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "updates on prepareToPut do not work"

    def pupdPutPutDone(self):
        cmd=["stager_update -M "+dirCastor+"fileClient"+rfiover+"pupdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pupdPutPutDone"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClient"+rfiover+"pupdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pupdPutPutDone"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClient"+rfiover+"pupdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pupdPutPutDone"+ticket,"stager_putdone -M "+dirCastor+"fileClient"+rfiover+"pupdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pupdPutPutDone"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"Client"+rfiover+"pupdPutPutDone",cmd,myScen)

        fi=open(localDir+"Client"+rfiover+"pupdPutPutDone","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "puts on prepareToUpdate do not work"

        fi=open(localDir+"Client"+rfiover+"pupdPutPutDone1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "puts on prepareToUpdate do not work"

        fi=open(localDir+"Client"+rfiover+"pupdPutPutDone2","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 1), "puts on prepareToUpdate do not work"

        fi=open(localDir+"Client"+rfiover+"pupdPutPutDone3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "puts on prepareToUpdate do not work"

        fi=open(localDir+"Client"+rfiover+"pupdPutPutDone4","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 1), "puts on prepareToUpdate do not work"

        fi=open(localDir+"Client"+rfiover+"pupdPutPutDone5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "puts on prepareToUpdate do not work"

        fi=open(localDir+"Client"+rfiover+"pupdPutPutDone6","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "puts on prepareToUpdate do not work"

        fi=open(localDir+"Client"+rfiover+"pupdPutPutDone7","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "puts on prepareToUpdate do not work"

    def pPutUpdPutPutDone(self):
        cmd=["stager_put -M "+dirCastor+"fileClient"+rfiover+"pPutUpdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pPutUpdPutPutDone"+ticket,"rfcpupd "+rfiover+" "+updInputFile+" "+dirCastor+"fileClient"+rfiover+"pPutUpdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pPutUpdPutPutDone"+ticket,"rfcpupd "+rfiover+" "+updInputFile+" "+dirCastor+"fileClient"+rfiover+"pPutUpdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pPutUpdPutPutDone"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClient"+rfiover+"pPutUpdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pPutUpdPutPutDone"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClient"+rfiover+"pPutUpdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pPutUpdPutPutDone"+ticket,"stager_putdone -M "+dirCastor+"fileClient"+rfiover+"pPutUpdPutPutDone"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pPutUpdPutPutDone"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"Client"+rfiover+"pPutUpdPutPutDone",cmd,myScen)

        fi=open(localDir+"Client"+rfiover+"pPutUpdPutPutDone","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "mixing puts and updates does not work"

        fi=open(localDir+"Client"+rfiover+"pPutUpdPutPutDone1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "mixing puts and updates does not work"

        fi=open(localDir+"Client"+rfiover+"pPutUpdPutPutDone2","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 1), "mixing puts and updates does not work"

        fi=open(localDir+"Client"+rfiover+"pPutUpdPutPutDone3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "mixing puts and updates does not work"

        fi=open(localDir+"Client"+rfiover+"pPutUpdPutPutDone4","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 1, 1), "mixing puts and updates does not work"

        fi=open(localDir+"Client"+rfiover+"pPutUpdPutPutDone5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "mixing puts and updates does not work"

        fi=open(localDir+"Client"+rfiover+"pPutUpdPutPutDone6","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 1), "mixing puts and updates does not work"

        fi=open(localDir+"Client"+rfiover+"pPutUpdPutPutDone7","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "mixing puts and updates does not work"

        fi=open(localDir+"Client"+rfiover+"pPutUpdPutPutDone8","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 1), "mixing puts and updates does not work"

        fi=open(localDir+"Client"+rfiover+"pPutUpdPutPutDone9","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "mixing puts and updates does not work"

        fi=open(localDir+"Client"+rfiover+"pPutUpdPutPutDone10","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "mixing puts and updates does not work"

        fi=open(localDir+"Client"+rfiover+"pPutUpdPutPutDone11","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "mixing puts and updates does not work"

    def pPutPUpd(self):
        cmd=["stager_put -M "+dirCastor+"fileClient"+rfiover+"pPutPUpd"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pPutPUpd"+ticket,"stager_update -M "+dirCastor+"fileClient"+rfiover+"pPutPUpd"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pPutPUpd"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClient"+rfiover+"pPutPUpd"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pPutPUpd"+ticket,"stager_update -M "+dirCastor+"fileClient"+rfiover+"pPutPUpd"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pPutPUpd"+ticket,"stager_putdone -M "+dirCastor+"fileClient"+rfiover+"pPutPUpd"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pPutPUpd"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"Client"+rfiover+"pPutPUpd",cmd,myScen)

        fi=open(localDir+"Client"+rfiover+"pPutPUpd","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "prepareToUpdate on prepareToPut does not work"

        fi=open(localDir+"Client"+rfiover+"pPutPUpd1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "prepareToUpdate on prepareToPut does not work"

        fi=open(localDir+"Client"+rfiover+"pPutPUpd2","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_FAILED") != -1, "prepareToUpdate on prepareToPut does not work"
        assert buffOut.find("File is being (re)created right now") != -1, "prepareToUpdate on prepareToPut does not work"

        fi=open(localDir+"Client"+rfiover+"pPutPUpd3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "prepareToUpdate on prepareToPut does not work"

        fi=open(localDir+"Client"+rfiover+"pPutPUpd4","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 1), "prepareToUpdate on prepareToPut does not work"

        fi=open(localDir+"Client"+rfiover+"pPutPUpd5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "prepareToUpdate on prepareToPut does not work"

        fi=open(localDir+"Client"+rfiover+"pPutPUpd6","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "prepareToUpdate on prepareToPut does not work"

        fi=open(localDir+"Client"+rfiover+"pPutPUpd7","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "prepareToUpdate on prepareToPut does not work"

        fi=open(localDir+"Client"+rfiover+"pPutPUpd8","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "prepareToUpdate on prepareToPut does not work"

        fi=open(localDir+"Client"+rfiover+"pPutPUpd9","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "prepareToUpdate on prepareToPut does not work"

    def pUpdpPut(self):
        cmd=["rfcp "+inputFile+" "+dirCastor+"fileClient"+rfiover+"pUpdpPut"+ticket,"stager_update -M "+dirCastor+"fileClient"+rfiover+"pUpdpPut"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pUpdpPut"+ticket,"stager_put -M "+dirCastor+"fileClient"+rfiover+"pUpdpPut"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pUpdpPut"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClient"+rfiover+"pUpdpPut"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pUpdpPut"+ticket,"stager_putdone -M "+dirCastor+"fileClient"+rfiover+"pUpdpPut"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pUpdpPut"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"Client"+rfiover+"pUpdpPut",cmd,myScen)

        fi=open(localDir+"Client"+rfiover+"pUpdpPut","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 1), "mixing puts and updates does not work"

        fi=open(localDir+"Client"+rfiover+"pUpdpPut1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"Client"+rfiover+"pUpdpPut2","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"Client"+rfiover+"pUpdpPut3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"Client"+rfiover+"pUpdpPut4","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 or buffOut.find("CANBEMIGR") != -1 , "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"Client"+rfiover+"pUpdpPut5","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 1), "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"Client"+rfiover+"pUpdpPut6","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"Client"+rfiover+"pUpdpPut7","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"Client"+rfiover+"pUpdpPut8","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "prepareToPut on prepareToUpdate does not work"

    def pUpdpPutNonExist(self):
        cmd=["stager_update -M "+dirCastor+"fileClient"+rfiover+"pUpdpPutNonExist"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pUpdpPutNonExist"+ticket,"stager_put -M "+dirCastor+"fileClient"+rfiover+"pUpdpPutNonExist"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClient"+rfiover+"pUpdpPutNonExist"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pUpdpPutNonExist"+ticket,"stager_putdone -M "+dirCastor+"fileClient"+rfiover+"pUpdpPutNonExist"+ticket,"stager_qry -M "+dirCastor+"fileClient"+rfiover+"pUpdpPutNonExist"+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"Client"+rfiover+"pUpdpPutNonExist",cmd,myScen)

        fi=open(localDir+"Client"+rfiover+"pUpdpPutNonExist","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"Client"+rfiover+"pUpdpPutNonExist1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"Client"+rfiover+"pUpdpPutNonExist2","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_FAILED") != -1, "prepareToPut on prepareToUpdate does not work"
        assert buffOut.find("Another prepareToPut/Update is ongoing") != -1, "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"Client"+rfiover+"pUpdpPutNonExist3","r")
        buffOut=fi.read()
        fi.close()
        assert UtilityForCastorTest.checkRfcpOutput(buffOut, 1), "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"Client"+rfiover+"pUpdpPutNonExist4","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1 , "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"Client"+rfiover+"pUpdpPutNonExist5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "prepareToPut on prepareToUpdate does not work"

        fi=open(localDir+"Client"+rfiover+"pUpdpPutNonExist6","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "prepareToPut on prepareToUpdate does not work"


class StagerUpdRfiov2Case(StagerUpdCase):
    def setUp(self):
      global rfiover
      rfiover = "-v2"


class StagerUpdRfiov3Case(StagerUpdCase):
    def setUp(self):
      global rfiover
      rfiover = ""


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

        assert (buffOut2.find("SUBREQUEST_FAILED") != -1 and buffOut.find("CANBEMIGR") != -1) or (buffOut2.find("SUBREQUEST_READY") != -1 and buffOut.find("STAGED") != -1), "stager_rm doesn't work after put, rfcp and putdone"
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
        if notEnoughSvcClasses:
            return

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
        if notEnoughSvcClasses:
            return

        cmd=["STAGE_SVCCLASS="+ stagerSvcClass+" rfcp "+inputFile+" "+dirCastor+"fileClientRmDualSvc"+ticket,"stager_get -M "+dirCastor+"fileClientRmDualSvc"+ticket+" -S "+stagerDiskOnlySvcClass]
        UtilityForCastorTest.saveOnFile(localDir+"ClientRmDualSvcP1",cmd,myScen)

        fi=open(localDir+"ClientRmDualSvcP11","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.rfind("SUBREQUEST_READY") != -1, "stager_get doesn't work with svc class option -S"

        buffOut=UtilityForCastorTest.runOnShell(["stager_qry -M "+dirCastor+"fileClientRmDualSvc"+ticket+" -S "+stagerDiskOnlySvcClass],myScen)
        while buffOut[0].find("CANBEMIGR") == -1 and buffOut[0].find("STAGED") == -1:
          time.sleep(10)
          buffOut=UtilityForCastorTest.runOnShell(["stager_qry -M "+dirCastor+"fileClientRmDualSvc"+ticket+" -S "+stagerDiskOnlySvcClass],myScen)

        cmd=["stager_rm -M "+dirCastor+"fileClientRmDualSvc"+ticket+" -S "+stagerSvcClass,"stager_qry -M "+dirCastor+"fileClientRmDualSvc"+ticket+" -S "+stagerDiskOnlySvcClass,"stager_rm -M "+dirCastor+"fileClientRmDualSvc"+ticket+" -S "+stagerDiskOnlySvcClass,"stager_qry -M "+dirCastor+"fileClientRmDualSvc"+ticket+" -S "+stagerDiskOnlySvcClass]
        UtilityForCastorTest.saveOnFile(localDir+"ClientRmDualSvcP2",cmd,myScen)

        fi=open(localDir+"ClientRmDualSvcP2","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_FAILED 16 The file is not yet migrated") != -1, "stager_rm doesn't work with svc class option -S"

        fi=open(localDir+"ClientRmDualSvcP21","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1, "stager_qry doesn't work after stager_rm with svc class option -S"

        fi=open(localDir+"ClientRmDualSvcP22","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.rfind("SUBREQUEST_READY") != -1, "stager_rm doesn't work with svc class option -S"

        fi=open(localDir+"ClientRmDualSvcP23","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("INVALID") != -1 or buffOut.rfind("No such file or directory") != -1, "stager_qry doesn't work after stager_rm with svc class option -S"


class StagerSpecialQueryCase(unittest.TestCase):
    def queryS(self):
        cmd=["stager_qry -s"]
        UtilityForCastorTest.saveOnFile(localDir+"ClientQueryS",cmd,myScen)

        fi=open(localDir+"ClientQueryS","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("Error") == -1, "stager_qry -s doesn't work"


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
        fileBig=UtilityForCastorTest.getBigFile(100, ticket)  # 100 MB file

        cmd1=["stager_put -M "+dirCastor+"fileClientPutDoneLongFile"+ticket,"rfcp "+fileBig+" "+dirCastor+"fileClientPutDoneLongFile"+ticket,"stager_rm -M "+dirCastor+"fileClientPutDoneLongFile"+ticket,"stager_putdone -M "+dirCastor+"fileClientPutDoneLongFile"+ticket]

        UtilityForCastorTest.saveOnFile(localDir+"ClientPutDoneAndLongFile",cmd1,myScen)
        os.system("rm "+fileBig)
        fi=open(localDir+"ClientPutDoneAndLongFile3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_FAILED") != -1, "putDone doesn't work with big files"

    def srmSimulation(self):
        cmd=["stager_put -M "+dirCastor+"fileClientSrm"+ticket,"touch /tmp/fileSrm"+ticket,"rfcp /tmp/fileSrm"+ticket+" "+dirCastor+"fileClientSrm"+ticket,"stager_putdone -M "+dirCastor+"fileClientSrm"+ticket]
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
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "the putdone failed"

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
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1 , "the putdone failed"

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
casesQuery=("queryS",)
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

class StagerUpdRfiov2Suite(unittest.TestSuite):
    def __init__(self):
      unittest.TestSuite.__init__(self,map(StagerUpdRfiov2Case,casesUpd))

class StagerUpdRfiov3Suite(unittest.TestSuite):
    def __init__(self):
      unittest.TestSuite.__init__(self,map(StagerUpdRfiov3Case,casesUpd))

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
