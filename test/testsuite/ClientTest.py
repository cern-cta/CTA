import unittest
import UtilityForCastorTest
import os
import sys
import time
import threading

# castor parameters

(stagerHost,stagerPort,stagerSvcClass,stagerVersion,stagerExtraSvcClass,stagerDiskOnlySvcClass,stagerForcedFileClass)= UtilityForCastorTest.getCastorParameters(sys.argv[1:])

# parameters

ticket= UtilityForCastorTest.getTicket()
myScen=""

# files and directories

inputFile="" 
localDir=""
myCastor=UtilityForCastorTest.prepareCastorString()
dirCastor=myCastor+"tmpClientTest"+ticket+"/"

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


class PreRequisitesCase(unittest.TestCase):
    def mainScenarium(self):
        assert (UtilityForCastorTest.checkUser() != -1), "you don't have a valid castor directory"

        try:

          f=open("./CASTORTESTCONFIG","r")
          configFileInfo=f.read()
          f.close
  
          index= configFileInfo.find("*** Client test specific parameters ***")
          configFileInfo=configFileInfo[index:]
          index=configFileInfo.find("***")
          if index != -1:
              index=index-1
              configFileInfo=configFileInfo[:index]
  
          global localDir
          localDir=(configFileInfo[configFileInfo.find("LOG_DIR"):]).split()[1]
          localDir=localDir+ticket+"/"
          os.system("mkdir "+localDir)
  
          global inputFile
          inputFile=(configFileInfo[configFileInfo.find("INPUT_FILE"):]).split()[1]
          
          global myScen
          myScen=UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerSvcClass,stagerVersion,None,[["STAGER_TRACE","3"]])
          UtilityForCastorTest.runOnShell(["nsmkdir "+dirCastor],myScen)
                
        except IOError:
          assert 0==-1, "An error in the preparation of the main setting occurred ... test is not valid"
    
    def stagerRfioFine(self):
        [ret,out]=UtilityForCastorTest.testCastorBasicFunctionality(inputFile,dirCastor+"ClientPreReq"+ticket,localDir,myScen)
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
        assert buffOut.rfind("Opt SVCCLASS="+stagerSvcClass) != -1, "stager_put doesn't work with svc class option -S"
        
        
        fi=open(localDir+"ClientPutSvc1","r")
        outBuf=fi.read()
        fi.close()
        assert outBuf.find("Opt SVCCLASS="+stagerExtraSvcClass) != -1, "stager_qry doesn't work with svc class option -S"
        assert outBuf.find("No such file") != -1, "stager_qry doesn't work after a simple put"
        
        fi=open(localDir+"ClientPutSvc2","r")
        buffOut=fi.read()
        fi.close()
        assert outBuf.find("Opt SVCCLASS="+stagerExtraSvcClass) != -1, "stager_qry doesn't work with svc class option -S"


        fi=open(localDir+"ClientPutSvc3","r")
        buffOut=fi.read()
        fi.close()
        assert outBuf.find("Opt SVCCLASS="+stagerExtraSvcClass) != -1, "stager_qry doesn't work with svc class option -S"
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

        assert buffOut.count(reqId) == 4, "stager_put doesn't work with svc class the option -r"
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
        assert buffOut.find("error=0") != -1, "putDone doesn't work with rfcp"
    
        fi=open(localDir+"ClientPutDoneAndRfcp3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("CANBEMIGR") != -1, "stager_qry doesn't work after a putDone with rfcp"
        
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
        assert buffOut.find("error=0") != -1, "putDone doesn't work with many files"
        
    
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
        assert buffOut.find("error=0") != -1, "stager_putDone doesn't work with -r"
        assert buffOut.find("PutRequestId="+reqId)  != -1, "stager_put doesn't work with svc class the option -r"

        fi=open(localDir+"ClientPutDoneRBis1","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("CANBEMIGR") != -1, "stager_putdone doesn't work after a putDone -r"
    
class StagerGetCase(unittest.TestCase):
        
    def getAndRfcp(self):
        cmd=["stager_put -M "+dirCastor+"fileClientGetRfcp"+ticket,"stager_get -M "+dirCastor+"fileClientGetRfcp"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientGetRfcp"+ticket,"stager_get -M "+dirCastor+"fileClientGetRfcp"+ticket,"stager_putdone -M "+dirCastor+"fileClientGetRfcp"+ticket,"stager_get -M "+dirCastor+"fileClientGetRfcp"+ticket,"stager_qry -M "+dirCastor+"fileClientGetRfcp"+ticket]

        UtilityForCastorTest.saveOnFile(localDir+"ClientGetAndRfcp",cmd,myScen)
    
        fi=open(localDir+"ClientGetAndRfcp2","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_FAILED") != -1, "stager_get doesn't work after prepare to put"

        fi=open(localDir+"ClientGetAndRfcp4","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_get doesn't work after stager_put and rfcp"
    
        fi=open(localDir+"ClientGetAndRfcp5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("error=0") != -1, "stager_putdone doesn't work after put, rfcp and get"
        
    
        fi=open(localDir+"ClientGetAndRfcp6","r")
        assert fi.read().find("SUBREQUEST_READY") != -1, "stager_get doesn't work after a put, rfcp and putDone"
        fi.close()
        
        fi=open(localDir+"ClientGetAndRfcp7","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("CANBEMIGR") != -1, "stager_qry doesn't work after a put, rfcp and putDone"
        
        
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
        cmd=["stager_put -M "+dirCastor+"fileClientGetSvc"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientGetSvc"+ticket,"stager_qry -M "+dirCastor+"fileClientGetSvc"+ticket+" -S"+stagerExtraSvcClass,"stager_get -M "+dirCastor+"fileClientGetSvc"+ticket+" -S "+stagerExtraSvcClass,"stager_qry -M "+dirCastor+"fileClientGetSvc"+ticket+" -S"+stagerExtraSvcClass,"stager_put -M "+dirCastor+"fileClientGetSvcBis"+ticket+" -S "+stagerExtraSvcClass,"rfcp "+inputFile+" "+dirCastor+"fileClientGetSvcBis"+ticket,"stager_qry -M "+dirCastor+"fileClientGetSvcBis"+ticket+" -S "+stagerExtraSvcClass,"stager_get -M "+dirCastor+"fileClientGetSvcBis"+ticket+" -S "+stagerExtraSvcClass,"stager_qry -M "+dirCastor+"fileClientGetSvcBis"+ticket+" -S "+stagerSvcClass]

        UtilityForCastorTest.saveOnFile(localDir+"ClientGetSvc",cmd,myScen)
        
        fi=open(localDir+"ClientGetSvc2","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.rfind("No such file") != -1, "stager_get doesn't work with svc class option -S"
        assert buffOut.rfind("Opt SVCCLASS="+stagerExtraSvcClass) != -1, "stager_get doesn't work with svc class option -S"
        
        fi=open(localDir+"ClientGetSvc3","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.rfind("SUBREQUEST_READY") != -1, "stager_get doesn't work with svc class option -S"
        assert buffOut.rfind("Opt SVCCLASS="+stagerExtraSvcClass) != -1, "stager_get doesn't work with svc class option -S"

        fi=open(localDir+"ClientGetSvc4","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1, "stager_get doesn't work after put"
        assert buffOut.rfind("Opt SVCCLASS="+stagerExtraSvcClass) != -1, "stager_get doesn't work with svc class option -S"
    
        fi=open(localDir+"ClientGetSvc8","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("STAGEOUT") != -1, "stager_get doesn't work with svc class option -S"
        assert buffOut.rfind("Opt SVCCLASS="+stagerExtraSvcClass) != -1, "stager_get doesn't work with svc class option -S"
        
        fi=open(localDir+"ClientGetSvc9","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("No such file") != -1, "stager_get doesn't work with svc class option -S"
        assert buffOut.rfind("Opt SVCCLASS="+stagerSvcClass) != -1, "stager_get doesn't work with svc class option -S"
        
    def getR(self):        
        cmd=["stager_put -M "+dirCastor+"fileClientGetR"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientGetR"+ticket,"stager_get -M "+dirCastor+"fileClientGetR"+ticket+" -r","stager_qry -M "+dirCastor+"fileClientGetR"+ticket]

        UtilityForCastorTest.saveOnFile(localDir+"ClientGetR",cmd,myScen)

        fi=open(localDir+"ClientGetR2","r")
        buffOut=fi.read()
        fi.close()
        
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_get doesn't work after put"

        line=buffOut.split("\n")

        reqId=line[len(line)-2]
        reqId=reqId.strip("Stager request ID: ")

        assert buffOut.count(reqId)  == 4, "stager_put doesn't work with svc class the option -r"
        
    
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
        assert buffOut.rfind("Opt SVCCLASS="+stagerExtraSvcClass) != -1, "stager_get doesn't work with svc class option -S"

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

        cmd=["stager_put -M "+dirCastor+"fileClientRmRfcpBis"+ticket,"rfcp "+inputFile+" "+dirCastor+"fileClientRmRfcpBis"+ticket,"stager_putdone -M "+dirCastor+"fileClientRmRfcpBis"+ticket,"stager_rm -M "+dirCastor+"fileClientRmRfcpBis"+ticket]

        UtilityForCastorTest.saveOnFile(localDir+"ClientRmAndRfcpBis",cmd,myScen)

        fi=open(localDir+"ClientRmAndRfcpBis3","r")
        buffOut=fi.read()
        fi.close()
        
        assert buffOut.find("SUBREQUEST_FAILED") != -1, "stager_rm doesn't work after put, rfcp and putdone"
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
        
        cmd=["stager_put -M "+dirCastor+"fileClientRmSvc"+ticket+" -S"+stagerSvcClass,"stager_rm -M "+dirCastor+"fileClientRmSvc"+ticket+" -S "+stagerExtraSvcClass,"stager_qry -M "+dirCastor+"fileClientRmSvc"+ticket,"stager_put -M "+dirCastor+"fileClientRmSvcBis"+ticket+" -S "+stagerExtraSvcClass,"stager_rm -M "+dirCastor+"fileClientRmSvcBis"+ticket+" -S "+stagerExtraSvcClass,"stager_qry -M "+dirCastor+"fileClientGetSvcBis"+ticket+" -S "+stagerExtraSvcClass]
        
        UtilityForCastorTest.saveOnFile(localDir+"ClientRmSvc",cmd,myScen)
        
        fi=open(localDir+"ClientRmSvc1","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.rfind("Opt SVCCLASS="+stagerExtraSvcClass) != -1, "stager_rm doesn't work with svc class option -S"
        
        fi=open(localDir+"ClientRmSvc2","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.rfind("STAGEOUT") != -1, "stager_rm doesn't work with svc class option -S"

        fi=open(localDir+"ClientRmSvc3","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_rm doesn't work after put"
        assert buffOut.rfind("Opt SVCCLASS="+stagerExtraSvcClass) != -1, "stager_rm doesn't work with svc class option -S"
        
        fi=open(localDir+"ClientRmSvc4","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("SUBREQUEST_READY") != -1, "stager_rm doesn't work after put"
        assert buffOut.rfind("Opt SVCCLASS="+stagerExtraSvcClass) != -1, "stager_rm doesn't work with svc class option -S"
    
        fi=open(localDir+"ClientRmSvc5","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.find("No such file") != -1, "stager_rm doesn't work with svc class option -S"

    def rmDualSvcClass(self):
        
        cmd=["rfcp "+inputFile+" "+dirCastor+"fileClientRmDualSvc"+ticket,"stager_get -M "+dirCastor+"fileClientRmDualSvc"+ticket+" -S "+stagerExtraSvcClass,"stager_rm -M "+dirCastor+"fileClientRmDualSvc"+ticket+" -S "+stagerSvcClass,"stager_qry -M "+dirCastor+"fileClientRmDualSvc"+ticket+" -S "+stagerExtraSvcClass,"stager_rm -M "+dirCastor+"fileClientRmDualSvc"+ticket+" -S "+stagerExtraSvcClass,"stager_qry -M "+dirCastor+"fileClientGetDualSvc"+ticket+" -S '*'"]
        
        UtilityForCastorTest.saveOnFile(localDir+"ClientRmDualSvc",cmd,myScen)
        
        fi=open(localDir+"ClientRmDualSvc2","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.rfind("SUBREQUEST_READY") != -1, "stager_get does not with svc class option -S"
        
        fi=open(localDir+"ClientRmDualSvc3","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.rfind("SUBREQUEST_READY") != -1, "stager_rm doesn't work with svc class option -S"

        fi=open(localDir+"ClientRmDualSvc4","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.find("STAGED") != -1 or buffOut.find("CANBEMIGR") != -1, "stager_qry doesn't work after stager_rm with svc class option -S"

        fi=open(localDir+"ClientRmDualSvc5","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.rfind("SUBREQUEST_READY") != -1, "stager_rm doesn't work with svc class option -S"

        fi=open(localDir+"ClientRmDualSvc6","r")
        buffOut=fi.read()
        fi.close()        
        assert buffOut.rfind("not in stager") != -1, "stager_qry doesn't work after stager_rm with svc class option -S"

        
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

        myScenE=UtilityForCastorTest.createScenarium(stagerHost,stagerPort,-1,stagerVersion,[["STAGER_TRACE","3"]])
        
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
        assert buffOut.find("error=0") != -1, "srm simulation failed"
      
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
