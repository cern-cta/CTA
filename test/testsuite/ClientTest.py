import unittest
import UtilityForCastorTest
import os
import sys
import time
import threading

ticket= UtilityForCastorTest.getTicket() 
localDir=""
myCastor=UtilityForCastorTest.prepareCastorString()
dirCastor=myCastor+"tmpClientTest"+ticket+"/"

myScen=""


def MakeBigFile():
	size=0
	fout=open("/tmp/bigFile"+ticket,"wb")
	fin=open("/etc/group","r")
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

			f=open("/etc/castor/CASTORTESTCONFIG","r")
			configFileInfo=f.read()
			f.close

			index= configFileInfo.find("*** Client test specific parameters ***")
			configFileInfo=configFileInfo[index:]
			index=configFileInfo.find("***")
			index=index-1
			configFileInfo=configFileInfo[:index]

			global localDir
			localDir=(configFileInfo[configFileInfo.find("LOCAL_DIR"):]).split()[1]
		
			localDir=localDir+ticket+"/"
			os.system("mkdir "+localDir)
		        
			global stagerHost
		 	stagerHost=(configFileInfo[configFileInfo.find("STAGE_HOST"):]).split()[1]

			global stagerPort
		 	stagerPort=(configFileInfo[configFileInfo.find("STAGE_PORT"):]).split()[1]

			global stagerSvcClass
		 	stagerSvcClass=(configFileInfo[configFileInfo.find("STAGE_SVCCLASS"):]).split()[1]

		        global stagerVersion
		 	stagerVersion=(configFileInfo[configFileInfo.find("CASTOR_V2"):]).split()[1]

			global myScen
			myScen=UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerSvcClass,stagerVersion,[["STAGER_TRACE","3"]])
					
	        except IOError:
			assert 0==-1, "An error in the preparation of the main setting occurred ... test is not valid"
	
	def stagerUp(self):
		
		cmd=["stager_qry -M "+myCastor+"noFile"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"stagerUp",cmd,myScen)
		fi=open(localDir+"stagerUp","r")
		outBuff=fi.read()
		fi.close()
        	assert (outBuff.rfind("Unknown host")== -1), "the stager host is unknown" 
		assert (outBuff.rfind("Internal error")== -1), "the stager gives Internal errors" 
	
	def rfioFine(self):
		cmd=["nsmkdir "+dirCastor,"rfcp /etc/group "+dirCastor+"fileRfcp1"+ticket,"rfcp "+dirCastor+"fileRfcp1"+ticket+" "+dirCastor+"fileRfcp2"+ticket,"rfcp "+dirCastor+"fileRfcp1"+ticket+"  "+localDir+"fileRfcp1Copy","rfcp "+dirCastor+"fileRfcp2"+ticket+"  "+localDir+"fileRfcp2Copy","diff /etc/group "+localDir+"fileRfcp1Copy","diff /etc/group "+localDir+"fileRfcp2Copy"]
		
		UtilityForCastorTest.saveOnFile(localDir+"rfioFine",cmd,myScen)

	        assert os.stat(localDir+"fileRfcp1Copy")[6] != 0, "Rfcp doesn't work"
		assert os.stat(localDir+"fileRfcp2Copy")[6] != 0, "Rfcp doesn't work"
		assert os.stat(localDir+"rfioFine5")[6] == 0, "Rfcp doesn't work"
                assert os.stat(localDir+"rfioFine6")[6] == 0, "Rfcp doesn't work"

	
class StagerPutCase(unittest.TestCase):
	def basicPut(self):
		cmd=["stager_put -M "+dirCastor+"filePut1"+ticket,"stager_qry -M "+dirCastor+"filePut1"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"basicPut",cmd,myScen)
		fi=open(localDir+"basicPut","r")
	        buffOut=fi.read()
		fi.close()

        	assert buffOut.rfind("SUBREQUEST_READY") != -1, "stager_put doesn't work"
		fi=open(localDir+"basicPut1","r")
		buffOut=fi.read()
		fi.close()
        	assert buffOut.rfind("STAGEOUT") != -1, "stager_qry doesn't work after a simple put"
        	
		
	def putAndRfcp(self):
		cmd=["stager_put -M "+dirCastor+"filePut2"+ticket,"rfcp /etc/group "+dirCastor+"filePut2"+ticket,"stager_qry -M "+dirCastor+"filePut2"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"putAndRfcp",cmd,myScen)
		fi=open(localDir+"putAndRfcp2","r")
	        buffOut=fi.read()
		fi.close()
		
        	assert buffOut.find("STAGEOUT") != -1, "stager_qry doesn't work after stager_put and rfcp"
        	
		
	def putManyFiles(self):
		cmd=["stager_put -M "+dirCastor+"filePut3"+ticket+" -M "+dirCastor+"filePut4"+ticket,"rfcp /etc/group "+dirCastor+"filePut3"+ticket,"stager_qry -M "+dirCastor+"filePut3"+ticket,"rfcp /etc/group "+dirCastor+"filePut4"+ticket,"stager_qry -M "+dirCastor+"filePut4"+ticket,"stager_qry -M "+dirCastor+"filePut3"+ticket+" -M "+dirCastor+"filePut4"+ticket]

	        UtilityForCastorTest.saveOnFile(localDir+"putManyFiles",cmd,myScen)

		fi=open(localDir+"putManyFiles","r")
	        buffOut=fi.read()
		fi.close()
		
        	assert buffOut.count("SUBREQUEST_READY") == 2, "stager_put doesn't work with two files"
        	

		fi=open(localDir+"putManyFiles2","r")
		buffOut=fi.read()
		fi.close()
		
        	assert buffOut.find("STAGEOUT") != -1, "stager_qry doesn't work after stager_put with many files and rfcp"
		 
		fi=open(localDir+"putManyFiles4","r")
		buffOut=fi.read()
		fi.close()
		
        	assert buffOut.find("STAGEOUT") != -1, "stager_qry doesn't work after stager_put with many files and rfcp"
 
		fi=open(localDir+"putManyFiles5","r")
		buffOut=fi.read()
		fi.close()
		
		assert buffOut.count("STAGEOUT") == 2, "stager_qry doesn't work with two parameters"
		
		
	def putTag(self):
		cmd=["stager_put -M "+dirCastor+"filePut5"+ticket+" -U tagPut"+ticket,"stager_qry -M "+dirCastor+"filePut5"+ticket,"stager_qry -U tagPut"+ticket,"rfcp /etc/group "+dirCastor+"filePut5"+ticket,"stager_qry -M "+dirCastor+"filePut5"+ticket,"stager_qry -U tagPut"+ticket]
		
	        UtilityForCastorTest.saveOnFile(localDir+"putTag",cmd,myScen)
	
		fi=open(localDir+"putTag","r")
	        buffOut=fi.read()
		fi.close()
		
        	assert buffOut.find("SUBREQUEST_READY") != -1, "put doesn't work with tag"

		fi=open(localDir+"putTag1","r")
	        buffOut=fi.read()
		fi.close()
		
        	assert buffOut.find("STAGEOUT") != -1, "stager_qry doesn't work after a put with tag"
	
		fi=open(localDir+"putTag2","r")
	        buffOut=fi.read()
		fi.close()		
        	assert buffOut.find("STAGEOUT") != -1, "stager_qry doesn't work after a put with tag"

		fi=open(localDir+"putTag4","r")
	        buffOut=fi.read()
		fi.close()
		
		assert buffOut.find("STAGEOUT") != -1, "stager_qry doesn't work with tag"
		
		fi=open(localDir+"putTag5","r")
	        buffOut=fi.read()
		fi.close()
		
		assert buffOut.find("STAGEOUT") != -1, "stager_qry doesn't work with tag"
		
		
	def putSvcClass(self):

		cmd=["stager_put -M "+dirCastor+"filePutSvc1"+ticket+" -S mySVC","stager_put -M "+dirCastor+"filePutSvc2"+ticket+" -S default","stager_qry -M "+dirCastor+"filePutSvc2"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"putSvc",cmd,myScen)
		fi=open(localDir+"putSvc","r")
		buffOut=fi.read()
		fi.close()
        	assert buffOut.rfind("Opt SVCCLASS=mySVC") != -1, "stager_put doesn't work with svc class option -S"
        	
		
		fi=open(localDir+"putSvc1","r")
		outBuf=fi.read()
        	assert outBuf.find("Opt SVCCLASS=default") != -1, "stager_qry doesn't work with svc class option -S"
        	fi.close()
	        assert outBuf.find("SUBREQUEST_READY") != -1, "stager_qry doesn't work after a simple put"

		fi=open(localDir+"putSvc2","r")
		buffOut=fi.read()
		fi.close()
        	assert buffOut.rfind("STAGEOUT") != -1, "stager_qry doesn't work with svc class option -S"
	
		
	def putR(self):
		cmd=["stager_put -M "+dirCastor+"filePutR"+ticket+" -r","stager_qry -M "+dirCastor+"filePutR"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"putR",cmd,myScen)
		
		fi=open(localDir+"putR","r")
		buffOut=fi.read()
		fi.close()

        	assert buffOut.find("SUBREQUEST_READY") != -1, "stager_put doesn't work with svc class the option -r"
		line=buffOut.split("\n")
	        reqId=line[len(line)-3]

        	assert buffOut.count(reqId) == 4, "stager_put doesn't work with svc class the option -r"
		fi=open(localDir+"putR1","r")
		buffOut=fi.read()
		fi.close()
        	assert buffOut.rfind("STAGEOUT") != -1, "stager_qry doesn't work with the  option -r"
        	

class StagerPutDoneCase(unittest.TestCase):
	def basicPutDone(self):
		cmd=["stager_put -M "+dirCastor+"fileDone1"+ticket,"stager_putdone -M "+dirCastor+"fileDone1"+ticket,"stager_qry -M "+dirCastor+"fileDone1"+ticket]
		
	        UtilityForCastorTest.saveOnFile(localDir+"basicPutDone",cmd,myScen)
		
		fi=open(localDir+"basicPutDone1","r")
		buffOut=fi.read()
		fi.close()
        	assert buffOut.find("putDone without a put") != -1, "putDone doesn't work"

		fi=open(localDir+"basicPutDone2","r")
	        buffOut=fi.read()
		fi.close()
		
        	assert buffOut.find("STAGEOUT") != -1, "stager_qry doesn't work after a putDone"
        	
		
	def putDoneAndRfcp(self):
		cmd=["stager_put -M "+dirCastor+"fileDone2"+ticket,"rfcp /etc/group "+dirCastor+"fileDone2"+ticket,"stager_putdone -M "+dirCastor+"fileDone2"+ticket,"stager_qry -M "+dirCastor+"fileDone2"+ticket]

	        UtilityForCastorTest.saveOnFile(localDir+"putDoneAndRfcp",cmd,myScen)
       
		fi=open(localDir+"putDoneAndRfcp2","r")
	        buffOut=fi.read()
		fi.close()
        	assert buffOut.find("error=0") != -1, "putDone doesn't work with rfcp"
	
		fi=open(localDir+"putDoneAndRfcp3","r")
	        buffOut=fi.read()
		fi.close()
        	assert buffOut.find("CANBEMIGR") != -1, "stager_qry doesn't work after a putDone with rfcp"
		
	def putDoneManyFiles(self):
		cmd=["stager_put -M "+dirCastor+"fileDone3"+ticket+" -M "+dirCastor+"fileDone4"+ticket,"rfcp /etc/group "+dirCastor+"fileDone3"+ticket,"rfcp /etc/group "+dirCastor+"fileDone4"+ticket,"stager_putdone -M "+dirCastor+"fileDone3"+ticket+" -M "+dirCastor+"fileDone4"+ticket,"stager_qry -M "+dirCastor+"fileDone3"+ticket,"stager_qry -M "+dirCastor+"fileDone4"+ticket,"stager_qry -M "+dirCastor+"fileDone3"+ticket+" -M "+dirCastor+"fileDone4"+ticket]

		UtilityForCastorTest.saveOnFile(localDir+"putDoneManyFiles",cmd,myScen)

		fi=open(localDir+"putDoneManyFiles3","r")
	        buffOut=fi.read()
		fi.close()		
        	assert buffOut.find("error=0") != -1, "putDone doesn't work with many files"
        	
	
		fi=open(localDir+"putDoneManyFiles4","r")
	        buffOut=fi.read()
		fi.close()		
        	assert buffOut.find("CANBEMIGR") != -1, "stager_qry doesn't work after a putDone with many files"
        	
		
		fi=open(localDir+"putDoneManyFiles5","r")
	        buffOut=fi.read()
		fi.close()
		
        	assert buffOut.find("CANBEMIGR") != -1, "stager_qry doesn't work after a putDone with many files"

		
		fi=open(localDir+"putDoneManyFiles6","r")
	        buffOut=fi.read()
		fi.close()
		
        	assert buffOut.count("CANBEMIGR") == 2,  "stager_qry doesn't work after a putDone with many files"
        	
		
	def putDoneR(self):	
			
		cmd=["stager_put -M "+dirCastor+"fileDoneR"+ticket+" -r","rfcp /etc/group "+dirCastor+"fileDoneR"+ticket]
		cmd2=["stager_putdone -M "+dirCastor+"fileDoneR"+ticket+" -r ","stager_qry -M "+dirCastor+"fileDoneR"+ticket]
		
	        UtilityForCastorTest.saveOnFile(localDir+"putDoneR",cmd,myScen)
		fi=open(localDir+"putDoneR","r")
		buffOut=fi.read() 
		fi.close()
		line=buffOut.split("\n")
	        reqId=line[len(line)-3]
		
		cmd2=["stager_putdone -M "+dirCastor+"fileDoneR"+ticket+" -r "+reqId,"stager_qry -M "+dirCastor+"fileDoneR"+ticket]
		
	        UtilityForCastorTest.saveOnFile(localDir+"putDoneR2",cmd2,myScen)
		fi=open(localDir+"putDoneR2","r")
		buffOut=fi.read()
		fi.close()
        	assert buffOut.find("error=0") != -1, "stager_putDone doesn't work with -r"
        	assert buffOut.find("PutRequestId="+reqId)  != -1, "stager_put doesn't work with svc class the option -r"

		fi=open(localDir+"putDoneR21","r")
	        buffOut=fi.read()
		fi.close()
        	assert buffOut.find("CANBEMIGR") != -1, "stager_putdone doesn't work after a putDone -r"
        	
	
	def putDoneAndLongFile(self):
		fileBig=MakeBigFile()
	
		cmd1=["stager_put -M "+dirCastor+"fileDone5"+ticket,"rfcp "+fileBig+" "+dirCastor+"fileDone5"+ticket]
		cmd2=["stager_rm "+dirCastor+"fileDone5"+ticket,"stager_putdone -M "+dirCastor+"fileDone5"+ticket]

	        UtilityForCastorTest.runOnShell(cmd1,myScen)
                UtilityForCastorTest.saveOnFile(localDir+"putDoneAndLongFile",cmd2,myScen)
                os.system("rm "+fileBig) 
		fi=open(localDir+"putDoneAndLongFile1","r")
	        buffOut=fi.read()
		fi.close()
        	assert buffOut.find("SUBREQUEST_FAILED") != -1, "putDone doesn't work with big files"

	
class StagerGetCase(unittest.TestCase):
	def basicGet(self):
		cmd=["stager_put -M "+dirCastor+"fileGet1"+ticket,"stager_get -M "+dirCastor+"fileGet1"+ticket,"stager_qry -M "+dirCastor+"fileGet1"+ticket]

	        UtilityForCastorTest.saveOnFile(localDir+"basicGet",cmd,myScen)

		fi=open(localDir+"basicGet1","r")
	        buffOut=fi.read()
		fi.close()
            	assert buffOut.find("SUBREQUEST_READY") != -1, "stager_get doesn't work after put"
	
		fi=open(localDir+"basicGet2","r")
	        buffOut=fi.read()
		fi.close()		
        	assert buffOut.find("STAGEOUT") != -1, "stager_get doesn't work after put"
        	
		
	def getAndRfcp(self):
		cmd=["stager_put -M "+dirCastor+"fileGet2"+ticket,"rfcp /etc/group "+dirCastor+"fileGet2"+ticket,"stager_get -M "+dirCastor+"fileGet2"+ticket,"stager_putdone -M "+dirCastor+"fileGet2"+ticket,"stager_get -M "+dirCastor+"fileGet2"+ticket,"stager_qry -M "+dirCastor+"fileGet2"+ticket]

		UtilityForCastorTest.saveOnFile(localDir+"getAndRfcp",cmd,myScen)
	
		fi=open(localDir+"getAndRfcp2","r")
	        buffOut=fi.read()
		fi.close()		
		assert buffOut.find("SUBREQUEST_READY") != -1, "stager_get doesn't work after stager_put and rfcp"
	
		fi=open(localDir+"getAndRfcp3","r")
		buffOut=fi.read()
		fi.close()
            	assert buffOut.find("error=0") != -1, "stager_get doesn't work after rfcp and putdone"
		
	
		fi=open(localDir+"getAndRfcp4","r")
        	assert fi.read().find("SUBREQUEST_READY") != -1, "stager_get doesn't work after a put, rfcp and putDone"
        	fi.close()
		
	        fi=open(localDir+"getAndRfcp5","r")
		buffOut=fi.read()
		fi.close()
        	assert buffOut.find("CANBEMIGR") != -1, "stager_get doesn't work after a put, rfcp and putDone"
        	
		
	def getManyFiles(self):
		cmd=["stager_put -M "+dirCastor+"fileGet3"+ticket+" -M "+dirCastor+"fileGet4"+ticket,"rfcp /etc/group "+dirCastor+"fileGet3"+ticket,"rfcp /etc/group "+dirCastor+"fileGet4"+ticket,"stager_get -M "+dirCastor+"fileGet3"+ticket+" -M "+dirCastor+"fileGet4"+ticket,"stager_putdone -M "+dirCastor+"fileGet3"+ticket,"stager_putdone -M "+dirCastor+"fileGet4"+ticket,"stager_get -M "+dirCastor+"fileGet3"+ticket+" -M "+dirCastor+"fileGet4"+ticket,"stager_qry -M "+dirCastor+"fileGet3"+ticket+" -M "+dirCastor+"fileGet4"+ticket]
	        UtilityForCastorTest.saveOnFile(localDir+"getManyFiles",cmd,myScen)
	
		fi=open(localDir+"getManyFiles3","r")
		buffOut=fi.read()
		fi.close()
		assert buffOut.count("SUBREQUEST_READY") == 2, "stager_get doesn't work after stager_put with many files and rfcp"
		
		
		fi=open(localDir+"getManyFiles6","r")
	        buffOut=fi.read()
		fi.close()		
		assert buffOut.count("SUBREQUEST_READY") == 2, "stager_get doesn't work after put, rfcp and putdone with many files"
		
       
		fi=open(localDir+"getManyFiles7","r")
	        buffOut=fi.read()
		fi.close()		
        	assert buffOut.count("CANBEMIGR") == 2,  "stager_qry doesn't work after put, rfcp and putdone with many files"
        	
		
	
	def getTag(self):
	        cmd=["stager_put -M "+dirCastor+"fileGet5"+ticket+" -U tagGet"+ticket,"stager_get -M "+dirCastor+"fileGet5"+ticket+" -U tagGet"+ticket,"rfcp /etc/group "+dirCastor+"fileGet5"+ticket,"stager_get  -M "+dirCastor+"fileGet5"+ticket+" -U tagGet"+ticket,"stager_putdone -M "+dirCastor+"fileGet5"+ticket,"stager_get -M "+dirCastor+"fileGet5"+ticket+" -U tagGet"+ticket ]

	        UtilityForCastorTest.saveOnFile(localDir+"getTag",cmd,myScen)
	
		fi=open(localDir+"getTag1","r")
	        buffOut=fi.read()
		fi.close()		
		assert buffOut.find("SUBREQUEST_READY") != -1, "stager_get doesn't work with tag after put"
		
      
		fi=open(localDir+"getTag3","r")
	        buffOut=fi.read()
		fi.close()		
		assert buffOut.find("SUBREQUEST_READY") != -1, "stager_get doesn't work with tag after put and rfcp"
		

		fi=open(localDir+"getTag5","r")
	        buffOut=fi.read()
		fi.close()		
		assert buffOut.find("SUBREQUEST_READY") != -1, "stager_get doesn't work with tag after put, rfcp and putdone"
		
		
	def getSvcClass(self):
		cmd=["stager_put -M "+dirCastor+"fileGetSvc1"+ticket,"stager_get -M "+dirCastor+"fileGetSvc1"+ticket+" -S mySVC","stager_put -M "+dirCastor+"fileGetSvc2"+ticket+" -S default","stager_get -M "+dirCastor+"fileGetSvc2"+ticket,"stager_qry -M "+dirCastor+"fileGetSvc2"+ticket]

	        UtilityForCastorTest.saveOnFile(localDir+"getSvc",cmd,myScen)
                
		fi=open(localDir+"getSvc1","r")
	        buffOut=fi.read()
		fi.close()		
        	assert buffOut.rfind("Opt SVCCLASS=mySVC") != -1, "stager_get doesn't work with svc class option -S"

		fi=open(localDir+"getSvc3","r")
		buffOut=fi.read()
		fi.close()
            	assert buffOut.find("SUBREQUEST_READY") != -1, "stager_get doesn't work after put"
		assert buffOut.rfind("Opt SVCCLASS=default") != -1, "stager_get doesn't work with svc class option -S"
	
		fi=open(localDir+"getSvc4","r")
		buffOut=fi.read()
		fi.close()
        	assert buffOut.find("STAGEOUT") != -1, "stager_get doesn't work with svc class option -S"
        	
		
	def getR(self):		
		cmd=["stager_put -M "+dirCastor+"fileGetR1"+ticket,"stager_get -M "+dirCastor+"fileGetR1"+ticket+" -r","stager_qry -M "+dirCastor+"fileGetR1"+ticket]

	        UtilityForCastorTest.saveOnFile(localDir+"getR",cmd,myScen)

		fi=open(localDir+"getR1","r")
		buffOut=fi.read()
		fi.close()
		
            	assert buffOut.find("SUBREQUEST_READY") != -1, "stager_get doesn't work after put"

		line=buffOut.split("\n")

	        reqId=line[len(line)-2]
		reqId=reqId.strip("Stager request ID: ")

        	assert buffOut.count(reqId)  == 4, "stager_put doesn't work with svc class the option -r"
		
	
		fi=open(localDir+"getR2","r")
		buffOut=fi.read()
		fi.close()
        	assert buffOut.find("STAGEOUT") != -1, "stager_get doesn't work after put"
        	

class StagerRmCase(unittest.TestCase):
	def basicRm(self):
		cmd=["stager_put -M "+dirCastor+"fileRm1"+ticket,"stager_rm -M "+dirCastor+"fileRm1"+ticket]
         	UtilityForCastorTest.saveOnFile(localDir+"basicRm",cmd,myScen)
		     
		fi=open(localDir+"basicRm1","r")
		buffOut=fi.read()
		fi.close()
		assert buffOut.find("SUBREQUEST_READY") != -1, "stager_rm doesn't work after put"
		
		
	def rmAndRfcp(self):
		cmd=["stager_put -M "+dirCastor+"fileRm3"+ticket,"rfcp /etc/group "+dirCastor+"fileRm3"+ticket,"stager_rm -M "+dirCastor+"fileRm3"+ticket,]
		UtilityForCastorTest.saveOnFile(localDir+"rmAndRfcp",cmd,myScen)
	
		fi=open(localDir+"rmAndRfcp2","r")
		buffOut=fi.read()
		fi.close()
		assert buffOut.find("SUBREQUEST_READY") != -1, "stager_rm doesn't work after put and rfcp"

                cmd=["stager_put -M "+dirCastor+"fileRm4"+ticket,"rfcp /etc/group "+dirCastor+"fileRm4"+ticket,"stager_putdone -M "+dirCastor+"fileRm4"+ticket,"stager_rm -M "+dirCastor+"fileRm4"+ticket]

		UtilityForCastorTest.saveOnFile(localDir+"rmAndRfcpBis",cmd,myScen)

		fi=open(localDir+"rmAndRfcpBis3","r")
	        buffOut=fi.read()
		fi.close()
		
		assert buffOut.find("SUBREQUEST_FAILED") != -1, "stager_rm doesn't work after put, rfcp and putdone"
	        #weird .. not migrated yet ... 
		
	def rmManyFiles(self):
		cmd=["stager_put -M "+dirCastor+"fileRm5"+ticket+" -M"+dirCastor+"fileRm6"+ticket,"rfcp /etc/group "+dirCastor+"fileRm5"+ticket,"rfcp /etc/group "+dirCastor+"fileRm6"+ticket,"stager_rm -M "+dirCastor+"fileRm5"+ticket+" -M"+dirCastor+"fileRm6"+ticket]
		
	        UtilityForCastorTest.saveOnFile(localDir+"rmManyFiles",cmd,myScen)

		fi=open(localDir+"rmManyFiles3","r")
	        buffOut=fi.read()
		fi.close()
		assert buffOut.count("SUBREQUEST_READY") == 2, "stager_rm doesn't work with many files"
		
		
class StagerSpecialQueryCase(unittest.TestCase):
	def queryS(self):
	        cmd=["stager_qry -s"]
	        UtilityForCastorTest.saveOnFile(localDir+"queryS",cmd,myScen)
		
		fi=open(localDir+"queryS","r")
	        buffOut=fi.read()
		fi.close()		
		assert buffOut.find("Error") == -1, "stager_qry -s doesn't work"
		
		
	def queryE(self):
		cmd=["stager_put -M "+dirCastor+"fileQueryE"+ticket+"TEXT1","stager_put -M "+dirCastor+"fileQueryE"+ticket+"TEXT2","stager_qry -E "+dirCastor+"fileQueryE"+ticket+"*"]

		
	        UtilityForCastorTest.saveOnFile(localDir+"queryE",cmd,myScen)
		fi=open(localDir+"queryE2","r")
	        buffOut=fi.read()
		fi.close()		
		assert buffOut.find("STAGEOUT") != -1, "stager_qry -E doesn't work"
		
		
	
casesPreClient=("mainScenarium","stagerUp","rfioFine")
casesPut=("basicPut","putAndRfcp","putManyFiles","putTag","putSvcClass","putR")
casesPutDone=("basicPutDone","putDoneAndRfcp","putDoneManyFiles","putDoneR","putDoneAndLongFile")
casesGet=("basicGet","getAndRfcp","getManyFiles","getTag","getSvcClass","getR")
casesRm=("basicRm","rmAndRfcp","rmManyFiles")
casesQuery=("queryS","queryE")

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


 
