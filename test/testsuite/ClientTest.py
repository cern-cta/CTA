import unittest
import UtilityForCastorTest
import os
import sys
import time

ticket= UtilityForCastorTest.getTicket() 
localDir="./tmpClientTest"+ticket+"/"
myCastor=UtilityForCastorTest.prepareCastorString()
dirCastor=myCastor+"TestClient"+ticket+"/"

class PreRequisitesCase(unittest.TestCase):
	def userValid(self):
		assert (UtilityForCastorTest.checkUser != -1), "you don't have a valid castor directory"
		
	def stagerUp(self):
		UtilityForCastorTest.runOnShell("mkdir "+localDir)
		time.sleep(2)
		UtilityForCastorTest.saveOnFile(localDir+"stagerUp1","stager_qry -M "+myCastor+"noFile"+ticket)
		fi=open(localDir+"stagerUp1","r")
		outBuff=fi.read()
		fi.close()
        	assert (outBuff.rfind("Unknown host")== -1), "the stager host is unknown" 
		assert (outBuff.rfind("Internal error")== -1), "the stager gives Internal errors" 
	
	def rfioFine(self):
		UtilityForCastorTest.runOnShell("rfmkdir "+dirCastor)
		UtilityForCastorTest.saveOnFile(localDir+"rfioFine1","rfcp /etc/group "+dirCastor+"fileRfcp1"+ticket)
		UtilityForCastorTest.saveOnFile(localDir+"rfioFine2","rfcp "+dirCastor+"fileRfcp1"+ticket+" "+dirCastor+"fileRfcp2"+ticket)
		UtilityForCastorTest.saveOnFile(localDir+"rfioFine3","rfcp "+dirCastor+"fileRfcp1"+ticket+"  "+localDir+"fileRfcp1Copy")
		UtilityForCastorTest.saveOnFile(localDir+"rfioFine4","rfcp "+dirCastor+"fileRfcp2"+ticket+"  "+localDir+"fileRfcp2Copy")
		UtilityForCastorTest.saveOnFile(localDir+"rfioFineDiff1","diff /etc/group "+localDir+"fileRfcp1Copy")
		assert os.stat(localDir+"rfioFineDiff1")[6] == 0, "Rfcp doesn't work"
		UtilityForCastorTest.saveOnFile(localDir+"rfioFineDiff2","diff /etc/group "+localDir+"fileRfcp2Copy")
        	assert os.stat(localDir+"rfioFineDiff2")[6] == 0, "Rfcp doesn't work"
		
class StagerPutCase(unittest.TestCase):
	def basicPut(self):
		UtilityForCastorTest.saveOnFile(localDir+"basicPut1","stager_put -M "+dirCastor+"filePut1"+ticket)
		fi=open(localDir+"basicPut1","r")
        	assert fi.read().rfind("SUBREQUEST_READY") != -1, "stager_put doesn't work"
        	fi.close()
		
		UtilityForCastorTest.saveOnFile(localDir+"basicPut2","stager_qry -M "+dirCastor+"filePut1"+ticket)
		fi=open(localDir+"basicPut2","r")
        	assert fi.read().rfind("STAGEOUT") != -1, "stager_qry doesn't work after a simple put"
        	fi.close()
		
	def putAndRfcp(self):
		UtilityForCastorTest.runOnShell("stager_put -M "+dirCastor+"filePut2"+ticket)
		UtilityForCastorTest.runOnShell("rfcp /etc/group "+dirCastor+"filePut2"+ticket)
		UtilityForCastorTest.saveOnFile(localDir+"putAndRfcp1","stager_qry -M "+dirCastor+"filePut2"+ticket) 
		fi=open(localDir+"putAndRfcp1","r")
        	assert fi.read().find("STAGEOUT") != -1, "stager_qry doesn't work after stager_put and rfcp"
        	fi.close()
		
	def putManyFiles(self):
		UtilityForCastorTest.saveOnFile(localDir+"putManyFiles1","stager_put -M "+dirCastor+"filePut3"+ticket+" -M "+dirCastor+"filePut4"+ticket)
		fi=open(localDir+"putManyFiles1","r")
        	assert fi.read().count("SUBREQUEST_READY") == 2, "stager_put doesn't work with two files"
        	fi.close()
		
		UtilityForCastorTest.runOnShell("rfcp /etc/group "+dirCastor+"filePut3"+ticket)
		UtilityForCastorTest.saveOnFile(localDir+"putManyFiles2","stager_qry -M "+dirCastor+"filePut3"+ticket) 
		fi=open(localDir+"putManyFiles2","r")
        	assert fi.read().find("STAGEOUT") != -1, "stager_qry doesn't work after stager_put with many files and rfcp"
        	fi.close()
		
		UtilityForCastorTest.runOnShell("rfcp /etc/group "+dirCastor+"filePut4"+ticket)
		UtilityForCastorTest.saveOnFile(localDir+"putManyFiles3","stager_qry -M "+dirCastor+"filePut4"+ticket) 
		fi=open(localDir+"putManyFiles3","r")
        	assert fi.read().find("STAGEOUT") != -1, "stager_qry doesn't work after stager_put with many files and rfcp"
        	fi.close()
		
		UtilityForCastorTest.saveOnFile(localDir+"putManyFiles4","stager_qry -M "+dirCastor+"filePut3"+ticket+" -M "+dirCastor+"filePut4"+ticket) 
		fi=open(localDir+"putManyFiles4","r")
		assert fi.read().count("STAGEOUT") == 2, "stager_qry doesn't work with two parameters"
		fi.close()
		
	def putTag(self):
		UtilityForCastorTest.saveOnFile(localDir+"putTag1","stager_put -M "+dirCastor+"filePut5"+ticket+" -U tagPut"+ticket)
		fi=open(localDir+"putTag1","r")
        	assert fi.read().find("SUBREQUEST_READY") != -1, "put doesn't work with tag"
        	fi.close()
		
		UtilityForCastorTest.saveOnFile(localDir+"putTag2","stager_qry -M "+dirCastor+"filePut5"+ticket)
		fi=open(localDir+"putTag2","r")
        	assert fi.read().find("STAGEOUT") != -1, "stager_qry doesn't work after a put with tag"
        	fi.close()
		
		UtilityForCastorTest.saveOnFile(localDir+"putTag3","stager_qry -U tagPut"+ticket)
		fi=open(localDir+"putTag3","r")
        	assert fi.read().find("STAGEOUT") != -1, "stager_qry doesn't work after a put with tag"
        	fi.close()
		UtilityForCastorTest.runOnShell("rfcp /etc/group "+dirCastor+"filePut5"+ticket)
		
		UtilityForCastorTest.saveOnFile(localDir+"putTag4","stager_qry -M "+dirCastor+"filePut5"+ticket)
		fi=open(localDir+"putTag4","r")
		assert fi.read().find("STAGEOUT") != -1, "stager_qry doesn't work with tag"
		fi.close()
		 
		UtilityForCastorTest.saveOnFile(localDir+"putTag5","stager_qry -U tagPut"+ticket)
		fi=open(localDir+"putTag5","r")
		assert fi.read().find("STAGEOUT") != -1, "stager_qry doesn't work with tag"
		fi.close()
		
	def putSvcClass(self):
		pass
	def putR(self):
		pass

class StagerPutDoneCase(unittest.TestCase):
	def basicPutDone(self):
		UtilityForCastorTest.runOnShell("stager_put -M "+dirCastor+"fileDone1"+ticket)
		UtilityForCastorTest.saveOnFile(localDir+"basicPutDone1","stager_putdone -M "+dirCastor+"fileDone1"+ticket)
		fi=open(localDir+"basicPutDone1","r")
        	assert fi.read().find("error=1") != -1, "putDone doesn't work"
        	fi.close()
		UtilityForCastorTest.saveOnFile(localDir+"basicPutDone2","stager_qry -M "+dirCastor+"fileDone1"+ticket)
		fi=open(localDir+"basicPutDone2","r")
        	assert fi.read().find("STAGEOUT") != -1, "stager_qry doesn't work after a putDone"
        	fi.close()
		
	def putDoneAndRfcp(self):
		UtilityForCastorTest.runOnShell("stager_put -M "+dirCastor+"fileDone2"+ticket)
		UtilityForCastorTest.runOnShell("rfcp /etc/group "+dirCastor+"fileDone2"+ticket)
		UtilityForCastorTest.saveOnFile(localDir+"putDoneAndRfcp1","stager_putdone -M "+dirCastor+"fileDone2"+ticket)
		fi=open(localDir+"putDoneAndRfcp1","r")
        	assert fi.read().find("error=0") != -1, "putDone doesn't work with rfcp"
        	fi.close()
		UtilityForCastorTest.saveOnFile(localDir+"putDoneAndRfcp2","stager_qry -M "+dirCastor+"fileDone2"+ticket)
		fi=open(localDir+"putDoneAndRfcp2","r")
        	assert fi.read().find("CANBEMIGR") != -1, "stager_qry doesn't work after a putDone with rfcp"
        	fi.close()
		
	def putDoneManyFiles(self):
		UtilityForCastorTest.runOnShell("stager_put -M "+dirCastor+"fileDone3"+ticket+" -M "+dirCastor+"fileDone4"+ticket)
		UtilityForCastorTest.runOnShell("rfcp /etc/group "+dirCastor+"fileDone3"+ticket)
		UtilityForCastorTest.runOnShell("rfcp /etc/group "+dirCastor+"fileDone4"+ticket)
		UtilityForCastorTest.saveOnFile(localDir+"putDoneManyFiles1","stager_putdone -M "+dirCastor+"fileDone3"+ticket+" -M "+dirCastor+"fileDone4"+ticket)
		fi=open(localDir+"putDoneManyFiles1","r")
        	assert fi.read().find("error=0") != -1, "putDone doesn't work with many files"
        	fi.close()
		
		UtilityForCastorTest.saveOnFile(localDir+"putDoneManyFiles2","stager_qry -M "+dirCastor+"fileDone3"+ticket)
		fi=open(localDir+"putDoneManyFiles2","r")
        	assert fi.read().find("CANBEMIGR") != -1, "stager_qry doesn't work after a putDone with many files"
        	fi.close()
		
		UtilityForCastorTest.saveOnFile(localDir+"putDoneManyFiles3","stager_qry -M "+dirCastor+"fileDone4"+ticket)
		fi=open(localDir+"putDoneManyFiles3","r")
        	assert fi.read().find("CANBEMIGR") != -1, "stager_qry doesn't work after a putDone with many files"
        	fi.close()
		
		UtilityForCastorTest.saveOnFile(localDir+"putDoneManyFiles4","stager_qry -M "+dirCastor+"fileDone3"+ticket+" -M "+dirCastor+"fileDone4"+ticket)
		fi=open(localDir+"putDoneManyFiles4","r")
        	assert fi.read().count("CANBEMIGR") == 2,  "stager_qry doesn't work after a putDone with many files"
        	fi.close()	
		
	def putDoneR(self):	
		pass 
	
class StagerGetCase(unittest.TestCase):
	def basicGet(self):
		UtilityForCastorTest.runOnShell("stager_put -M "+dirCastor+"fileGet1"+ticket)
		
		UtilityForCastorTest.saveOnFile(localDir+"basicGet1","stager_get -M "+dirCastor+"fileGet1"+ticket)
		fi=open(localDir+"basicGet1","r")
            	assert fi.read().find("SUBREQUEST_READY") != -1, "stager_get doesn't work after put"
            	fi.close()
		
		UtilityForCastorTest.saveOnFile(localDir+"basicGet2","stager_qry -M "+dirCastor+"fileGet1"+ticket)
		fi=open(localDir+"basicGet2","r")
        	assert fi.read().find("STAGEOUT") != -1, "stager_get doesn't work after put"
        	fi.close()
		
	def getAndRfcp(self):
		UtilityForCastorTest.runOnShell("stager_put -M "+dirCastor+"fileGet2"+ticket)
		UtilityForCastorTest.runOnShell("rfcp /etc/group "+dirCastor+"fileGet2"+ticket)
		
		UtilityForCastorTest.saveOnFile(localDir+"getAndRfcp1","stager_get -M "+dirCastor+"fileGet2"+ticket) 
		fi=open(localDir+"getAndRfcp1","r")
		assert fi.read().find("SUBREQUEST_READY") != -1, "stager_get doesn't work after stager_put and rfcp"
		fi.close()
		UtilityForCastorTest.runOnShell("stager_putdone -M "+dirCastor+"fileGet2")
		
		UtilityForCastorTest.saveOnFile(localDir+"getAndRfcp2","stager_get -M "+dirCastor+"fileGet2"+ticket)
		fi=open(localDir+"getAndRfcp2","r")
            	assert fi.read().find("SUBREQUEST_READY") != -1, "stager_get doesn't work after rfcp and putdone"
            	fi.close()
		
		UtilityForCastorTest.saveOnFile(localDir+"getAndRfcp3","stager_qry -M "+dirCastor+"fileGet2"+ticket)
		fi=open(localDir+"getAndRfcp3","r")
        	assert fi.read().find("CANBEMIGR") != -1, "stager_qry doesn't work after a put, rfcp, putDone and get"
        	fi.close()
		
	def getManyFiles(self):
		UtilityForCastorTest.runOnShell("stager_put -M "+dirCastor+"fileGet3"+ticket+" -M "+dirCastor+"fileGet4"+ticket)
		UtilityForCastorTest.runOnShell("rfcp /etc/group "+dirCastor+"fileGet3"+ticket)
		UtilityForCastorTest.runOnShell("rfcp /etc/group "+dirCastor+"fileGet4"+ticket)
		
		UtilityForCastorTest.saveOnFile(localDir+"getManyFiles1","stager_get -M "+dirCastor+"fileGet3"+ticket+" -M "+dirCastor+"fileGet4"+ticket) 
		fi=open(localDir+"getManyFiles1","r")
		assert fi.read().count("SUBREQUEST_READY") == 2, "stager_get doesn't work after stager_put with many files and rfcp"
		fi.close()
		UtilityForCastorTest.runOnShell("stager_putdone -M "+dirCastor+"fileGet3"+ticket)
		UtilityForCastorTest.runOnShell("stager_putdone -M "+dirCastor+"fileGet4"+ticket)
		UtilityForCastorTest.saveOnFile(localDir+"getManyFiles2","stager_get -M "+dirCastor+"fileGet3"+ticket+" -M "+dirCastor+"fileGet4"+ticket) 
		fi=open(localDir+"getManyFiles2","r")
		assert fi.read().count("SUBREQUEST_READY") == 2, "stager_get doesn't work after put, rfcp and putdone with many files"
		fi.close()
		UtilityForCastorTest.saveOnFile(localDir+"getManyFiles3","stager_qry -M "+dirCastor+"fileGet3"+ticket+" -M "+dirCastor+"fileGet4"+ticket)
		fi=open(localDir+"getManyFiles3","r")
        	assert fi.read().count("CANBEMIGR") == 2,  "stager_qry doesn't work after put, rfcp and putdone with many files"
        	fi.close()
		
	
	def getTag(self):
		pass
	
	#	UtilityForCastorTest.runOnShell("stager_put -M "+dirCastor+"fileGet5"+ticket+" -U tagGet"+ticket)
	#	UtilityForCastorTest.saveOnFile("getTag1","stager_get -U tagGet"+ticket)
	#	fi=open(localDir+"getTag1","r")
	#	assert fi.read().find("SUBREQUEST_READY") != -1, "stager_get doesn't work with tag after put"
	#	fi.close()
	#	UtilityForCastorTest.runOnShell("rfcp /etc/group "+dirCastor+"fileGet5"+ticket)
	#	UtilityForCastorTest.saveOnFile("getTag2","stager_get -U tagGet"+ticket)
	#	fi=open(localDir+"getTag2","r")
	#	assert fi.read().find("SUBREQUEST_READY") != -1, "stager_get doesn't work with tag after put and rfcp"
	#	fi.close()
	#	UtilityForCastorTest.runOnShell("stager_putdone -M "+dirCastor+"fileGet5"+ticket)
	#	UtilityForCastorTest.saveOnFile("getTag3","stager_get -U tagGet"+ticket)
	#	fi=open(localDir+"getTag3","r")
	#	assert fi.read().find("SUBREQUEST_READY") != -1, "stager_get doesn't work with tag after put, rfcp and putdone"
	#	fi.close()
		
	def getSvcClass(self):
		pass
	def getR(self):
		pass

class StagerRmCase(unittest.TestCase):
	def basicRm(self):
		UtilityForCastorTest.runOnShell("stager_put -M "+dirCastor+"fileRm1"+ticket)
		UtilityForCastorTest.saveOnFile(localDir+"basicRm1","stager_rm -M "+dirCastor+"fileRm1"+ticket)
		fi=open(localDir+"basicRm1","r")
		assert fi.read().find("SUBREQUEST_READY") != -1, "stager_rm doesn't work after put"
		fi.close()
		
	def rmAndRfcp(self):
		UtilityForCastorTest.runOnShell("stager_put -M "+dirCastor+"fileRm3"+ticket)
		UtilityForCastorTest.runOnShell("rfcp /etc/group "+dirCastor+"fileRm3"+ticket)
		UtilityForCastorTest.saveOnFile(localDir+"rmAndRfcp1","stager_rm -M "+dirCastor+"fileRm3"+ticket)
		fi=open(localDir+"rmAndRfcp1","r")
		assert fi.read().find("SUBREQUEST_READY") != -1, "stager_rm doesn't work after put and rfcp"
		fi.close()
		
		UtilityForCastorTest.runOnShell("stager_put -M "+dirCastor+"fileRm4"+ticket)
		UtilityForCastorTest.runOnShell("rfcp /etc/group "+dirCastor+"fileRm4"+ticket)
		UtilityForCastorTest.runOnShell("stager_putdone -M "+dirCastor+"fileRm4"+ticket)
		UtilityForCastorTest.saveOnFile(localDir+"rmAndRfcp2","stager_rm -M "+dirCastor+"fileRm4"+ticket)
		fi=open(localDir+"rmAndRfcp2","r")
		assert fi.read().find("SUBREQUEST_FAILED") != -1, "stager_rm doesn't work after put, rfcp and putdone"
		fi.close() #weird .. not migrated yet ... 
		
	def rmManyFiles(self):
		UtilityForCastorTest.runOnShell("stager_put -M "+dirCastor+"fileRm5"+ticket+" -M"+dirCastor+"fileRm6"+ticket)
	        UtilityForCastorTest.runOnShell("rfcp /etc/group "+dirCastor+"fileRm5"+ticket)
		UtilityForCastorTest.runOnShell("rfcp /etc/group "+dirCastor+"fileRm6"+ticket)
		UtilityForCastorTest.saveOnFile(localDir+"rmManyFiles1","stager_rm -M "+dirCastor+"fileRm5"+ticket+" -M"+dirCastor+"fileRm6"+ticket)
		fi=open(localDir+"rmManyFiles1","r")
		assert fi.read().count("SUBREQUEST_READY") == 2, "stager_rm doesn't work with many files"
		fi.close()
		
class StagerSpecialQueryCase(unittest.TestCase):
	def queryS(self):
		pass
	def queryE(self):
		pass
	
casesPreClient=("userValid","stagerUp","rfioFine")
casesPut=("basicPut","putAndRfcp","putManyFiles","putTag","putSvcClass","putR")
casesPutDone=("basicPutDone","putDoneAndRfcp","putDoneManyFiles","putDoneR")
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


 
