import unittest
import UtilityForCastorTest
import os
import sys
import time
import threading

ticket= UtilityForCastorTest.getTicket() 
localDir="./tmpClientTest"+ticket+"/"
myCastor=UtilityForCastorTest.prepareCastorString()
dirCastor=myCastor+"TestClient"+ticket+"/"
os.system("mkdir "+localDir)

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
	def userValid(self):
		assert (UtilityForCastorTest.checkUser != -1), "you don't have a valid castor directory"
		
	def stagerUp(self):
		cmd=["stager_qry -M "+myCastor+"noFile"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"stagerUp",cmd)
		fi=open(localDir+"stagerUp","r")
		outBuff=fi.read()
		fi.close()
        	assert (outBuff.rfind("Unknown host")== -1), "the stager host is unknown" 
		assert (outBuff.rfind("Internal error")== -1), "the stager gives Internal errors" 
	
	def rfioFine(self):
		cmd=["rfmkdir "+dirCastor,localDir+"rfioFine1","rfcp /etc/group "+dirCastor+"fileRfcp1"+ticket,"rfcp "+dirCastor+"fileRfcp1"+ticket+" "+dirCastor+"fileRfcp2"+ticket,"rfcp "+dirCastor+"fileRfcp1"+ticket+"  "+localDir+"fileRfcp1Copy","rfcp "+dirCastor+"fileRfcp2"+ticket+"  "+localDir+"fileRfcp2Copy","diff /etc/group "+localDir+"fileRfcp1Copy","diff /etc/group "+localDir+"fileRfcp2Copy"]
		UtilityForCastorTest.saveOnFile(localDir+"rfioFine",cmd)

	        assert os.stat(localDir+"fileRfcp1Copy")[6] != 0, "Rfcp doesn't work"
		assert os.stat(localDir+"fileRfcp2Copy")[6] != 0, "Rfcp doesn't work"
		assert os.stat(localDir+"rfioFine6")[6] == 0, "Rfcp doesn't work"
                assert os.stat(localDir+"rfioFine7")[6] == 0, "Rfcp doesn't work"
		 
	
class StagerPutCase(unittest.TestCase):
	def basicPut(self):
		cmd=["stager_put -M "+dirCastor+"filePut1"+ticket,"stager_qry -M "+dirCastor+"filePut1"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"basicPut",cmd)
		fi=open(localDir+"basicPut","r")
        	assert fi.read().rfind("SUBREQUEST_READY") != -1, "stager_put doesn't work"
        	fi.close()
		fi=open(localDir+"basicPut1","r")
        	assert fi.read().rfind("STAGEOUT") != -1, "stager_qry doesn't work after a simple put"
        	fi.close()
		
	def putAndRfcp(self):
		cmd=["stager_put -M "+dirCastor+"filePut2"+ticket,"rfcp /etc/group "+dirCastor+"filePut2"+ticket,"stager_qry -M "+dirCastor+"filePut2"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"putAndRfcp",cmd)
		fi=open(localDir+"putAndRfcp2","r")
        	assert fi.read().find("STAGEOUT") != -1, "stager_qry doesn't work after stager_put and rfcp"
        	fi.close()
		
	def putManyFiles(self):
		cmd=["stager_put -M "+dirCastor+"filePut3"+ticket+" -M "+dirCastor+"filePut4"+ticket,"rfcp /etc/group "+dirCastor+"filePut3"+ticket,"stager_qry -M "+dirCastor+"filePut3"+ticket,"rfcp /etc/group "+dirCastor+"filePut4"+ticket,"stager_qry -M "+dirCastor+"filePut4"+ticket,"stager_qry -M "+dirCastor+"filePut3"+ticket+" -M "+dirCastor+"filePut4"+ticket]

	        UtilityForCastorTest.saveOnFile(localDir+"putManyFiles",cmd)

		fi=open(localDir+"putManyFiles","r")
        	assert fi.read().count("SUBREQUEST_READY") == 2, "stager_put doesn't work with two files"
        	fi.close()

		fi=open(localDir+"putManyFiles2","r")
        	assert fi.read().find("STAGEOUT") != -1, "stager_qry doesn't work after stager_put with many files and rfcp"
		fi.close()
		 
		fi=open(localDir+"putManyFiles4","r")
        	assert fi.read().find("STAGEOUT") != -1, "stager_qry doesn't work after stager_put with many files and rfcp"
		fi.close()
 
		fi=open(localDir+"putManyFiles5","r")
		assert fi.read().count("STAGEOUT") == 2, "stager_qry doesn't work with two parameters"
		fi.close()
		
	def putTag(self):
		cmd=["stager_put -M "+dirCastor+"filePut5"+ticket+" -U tagPut"+ticket,"stager_qry -M "+dirCastor+"filePut5"+ticket,"stager_qry -U tagPut"+ticket,"rfcp /etc/group "+dirCastor+"filePut5"+ticket,"stager_qry -M "+dirCastor+"filePut5"+ticket,"stager_qry -U tagPut"+ticket]
		
	        UtilityForCastorTest.saveOnFile(localDir+"putTag",cmd)
	
		fi=open(localDir+"putTag","r")
        	assert fi.read().find("SUBREQUEST_READY") != -1, "put doesn't work with tag"
        	fi.close()

		fi=open(localDir+"putTag1","r")
        	assert fi.read().find("STAGEOUT") != -1, "stager_qry doesn't work after a put with tag"
        	fi.close()
	
		fi=open(localDir+"putTag2","r")
        	assert fi.read().find("STAGEOUT") != -1, "stager_qry doesn't work after a put with tag"
        	fi.close()

		fi=open(localDir+"putTag4","r")
		assert fi.read().find("STAGEOUT") != -1, "stager_qry doesn't work with tag"
		fi.close()
		
		fi=open(localDir+"putTag5","r")
		assert fi.read().find("STAGEOUT") != -1, "stager_qry doesn't work with tag"
		fi.close()
		
	def putSvcClass(self):
		pass
	def putR(self):
		pass

class StagerPutDoneCase(unittest.TestCase):
	def basicPutDone(self):
		cmd=["stager_put -M "+dirCastor+"fileDone1"+ticket,"stager_putdone -M "+dirCastor+"fileDone1"+ticket,"stager_qry -M "+dirCastor+"fileDone1"+ticket]
		
	        UtilityForCastorTest.saveOnFile(localDir+"basicPutDone",cmd)
		
		fi=open(localDir+"basicPutDone1","r")
        	assert fi.read().find("error=1") != -1, "putDone doesn't work"
        	fi.close()

		fi=open(localDir+"basicPutDone2","r")
        	assert fi.read().find("STAGEOUT") != -1, "stager_qry doesn't work after a putDone"
        	fi.close()
		
	def putDoneAndRfcp(self):
		cmd=["stager_put -M "+dirCastor+"fileDone2"+ticket,"rfcp /etc/group "+dirCastor+"fileDone2"+ticket,"stager_putdone -M "+dirCastor+"fileDone2"+ticket,"stager_qry -M "+dirCastor+"fileDone2"+ticket]

	        UtilityForCastorTest.saveOnFile(localDir+"putDoneAndRfcp",cmd)
       
		fi=open(localDir+"putDoneAndRfcp2","r")
        	assert fi.read().find("error=0") != -1, "putDone doesn't work with rfcp"
        	fi.close()
	
		fi=open(localDir+"putDoneAndRfcp3","r")
        	assert fi.read().find("CANBEMIGR") != -1, "stager_qry doesn't work after a putDone with rfcp"
        	fi.close()
		
	def putDoneManyFiles(self):
		cmd=["stager_put -M "+dirCastor+"fileDone3"+ticket+" -M "+dirCastor+"fileDone4"+ticket,"rfcp /etc/group "+dirCastor+"fileDone3"+ticket,"rfcp /etc/group "+dirCastor+"fileDone4"+ticket,"stager_putdone -M "+dirCastor+"fileDone3"+ticket+" -M "+dirCastor+"fileDone4"+ticket,"stager_qry -M "+dirCastor+"fileDone3"+ticket,"stager_qry -M "+dirCastor+"fileDone4"+ticket,"stager_qry -M "+dirCastor+"fileDone3"+ticket+" -M "+dirCastor+"fileDone4"+ticket]

		UtilityForCastorTest.saveOnFile(localDir+"putDoneManyFiles",cmd)

		fi=open(localDir+"putDoneManyFiles3","r")
        	assert fi.read().find("error=0") != -1, "putDone doesn't work with many files"
        	fi.close()
	
		fi=open(localDir+"putDoneManyFiles4","r")
        	assert fi.read().find("CANBEMIGR") != -1, "stager_qry doesn't work after a putDone with many files"
        	fi.close()
		
		fi=open(localDir+"putDoneManyFiles5","r")
        	assert fi.read().find("CANBEMIGR") != -1, "stager_qry doesn't work after a putDone with many files"
        	fi.close()
		
		fi=open(localDir+"putDoneManyFiles6","r")
        	assert fi.read().count("CANBEMIGR") == 2,  "stager_qry doesn't work after a putDone with many files"
        	fi.close()	
		
	def putDoneR(self):	
		pass
	
	def putDoneAndLongFile(self):
		fileBig=MakeBigFile()
		t=threading.Timer(120.0,UtilityForCastorTest.timeOut,"fake")
		t2=threading.Timer(120.0,UtilityForCastorTest.timeOut,"fake")
		cmd1=["stager_put -M "+dirCastor+"fileDone5"+ticket,"rfcp "+fileBig+" "+dirCastor+"fileDone5"+ticket]
		cmd2=["stager_rm "+dirCastor+"fileDone5"+ticket,"stager_putdone -M "+dirCastor+"fileDone5"+ticket]

	        UtilityForCastorTest.runOnShell(cmd1)
		time.sleep(1)
                UtilityForCastorTest.saveOnFile(localDir+"putDoneAndLongFile",cmd2)
                os.system("rm "+fileBig) 
		fi=open(localDir+"putDoneAndLongFile1","r")
        	assert fi.read().find("SUBREQUEST_FAILED") != -1, "putDone doesn't work with big files"
        	fi.close()

	
class StagerGetCase(unittest.TestCase):
	def basicGet(self):
		cmd=["stager_put -M "+dirCastor+"fileGet1"+ticket,"stager_get -M "+dirCastor+"fileGet1"+ticket,"stager_qry -M "+dirCastor+"fileGet1"+ticket]

	        UtilityForCastorTest.saveOnFile(localDir+"basicGet",cmd)

		fi=open(localDir+"basicGet1","r")
            	assert fi.read().find("SUBREQUEST_READY") != -1, "stager_get doesn't work after put"
            	fi.close()
	
		fi=open(localDir+"basicGet2","r")
        	assert fi.read().find("STAGEOUT") != -1, "stager_get doesn't work after put"
        	fi.close()
		
	def getAndRfcp(self):
		cmd=["stager_put -M "+dirCastor+"fileGet2"+ticket,"rfcp /etc/group "+dirCastor+"fileGet2"+ticket,"stager_get -M "+dirCastor+"fileGet2"+ticket,"stager_putdone -M "+dirCastor+"fileGet2"+ticket,"stager_get -M "+dirCastor+"fileGet2"+ticket]
		
		UtilityForCastorTest.saveOnFile(localDir+"getAndRfcp",cmd) 
		
		fi=open(localDir+"getAndRfcp2","r")
		assert fi.read().find("SUBREQUEST_READY") != -1, "stager_get doesn't work after stager_put and rfcp"
		fi.close()
	
		fi=open(localDir+"getAndRfcp3","r")
            	assert fi.read().find("error=0") != -1, "stager_get doesn't work after rfcp and putdone"
            	fi.close()
	
		fi=open(localDir+"getAndRfcp4","r")
        	assert fi.read().find("SUBREQUEST_READY") != -1, "stager_get doesn't work after a put, rfcp and putDone"
        	fi.close()
		
	def getManyFiles(self):
		cmd=["stager_put -M "+dirCastor+"fileGet3"+ticket+" -M "+dirCastor+"fileGet4"+ticket,"rfcp /etc/group "+dirCastor+"fileGet3"+ticket,"rfcp /etc/group "+dirCastor+"fileGet4"+ticket,"stager_get -M "+dirCastor+"fileGet3"+ticket+" -M "+dirCastor+"fileGet4"+ticket,"stager_putdone -M "+dirCastor+"fileGet3"+ticket,"stager_putdone -M "+dirCastor+"fileGet4"+ticket,"stager_get -M "+dirCastor+"fileGet3"+ticket+" -M "+dirCastor+"fileGet4"+ticket,"stager_qry -M "+dirCastor+"fileGet3"+ticket+" -M "+dirCastor+"fileGet4"+ticket]
	        UtilityForCastorTest.saveOnFile(localDir+"getManyFiles",cmd)
	
		fi=open(localDir+"getManyFiles3","r")
		assert fi.read().count("SUBREQUEST_READY") == 2, "stager_get doesn't work after stager_put with many files and rfcp"
		fi.close()
		
		fi=open(localDir+"getManyFiles6","r")
		assert fi.read().count("SUBREQUEST_READY") == 2, "stager_get doesn't work after put, rfcp and putdone with many files"
		fi.close()
       
		fi=open(localDir+"getManyFiles7","r")
        	assert fi.read().count("CANBEMIGR") == 2,  "stager_qry doesn't work after put, rfcp and putdone with many files"
        	fi.close()
		
	
	def getTag(self):
	        cmd=["stager_put -M "+dirCastor+"fileGet5"+ticket+" -U tagGet"+ticket,"stager_get -M "+dirCastor+"fileGet5"+ticket+" -U tagGet"+ticket,"rfcp /etc/group "+dirCastor+"fileGet5"+ticket,"stager_get  -M "+dirCastor+"fileGet5"+ticket+" -U tagGet"+ticket,"stager_putdone -M "+dirCastor+"fileGet5"+ticket,"stager_get -M "+dirCastor+"fileGet5"+ticket+" -U tagGet"+ticket ]

	        UtilityForCastorTest.saveOnFile(localDir+"getTag",cmd)
	
		fi=open(localDir+"getTag1","r")
		assert fi.read().find("SUBREQUEST_READY") != -1, "stager_get doesn't work with tag after put"
		fi.close()
      
		fi=open(localDir+"getTag3","r")
		assert fi.read().find("SUBREQUEST_READY") != -1, "stager_get doesn't work with tag after put and rfcp"
		fi.close()

		fi=open(localDir+"getTag5","r")
		assert fi.read().find("SUBREQUEST_READY") != -1, "stager_get doesn't work with tag after put, rfcp and putdone"
		fi.close()
		
	def getSvcClass(self):
		pass
	def getR(self):
		pass

class StagerRmCase(unittest.TestCase):
	def basicRm(self):
		cmd=["stager_put -M "+dirCastor+"fileRm1"+ticket,"stager_rm -M "+dirCastor+"fileRm1"+ticket]
         	UtilityForCastorTest.saveOnFile(localDir+"basicRm",cmd)
		     
		fi=open(localDir+"basicRm1","r")
		assert fi.read().find("SUBREQUEST_READY") != -1, "stager_rm doesn't work after put"
		fi.close()
		
	def rmAndRfcp(self):
		cmd=["stager_put -M "+dirCastor+"fileRm3"+ticket,"rfcp /etc/group "+dirCastor+"fileRm3"+ticket,"stager_rm -M "+dirCastor+"fileRm3"+ticket,]
		UtilityForCastorTest.saveOnFile(localDir+"rmAndRfcp",cmd)
	
		fi=open(localDir+"rmAndRfcp2","r")
		assert fi.read().find("SUBREQUEST_READY") != -1, "stager_rm doesn't work after put and rfcp"
		fi.close()

                cmd=["stager_put -M "+dirCastor+"fileRm4"+ticket,"rfcp /etc/group "+dirCastor+"fileRm4"+ticket,"stager_putdone -M "+dirCastor+"fileRm4"+ticket,"stager_rm -M "+dirCastor+"fileRm4"+ticket]

		UtilityForCastorTest.saveOnFile(localDir+"rmAndRfcpBis",cmd)

		fi=open(localDir+"rmAndRfcpBis3","r")
		assert fi.read().find("SUBREQUEST_FAILED") != -1, "stager_rm doesn't work after put, rfcp and putdone"
		fi.close() #weird .. not migrated yet ... 
		
	def rmManyFiles(self):
		cmd=["stager_put -M "+dirCastor+"fileRm5"+ticket+" -M"+dirCastor+"fileRm6"+ticket,"rfcp /etc/group "+dirCastor+"fileRm5"+ticket,"rfcp /etc/group "+dirCastor+"fileRm6"+ticket,"stager_rm -M "+dirCastor+"fileRm5"+ticket+" -M"+dirCastor+"fileRm6"+ticket]
		
	        UtilityForCastorTest.saveOnFile(localDir+"rmManyFiles",cmd)
	
		fi=open(localDir+"rmManyFiles3","r")
		assert fi.read().count("SUBREQUEST_READY") == 2, "stager_rm doesn't work with many files"
		fi.close()
		
class StagerSpecialQueryCase(unittest.TestCase):
	def queryS(self):
		pass
	def queryE(self):
		pass
		
	
casesPreClient=("userValid","stagerUp","rfioFine")
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


 
