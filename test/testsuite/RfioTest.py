import unittest
import UtilityForCastorTest
import os
import sys
import time
import threading

ticket= UtilityForCastorTest.getTicket()
stagerHost=""
stagerPort=""
stagerSvcClass=""
stagerVersion=""
remoteHost=""

localDir=""
dirCastor=""
myRemoteDir=""

myTurl=""
myTag=""
caseScen=""
stageMap="no"
castorConf="no"

#global localDir
localDir="./tmpClientTest"+ticket+"/"
myCastor=UtilityForCastorTest.prepareCastorString()
#global dirCastor
dirCastor=myCastor+"TestClient"+ticket+"/"

print "After the test, delete the different log files used in:"
print "- "+localDir
print "- "+dirCastor
print "- REMOTE_HOST:/tmp"+localDir[1:]+" (with remoteHostSpecified in the RFIOTESTCONFIG)"
print

os.system("rfmkdir "+dirCastor)

class RfioPreRequisitesCase(unittest.TestCase):
	def mainScenariumSetUp(self):
		try:
		        f=open("/etc/castor/RFIOTESTCONFIG","r")
		        configFileInfo=f.read()
			f.close()
  
			global stagerHost
		 	stagerHost=(configFileInfo[configFileInfo.find("STAGE_HOST"):]).split()[1]

			global stagerPort
		 	stagerPort=(configFileInfo[configFileInfo.find("STAGE_PORT"):]).split()[1]

			global stagerSvcClass
		 	stagerSvcClass=(configFileInfo[configFileInfo.find("STAGE_SVCCLASS"):]).split()[1]

		        global stagerVersion
		 	stagerVersion=(configFileInfo[configFileInfo.find("CASTOR_V2"):]).split()[1]

		 	remoteHost=(configFileInfo[configFileInfo.find("REMOTE_HOST"):]).split()[1]

			global myRemoteDir
			myRemoteDir=remoteHost +":/tmp"+localDir[1:]
			os.system("rfmkdir "+myRemoteDir)
			
			global stageMap
		 	stageMap=((configFileInfo[configFileInfo.find("USE_STAGEMAP.CONF"):]).split()[1]).lower()

			global castorConf
		 	castorConf=((configFileInfo[configFileInfo.find("USE_CASTOR.CONF"):]).split()[1]).lower()
			
	        except IOError:
			assert 0==-1, "An error in the preparation of the main setting occurred ... test is not valid"
		
		
	def stageMapSituation(self):
		try:
		   f=open("/etc/castor/stagemap.conf","r")
		   f.close()
		   assert stageMap == "yes", "You might use value in /etc/castor/stagemap.conf even if you don't want."
                except IOError:                     
                   assert stageMap == "no", "It is not possible to read /etc/castor/stagemap.conf."

		
	def castorConfSituation(self):
		try:
		   f=open("/etc/castor/castor.conf","r")
		   f.close()
		   assert castorConf == "yes", "You might use value in /etc/castor/castor.conf even if you don't want."
                except IOError:                     
                   assert castorConf == "no", "It is not possible to read /etc/castor/castor.conf."



class RfioRfcpSimpleCase(unittest.TestCase):
	def localToLocal(self):
		cmd=caseScen
		cmd=["rfcp /etc/group "+localDir+"fileRfio1"+ticket,"rfcp "+localDir+"fileRfio1"+ticket+" "+localDir+"fileRfio1Copy"+ticket,"diff /etc/group "+localDir+"fileRfio1Copy"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"RfioSimpleLocalToLocal",cmd)
                fi=open(localDir+"RfioSimpleLocalToLocal","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and local (out)") != -1, "rfio doesn't work local to local"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work local to local"

                fi=open(localDir+"RfioSimpleLocalToLocal1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and local (out)") != -1, "rfio doesn't work local to local"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work local to local"
        	assert os.stat(localDir+"RfioSimpleLocalToLocal2")[6] == 0, "rfio doesn't work local to local"
                assert os.stat(localDir+"fileRfio1Copy"+ticket)[6] != 0, "rfio doesn't work local to local"

        def localToRemote(self):
                cmd=[caseScen+"rfcp /etc/group "+myRemoteDir+"localToRemote1"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"RfioSimpleLocalToRemote",cmd)
                fi=open(localDir+"RfioSimpleLocalToRemote","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1, "rfio doesn't work from local to remote"
        def remoteToLocal(self):
                cmd=["rfcp /etc/group "+myRemoteDir+"localToRemote2"+ticket,"rfcp "+myRemoteDir+"localToRemote2"+ticket+" "+localDir+"fileRfio3"+ticket,"diff /etc/group "+localDir+"fileRfio3"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"RfioSimpleRemoteToLocal",cmd)
                
                fi=open(localDir+"RfioSimpleRemoteToLocal","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1, "rfio doesn't work from local to remote"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from local to remote"

                fi=open(localDir+"RfioSimpleRemoteToLocal1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and local (out)") != -1, "rfio doesn't work from  remote  to local"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from remote to local"

                assert os.stat(localDir+"fileRfio3"+ticket)[6] != 0, "rfio doesn't work from remote to local"
                assert os.stat(localDir+"RfioSimpleRemoteToLocal2")[6] == 0, "rfio doesn't work from remote to local"
                
        def remoteToRemote(self):
                cmd=["rfcp /etc/group "+myRemoteDir+"remoteToRemote1"+ticket,"rfcp "+myRemoteDir+"remoteToRemote1"+ticket+" "+myRemoteDir+"remoteToRemote2"+ticket,"rfcp "+myRemoteDir+"remoteToRemote2"+ticket+" "+localDir+"fileRfio4"+ticket,"diff /etc/group "+localDir+"fileRfio4"+ticket]

		UtilityForCastorTest.saveOnFile(localDir+"RfioSimpleRemoteToRemote",cmd)
                
                fi=open(localDir+"RfioSimpleRemoteToRemote","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1, "rfio doesn't work from local to remote"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from local to remote"

                fi=open(localDir+"RfioSimpleRemoteToRemote1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and eth0 (out)") != -1, "rfio doesn't work from  remote  to remote"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from remote to remote"

                fi=open(localDir+"RfioSimpleRemoteToRemote2","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and local (out)") != -1, "rfio doesn't work from  remote  to local"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from remote to local"

                assert os.stat(localDir+"fileRfio4"+ticket)[6] != 0, "rfio doesn't work from remote to remote"
                assert os.stat(localDir+"RfioSimpleRemoteToRemote3")[6] == 0, "rfio doesn't work from remote to remote"
                
      
class RfioRfcpCastorBasic(unittest.TestCase):          
        def localToCastor(self):
	        cmd=[caseScen+"rfcp /etc/group "+dirCastor+"fileLocalToCastor"+myTag+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"RfioLocalToCastor"+myTag,cmd)
                fi=open(localDir+"RfioLocalToCastor"+myTag,"r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1 , "rfio doesn't work from local to castor"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work local to castor"
                
	def castorToLocal(self):
                cmd=[caseScen+"rfcp /etc/group "+dirCastor+"fileCastorToLocal"+myTag+ticket,caseScen+"rfcp "+dirCastor+"fileCastorToLocal"+myTag+ticket+" "+localDir+"fileCastorToLocal"+myTag,"diff /etc/group "+localDir+"fileCastorToLocal"+myTag]

		
		UtilityForCastorTest.saveOnFile(localDir+"RfioCastorToLocal"+myTag,cmd)
                
                fi=open(localDir+"RfioCastorToLocal"+myTag,"r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)")or buffOut.rfind(" bytes ready for migration") != -1 != -1, "rfio doesn't work from local to castor"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from local to castor"

                fi=open(localDir+"RfioCastorToLocal"+myTag+"1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and local (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1 , "rfio doesn't work from  castor to local"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from castor to local"

                assert os.stat(localDir+"fileCastorToLocal"+myTag)[6] != 0, "rfio doesn't work from castor to local"
                assert os.stat(localDir+"RfioCastorToLocal"+myTag+"2")[6] == 0, "rfio doesn't work from castor to local"

        def remoteToCastor(self):
                cmd=[caseScen+"rfcp /etc/group "+myRemoteDir+"fileRemoteToCastor1"+myTag+ticket,caseScen+"rfcp "+myRemoteDir+"fileRemoteToCastor1"+myTag+ticket+" "+dirCastor+"fileRemoteToCastor2"+myTag+ticket,caseScen+"rfcp "+dirCastor+"fileRemoteToCastor2"+myTag+ticket+" "+localDir+"fileRemoteToCastor"+myTag,"diff /etc/group "+localDir+"fileRemoteToCastor"+myTag]


		UtilityForCastorTest.saveOnFile(localDir+"RfioRemoteToCastor"+myTag,cmd)
                
                fi=open(localDir+"RfioRemoteToCastor"+myTag,"r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from local to remote"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from local to remote"

                fi=open(localDir+"RfioRemoteToCastor"+myTag+"1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and eth0 (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from  remote  to castor"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from remote to castor"

                fi=open(localDir+"RfioRemoteToCastor"+myTag+"2","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and local (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from  castor  to local"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from remote to local"

                assert os.stat(localDir+"fileRemoteToCastor"+myTag)[6] != 0, "rfio doesn't work from remote to castor"
                assert os.stat(localDir+"RfioRemoteToCastor"+myTag+"3")[6] == 0, "rfio doesn't work from remote to castor"
                
        def castorToRemote(self):
                
                cmd=[caseScen+"rfcp /etc/group "+dirCastor+"fileCastorToRemote1"+myTag+ticket,caseScen+"rfcp "+dirCastor+"fileCastorToRemote1"+myTag+ticket+" "+myRemoteDir+"fileCastorToRemote2"+myTag+ticket,caseScen+"rfcp "+myRemoteDir+"fileCastorToRemote2"+myTag+ticket+" "+localDir+"fileCastorToRemote"+myTag,"diff /etc/group "+localDir+"fileCastorToRemote"+myTag]

		UtilityForCastorTest.saveOnFile(localDir+"RfioCastorToRemote"+myTag,cmd)
                
                fi=open(localDir+"RfioCastorToRemote"+myTag,"r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from local to castor"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from local to castor"

                fi=open(localDir+"RfioCastorToRemote"+myTag+"1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and eth0 (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from  castor  to remote"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from castor to remote"

                fi=open(localDir+"RfioCastorToRemote"+myTag+"2","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and local (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1 , "rfio doesn't work from  remote  to local"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from remote to local"

                assert os.stat(localDir+"fileCastorToRemote"+myTag)[6] != 0, "rfio doesn't work from castor to remote"
                assert os.stat(localDir+"RfioCastorToRemote"+myTag+"3")[6] == 0, "rfio doesn't work from castor to remote"

        def castorToCastor(self):
                
                cmd=[caseScen+"rfcp /etc/group "+dirCastor+"fileCastorToCastor1"+myTag+ticket,caseScen+"rfcp "+dirCastor+"fileCastorToCastor1"+myTag+ticket+" "+dirCastor+"fileCastorToCastor2"+myTag+ticket,caseScen+"rfcp "+dirCastor+"fileCastorToCastor2"+myTag+ticket+" "+localDir+"fileCastorToCastor"+myTag,"diff /etc/group "+localDir+"fileCastorToCastor"+myTag]

		UtilityForCastorTest.saveOnFile(localDir+"RfioCastorToCastor"+myTag,cmd)
                
                fi=open(localDir+"RfioCastorToCastor"+myTag,"r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from local to castor"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from local to castor"

                fi=open(localDir+"RfioCastorToCastor"+myTag+"1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and eth0 (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from  castor  to castor"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from castor to castor"

                fi=open(localDir+"RfioCastorToCastor"+myTag+"2","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and local (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from  castor  to local"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from remote to local"

                assert os.stat(localDir+"fileCastorToCastor"+myTag)[6] != 0, "rfio doesn't work from castor to castor"
                assert os.stat(localDir+"RfioCastorToCastor"+myTag+"3")[6] == 0, "rfio doesn't work from castor to castor"

class RfioRfcpCastorBasicFancyTurl(unittest.TestCase):

        def localToCastor(self):
	        cmd=[caseScen+"rfcp /etc/group "+myTurl+"fileLocalToCastor"+myTag+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"RfioFancyLocalToCastor"+myTag,cmd)
	
		
                fi=open(localDir+"RfioFancyLocalToCastor"+myTag,"r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1, "rfio doesn't work from local to castor"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work local to castor"
                
	def castorToLocal(self):
                cmd=[caseScen+"rfcp /etc/group "+myTurl+"fileCastorToLocal1"+myTag+ticket,caseScen+"rfcp "+myTurl+"fileCastorToLocal1"+myTag+ticket+" "+localDir+"fileCastorToLocal2"+myTag+ticket,"diff /etc/group "+localDir+"fileCastorToLocal2"+myTag+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"RfioFancyCastorToLocal"+myTag,cmd)
		
                
                fi=open(localDir+"RfioFancyCastorToLocal"+myTag,"r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1, "rfio doesn't work from local to castor"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from local to castor"

                fi=open(localDir+"RfioFancyCastorToLocal"+myTag+"1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and local (out)") != -1, "rfio doesn't work from  castor to local"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from castor to local"

                assert os.stat(localDir+"fileCastorToLocal2"+myTag+ticket)[6] != 0, "rfio doesn't work from castor to local"
                assert os.stat(localDir+"RfioFancyCastorToLocal"+myTag+"2")[6] == 0, "rfio doesn't work from castor to local"

        def remoteToCastor(self):
                cmd=[caseScen+"rfcp /etc/group "+myRemoteDir+"fileRemoteToCastor1"+myTag+ticket,caseScen+"rfcp "+myRemoteDir+"fileRemoteToCastor1"+myTag+ticket+" "+myTurl+"fileRemoteToCastor1"+myTag+ticket,caseScen+"rfcp "+myTurl+"fileRemoteToCastor1"+myTag+ticket+" "+localDir+"fileRemoteToCastor"+myTag+ticket,"diff /etc/group "+localDir+"fileRemoteToCastor"+myTag+ticket]

		UtilityForCastorTest.saveOnFile(localDir+"RfioFancyRemoteToCastor"+myTag,cmd)
                
                fi=open(localDir+"RfioFancyRemoteToCastor"+myTag,"r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1, "rfio doesn't work from local to remote"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from local to remote"

                fi=open(localDir+"RfioFancyRemoteToCastor"+myTag+"1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and eth0 (out)") != -1, "rfio doesn't work from  remote  to castor"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from remote to castor"

                fi=open(localDir+"RfioFancyRemoteToCastor"+myTag+"2","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and local (out)") != -1, "rfio doesn't work from  castor  to local"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from remote to local"

                assert os.stat(localDir+"fileRemoteToCastor"+myTag+ticket)[6] != 0, "rfio doesn't work from remote to castor"
                assert os.stat(localDir+"RfioFancyRemoteToCastor"+myTag+"3")[6] == 0, "rfio doesn't work from remote to castor"
                
        def castorToRemote(self):
                
		cmd=[caseScen+"rfcp /etc/group "+myTurl+"fileCastorToRemote1"+myTag+ticket,caseScen+"rfcp "+myTurl+"fileCastorToRemote1"+myTag+ticket+" "+myRemoteDir+"fileCastorToRemote2"+myTag+ticket,caseScen+"rfcp "+myRemoteDir+"fileCastorToRemote2"+myTag+ticket+" "+localDir+"fileCastorToRemote"+myTag+ticket,"diff /etc/group "+localDir+"fileCastorToRemote"+myTag+ticket]

		UtilityForCastorTest.saveOnFile(localDir+"RfioFancyCastorToRemote"+myTag,cmd)
                
                fi=open(localDir+"RfioFancyCastorToRemote"+myTag,"r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1, "rfio doesn't work from local to castor"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from local to castor"

                fi=open(localDir+"RfioFancyCastorToRemote"+myTag+"1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and eth0 (out)") != -1, "rfio doesn't work from  castor  to remote"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from castor to remote"

                fi=open(localDir+"RfioFancyCastorToRemote"+myTag+"2","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and local (out)") != -1, "rfio doesn't work from  remote  to local"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from remote to local"

                assert os.stat(localDir+"fileCastorToRemote"+myTag+ticket)[6] != 0, "rfio doesn't work from castor to remote"
                assert os.stat(localDir+"RfioFancyCastorToRemote"+myTag+"3")[6] == 0, "rfio doesn't work from castor to remote"

        def castorToCastor(self):
                
                cmd=[caseScen+"rfcp /etc/group "+myTurl+"fileCastorToCastor1"+myTag+ticket,caseScen+"rfcp "+myTurl+"fileCastorToCastor1"+myTag+ticket+" "+myTurl+"fileCastorToCastor2"+myTag+ticket,caseScen+"rfcp "+myTurl+"fileCastorToCastor2"+myTag+ticket+" "+localDir+"fileCastorToCastor"+myTag+ticket,"diff /etc/group "+localDir+"fileCastorToCastor"+myTag+ticket]

		UtilityForCastorTest.saveOnFile(localDir+"RfioFancyCastorToCastor"+myTag,cmd)

                fi=open(localDir+"RfioFancyCastorToCastor"+myTag,"r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1, "rfio doesn't work from local to castor"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from local to castor"

                fi=open(localDir+"RfioFancyCastorToCastor"+myTag+"1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and eth0 (out)") != -1, "rfio doesn't work from  castor  to castor"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from castor to castor"

                fi=open(localDir+"RfioFancyCastorToCastor"+myTag+"2","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and local (out)") != -1, "rfio doesn't work from  castor  to local"
        	assert buffOut.rfind("error") == -1, "rfio doesn't work from remote to local"

                assert os.stat(localDir+"fileCastorToCastor"+myTag+ticket)[6] != 0, "rfio doesn't work from castor to castor"
                assert os.stat(localDir+"RfioFancyCastorToCastor"+myTag+"3")[6] == 0, "rfio doesn't work from castor to castor"



#different scenarium for env

def createScenarium(host,port,serviceClass,version):
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
	        return myScen
	return myScen

class RfioRfcpScenarium1(RfioRfcpCastorBasic):
        def setUp(self):
	    global myTag
	    myTag="Env1"
	    global caseScen
            caseScen=createScenarium(-1,-1,-1,-1)
    
class RfioRfcpScenarium2(RfioRfcpCastorBasic):
        def setUp(self):
	    global myTag
	    myTag="Env2"
	    global caseScen
	    caseScen=createScenarium(stagerHost,-1,-1,stagerVersion)
            
class RfioRfcpScenarium3(RfioRfcpCastorBasic):
        def setUp(self):
	    global myTag
	    myTag="Env3"
	    global caseScen
            caseScen=createScenarium(stagerHost,stagerPort,-1,stagerVersion)


class RfioRfcpScenarium4(RfioRfcpCastorBasic):
        def setUp(self):
	    global myTag
	    myTag="Env4"
	    global caseScen
            caseScen=createScenarium(stagerHost,-1,stagerSvcClass,stagerVersion)

class RfioRfcpScenarium5(RfioRfcpCastorBasic):
        def setUp(self):
	    global myTag
	    myTag="Env5"
	    global caseScen
            caseScen=createScenarium(stagerHost,stagerPort,stagerSvcClass,stagerVersion)
	    
class RfioRfcpCastorFancyTurl1(RfioRfcpCastorBasicFancyTurl):
        def setUp(self):
	    global myTag
	    myTag="Turl1"
	    global caseScen
            caseScen=createScenarium(stagerHost,stagerPort,stagerSvcClass,stagerVersion)
            global myTurl 
	    myTurl="rfio:///"+dirCastor+"turl1"


class RfioRfcpCastorFancyTurl2(RfioRfcpCastorBasicFancyTurl):
        def setUp(self):
	    global myTag
	    myTag="Turl2"
	    global caseScen
            caseScen=createScenarium(stagerHost,stagerPort,stagerSvcClass,stagerVersion)
            global myTurl
	    myTurl="rfio://"+dirCastor+"turl2"

            
class RfioRfcpCastorFancyTurl3(RfioRfcpCastorBasicFancyTurl):
        def setUp(self):
	    global myTag
	    myTag="Turl3"
	    global caseScen
            caseScen=createScenarium(stagerHost,stagerPort,stagerSvcClass,stagerVersion)
            global myTurl
	    myTurl="rfio:///castor\\?path="+dirCastor+"turl3"
  


class RfioRfcpCastorFancyTurl4(RfioRfcpCastorBasicFancyTurl):
        def setUp(self):
	    global myTag
	    myTag="Turl4"
            global caseScen
            caseScen=createScenarium(-1,stagerPort,stagerSvcClass,stagerVersion)
            global myTurl
	    myTurl="rfio://"+stagerHost+"/castor\\?path="+dirCastor+"turl4"

            
class RfioRfcpCastorFancyTurl5(RfioRfcpCastorBasicFancyTurl):
        def setUp(self):
	    global myTag
	    myTag="Turl5"
	    global caseScen
            caseScen=createScenarium(-1,stagerPort,stagerSvcClass,stagerVersion)
            global myTurl
	    myTurl="rfio://"+stagerHost+"//castor\\?path="+dirCastor+"turl5"

            
class RfioRfcpCastorFancyTurl6(RfioRfcpCastorBasicFancyTurl):
        def setUp(self):
	    global myTag
	    myTag="Turl6"
            createScenarium(-1,-1,stagerSvcClass,stagerVersion)
            global myTurl
	    myTurl="rfio://"+stagerHost+":"+stagerPort+"/castor\\?path="+dirCastor+"turl6"
          
            
class RfioRfcpCastorFancyTurl7(RfioRfcpCastorBasicFancyTurl):
        def setUp(self):
	    global myTag
	    myTag="Turl7"
	    global caseScen
            caseScen=createScenarium(-1,stagerPort,stagerSvcClass,stagerVersion)
            global myTurl
	    myTurl="rfio://"+stagerHost+"/castor\\?path="+dirCastor+"turl7"
 

class RfioRfcpCastorFancyTurl8(RfioRfcpCastorBasicFancyTurl):
        def setUp(self):
	    global myTag
	    myTag="Turl8"
	    global caseScen
            caseScen=createScenarium(-1,-1,stagerSvcClass,stagerVersion)
            global myTurl
	    myTurl="rfio://"+stagerHost+":"+stagerPort+"/castor\\?path="+dirCastor+"turl8"
    
 
class RfioRfcpCastorFancyTurl9(RfioRfcpCastorBasicFancyTurl):
        def setUp(self):
	    global myTag
	    myTag="Turl9"
	    global caseScen
            caseScen=createScenarium(-1,-1,stagerSvcClass,stagerVersion)
            global myTurl
	    myTurl="rfio://"+stagerHost+":"+stagerPort+"/"+dirCastor+"turl9"

 
class RfioRfcpCastorFancyTurl10(RfioRfcpCastorBasicFancyTurl):
        def setUp(self):
	    global myTag
	    myTag="Turl10"
	    global caseScen
            caseScen=createScenarium(-1,-1,stagerSvcClass,stagerVersion)
            global myTurl
	    myTurl="rfio://"+stagerHost+":"+stagerPort+dirCastor+"turl10"

class RfioRfcpCastorFancyTurl11(RfioRfcpCastorBasicFancyTurl):
        def setUp(self):
	    global myTag
	    myTag="Turl11"
	    global caseScen
            caseScen=createScenarium(-1,-1,-1,-1)
            global myTurl
	    myTurl="rfio://"+stagerHost+":"+stagerPort+"/castor\\?svcClass="+stagerSvcClass+"\\&castorVersion="+stagerVersion+"\\&path="+dirCastor+"turl11"
      

casesRfcpSimple=("localToLocal","localToRemote","remoteToLocal","remoteToRemote")
casesRfcpCastor=("localToCastor","castorToLocal","remoteToCastor","castorToRemote","castorToCastor")
casesPreReq=("mainScenariumSetUp","stageMapSituation","castorConfSituation")


## PREREQ AND SETTING

class RfioPreRequisitesSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioPreRequisitesCase,casesPreReq))
	


##  BASIC TEST RFIO

class RfioRfcpSimpleSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpSimpleCase,casesRfcpSimple))


## DIFFERENT ENV POSSIBILITY FOR CASTOR


class RfioRfcpEnv1Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpScenarium1,casesRfcpCastor))
class RfioRfcpEnv2Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpScenarium2,casesRfcpCastor))

class RfioRfcpEnv3Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpScenarium3,casesRfcpCastor))

class RfioRfcpEnv4Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpScenarium4,casesRfcpCastor))
        
class RfioRfcpEnv5Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpScenarium5,casesRfcpCastor))


###  BIG TEST SUITE WITH ALL ENV CASES



class RfioRfcpEnvSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,(RfioRfcpEnv1Suite(),RfioRfcpEnv2Suite(),RfioRfcpEnv3Suite(),RfioRfcpEnv4Suite(),RfioRfcpEnv5Suite()))



## FANCY TURL POSSIBILITY
        
class RfioRfcpCastorFancyTurl1Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpCastorFancyTurl1,casesRfcpCastor))

class  RfioRfcpCastorFancyTurl2Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpCastorFancyTurl2,casesRfcpCastor))

class RfioRfcpCastorFancyTurl3Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpCastorFancyTurl3,casesRfcpCastor))

class  RfioRfcpCastorFancyTurl4Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpCastorFancyTurl4,casesRfcpCastor))
        
class  RfioRfcpCastorFancyTurl5Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpCastorFancyTurl5,casesRfcpCastor))

class  RfioRfcpCastorFancyTurl6Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpCastorFancyTurl6,casesRfcpCastor))

class  RfioRfcpCastorFancyTurl7Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpCastorFancyTurl7,casesRfcpCastor))

class  RfioRfcpCastorFancyTurl8Suite(unittest.TestSuite):

    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpCastorFancyTurl8,casesRfcpCastor))

class  RfioRfcpCastorFancyTurl9Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpCastorFancyTurl9,casesRfcpCastor))

class  RfioRfcpCastorFancyTurl10Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpCastorFancyTurl10,casesRfcpCastor))

class  RfioRfcpCastorFancyTurl11Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpCastorFancyTurl11,casesRfcpCastor))

###  BIG TEST SUITE WITH ALL FANCY TURL CASES

class RfioRfcpFancyTurlSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,(RfioRfcpCastorFancyTurl1Suite(),RfioRfcpCastorFancyTurl2Suite(),RfioRfcpCastorFancyTurl3Suite(),RfioRfcpCastorFancyTurl4Suite(),RfioRfcpCastorFancyTurl5Suite(),RfioRfcpCastorFancyTurl6Suite(),RfioRfcpCastorFancyTurl7Suite(),RfioRfcpCastorFancyTurl8Suite(),RfioRfcpCastorFancyTurl9Suite(),RfioRfcpCastorFancyTurl10Suite(),RfioRfcpCastorFancyTurl11Suite()))

