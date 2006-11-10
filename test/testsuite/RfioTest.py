import unittest
import UtilityForCastorTest
import os
import sys
import time
import threading

ticket= UtilityForCastorTest.getTicket()

# Information from RFIOTESTCONFIG

stagerHost=""
stagerPort=""
stagerSvcClass=""
stagerVersion=""
remoteHost=""
stageMap="no"
castorConf="no"

# Directories used

localDir="./tmpRfioTest"+ticket+"/"
remoteDir=""
castorDir=(UtilityForCastorTest.prepareCastorString())+"tmpRfioTest"+ticket+"/"


# Variables used for different enviroment

myTurl=""
myTag=""
caseScen=""



print "After the test, delete the different log files used in:"
print "- "+localDir
print "- "+castorDir
print "- REMOTE_HOST:/tmp"+localDir[1:]+" (with remoteHostSpecified in the RFIOTESTCONFIG)"
print


###################
#                 # 
#  GENERAL RFIO   # 
#                 #
###################


class RfioPreRequisitesCase(unittest.TestCase):
	def mainScenariumSetUp(self):
		assert (UtilityForCastorTest.checkUser != -1), "you don't have a valid castor directory"
		os.system("mkdir "+localDir)
		os.system("rfmkdir "+castorDir)
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

			global remoteDir
			remoteDir=remoteHost +":/tmp"+localDir[1:]
			os.system("rfmkdir "+remoteDir)
			
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







########################
#                      #
# RFIO WITHOUT CASTOR  #
#                      #
########################


### RFCP ###


class RfioRfcpSimpleCase(unittest.TestCase):
	def localToLocal(self):
		cmd=caseScen
		cmd=["rfcp /etc/group "+localDir+"fileRfio1"+ticket,"rfcp "+localDir+"fileRfio1"+ticket+" "+localDir+"fileRfio1Copy"+ticket,"diff /etc/group "+localDir+"fileRfio1Copy"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"RfioSimpleLocalToLocal",cmd)
                fi=open(localDir+"RfioSimpleLocalToLocal","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and local (out)") != -1, "rfio doesn't work local to local"
                fi=open(localDir+"RfioSimpleLocalToLocal1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and local (out)") != -1, "rfio doesn't work local to local"

        	assert os.stat(localDir+"RfioSimpleLocalToLocal2")[6] == 0, "rfio doesn't work local to local"
                assert os.stat(localDir+"fileRfio1Copy"+ticket)[6] != 0, "rfio doesn't work local to local"

        def localToRemote(self):
                cmd=[caseScen+"rfcp /etc/group "+remoteDir+"localToRemote1"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"RfioSimpleLocalToRemote",cmd)
                fi=open(localDir+"RfioSimpleLocalToRemote","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1, "rfio doesn't work from local to remote"
        def remoteToLocal(self):
                cmd=["rfcp /etc/group "+remoteDir+"localToRemote2"+ticket,"rfcp "+remoteDir+"localToRemote2"+ticket+" "+localDir+"fileRfio3"+ticket,"diff /etc/group "+localDir+"fileRfio3"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"RfioSimpleRemoteToLocal",cmd)
                
                fi=open(localDir+"RfioSimpleRemoteToLocal","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1, "rfio doesn't work from local to remote"

                fi=open(localDir+"RfioSimpleRemoteToLocal1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and local (out)") != -1, "rfio doesn't work from  remote  to local"

                assert os.stat(localDir+"fileRfio3"+ticket)[6] != 0, "rfio doesn't work from remote to local"
                assert os.stat(localDir+"RfioSimpleRemoteToLocal2")[6] == 0, "rfio doesn't work from remote to local"
                
        def remoteToRemote(self):
                cmd=["rfcp /etc/group "+remoteDir+"remoteToRemote1"+ticket,"rfcp "+remoteDir+"remoteToRemote1"+ticket+" "+remoteDir+"remoteToRemote2"+ticket,"rfcp "+remoteDir+"remoteToRemote2"+ticket+" "+localDir+"fileRfio4"+ticket,"diff /etc/group "+localDir+"fileRfio4"+ticket]

		UtilityForCastorTest.saveOnFile(localDir+"RfioSimpleRemoteToRemote",cmd)
                
                fi=open(localDir+"RfioSimpleRemoteToRemote","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1, "rfio doesn't work from local to remote"

                fi=open(localDir+"RfioSimpleRemoteToRemote1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and eth0 (out)") != -1, "rfio doesn't work from  remote  to remote"

                fi=open(localDir+"RfioSimpleRemoteToRemote2","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and local (out)") != -1, "rfio doesn't work from  remote  to local"

                assert os.stat(localDir+"fileRfio4"+ticket)[6] != 0, "rfio doesn't work from remote to remote"
                assert os.stat(localDir+"RfioSimpleRemoteToRemote3")[6] == 0, "rfio doesn't work from remote to remote"
                

### OTHER CMD RFIO ###


class RfioOtherCmdSimpleCase(unittest.TestCase):
	def localRfcat(self):
	        cmd=["rfcat /etc/group"]
		UtilityForCastorTest.saveOnFile(localDir+"LocalRfcat"+myTag,cmd,caseScen)
                fi=open("/etc/group","r")
                buffOut=fi.read()
                fi.close()
		fi=open(localDir+"LocalRfcat"+myTag,"r")
                buffOut2=fi.read()
                fi.close()
        	assert buffOut2.find(buffOut) != -1, "rfcat is not working for local files"
                
	def remoteRfcat(self):
		cmd=["rfcp /etc/group "+remoteDir+"fileRfcatRemote"+ticket,"rfcat "+remoteDir+"fileRfcatRemote"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"RemoteRfcat"+myTag,cmd,caseScen)
		
                fi=open(localDir+"RemoteRfcat"+myTag+"1","r")
                buffOut=fi.read()
                fi.close()

		fi=open("/etc/group","r")
                buffOut2=fi.read()
                fi.close()

                assert buffOut2.find(buffOut) != -1, "rfcat is not working for local files"
	
	def localRfchmod(self):
		cmd=["rfcp /etc/group "+localDir+"fileRfchmodLocal"+ticket,"ls -l "+localDir+"fileRfchmodLocal"+ticket,"rfchmod 777 "+localDir+"fileRfchmodLocal"+ticket,"ls -l "+localDir+"fileRfchmodLocal"+ticket,"rfchmod 600 "+localDir+"fileRfchmodLocal"+ticket,"ls -l "+localDir+"fileRfchmodLocal"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"LocalRfchmod"+myTag,cmd,caseScen)
		
                fi=open(localDir+"LocalRfchmod"+myTag+"1","r")
		buffOut=fi.read()
		fi.close()
                assert buffOut.find("-rw-r--r--")!= -1, "Rfchmod doesn't work with local files"

                fi=open(localDir+"LocalRfchmod"+myTag+"3","r")
		buffOut=fi.read()
		fi.close()
                assert buffOut.find("-rwxrwxrwx") != -1, "Rfchmod doesn't work with local files"
                

                fi=open(localDir+"LocalRfchmod"+myTag+"5","r")
		buffOut=fi.read()
		fi.close()
                assert buffOut.find("-rw-------") != -1 , "Rfchmod doesn't work with local files"
		
	def remoteRfchmod(self):
		cmd=["rfcp /etc/group "+remoteDir+"fileRfchmodRemote"+ticket,"rfdir "+remoteDir+"fileRfchmodRemote"+ticket,"rfchmod 777 "+remoteDir+"fileRfchmodRemote"+ticket,"rfdir "+remoteDir+"fileRfchmodRemote"+ticket,"rfchmod 600 "+remoteDir+"fileRfchmodRemote"+ticket,"rfdir "+remoteDir+"fileRfchmodRemote"+ticket]
		
		UtilityForCastorTest.saveOnFile(localDir+"RemoteRfchmod"+myTag,cmd,caseScen)
		
                fi=open(localDir+"RemoteRfchmod"+myTag+"1","r")
		buffOut= fi.read()
		fi.close()
                assert buffOut.find("-rw-r--r--")!= -1, "Rfchmod doesn't work with remote files"
                

                fi=open(localDir+"RemoteRfchmod"+myTag+"3","r")
		buffOut=fi.read()
		fi.close()
                assert buffOut.find("-rwxrwxrwx") != -1, "Rfchmod doesn't work with remote files"
               
                fi=open(localDir+"RemoteRfchmod"+myTag+"5","r")
		buffOut=fi.read()
		fi.close()
                assert buffOut.find("-rw-------") != -1 , "Rfchmod doesn't work with remote files"
                

	def localRfdir(self):
		cmd=["rfdir "+localDir,"ls "+localDir]
	        UtilityForCastorTest.saveOnFile(localDir+"localRfdir"+myTag,cmd,caseScen)
		
                fi=open(localDir+"localRfdir"+myTag,"r")
	        buffOut=fi.read()
                fi.close()
                fi=open(localDir+"localRfdir"+myTag+"1","r")
	        buffOut2=fi.read()
                fi.close()
		assert buffOut.find(buffOut2), "rfdir is not working with local dir"
		assert buffOut2.find(buffOut), "rfdir is not working with local dir"
		
	def remoteRfdir(self):
		cmd=["rfdir "+remoteDir]
	
	        UtilityForCastorTest.saveOnFile(localDir+"RemoteRfdir"+myTag,cmd,caseScen)
		
                fi=open(localDir+"RemoteRfdir"+myTag,"r")
		buffOut=fi.read()
		fi.close()
	        assert buffOut.find("error") == -1, "rfdir is not working with remote dir"  

			
	def localRfmkdir(self):
		cmd=["rfmkdir "+localDir+"fileLocalRfmkdir"+ticket,"ls "+localDir+"fileLocalRfmkdir"+ticket] 
		UtilityForCastorTest.saveOnFile(localDir+"LocalRfmkdir"+myTag,cmd,caseScen)
		
                fi=open(localDir+"LocalRfmkdir"+myTag+"1","r")
		buffOut=fi.read()
		fi.close()
                assert buffOut.find("No such file or directory") == -1, "rfmkdir is not working for local files"
		
	def remoteRfmkdir(self):
	        cmd=["rfmkdir "+remoteDir+"fileRemoteRfmkdir"+ticket,"rfdir "+remoteDir+"fileRemoteRfmkdir"+ticket] 
		UtilityForCastorTest.saveOnFile(localDir+"RemoteRfmkdir"+myTag,cmd,caseScen)
		
                fi=open(localDir+"RemoteRfmkdir"+myTag+"1","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory") == -1, "rfmkdir is not working for remote files"
		
		
	def localRfrename(self):
		cmd=["rfcp /etc/group "+localDir+"fileLocalRfrename"+ticket,"rfrename "+localDir+"fileLocalRfrename"+ticket+"  "+localDir+"fileLocalRfrename2"+ticket,"ls  "+localDir+"fileLocalRfrename"+ticket,"ls "+localDir+"fileLocalRfrename2"+ticket] 
		UtilityForCastorTest.saveOnFile(localDir+"LocalRfrename"+myTag,cmd,caseScen)
		
                fi=open(localDir+"LocalRfrename"+myTag+"2","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory")!= -1, "rfrename doesn't work with local files"
		fi.close()
		
                fi=open(localDir+"LocalRfrename"+myTag+"3","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory") == -1, "rfrename doesn't work with local files"
	

	def remoteRfrename(self):
		cmd=["rfcp /etc/group "+remoteDir+"fileRemoteRfrename"+ticket,"rfrename "+remoteDir+"fileRemoteRfrename"+ticket+"  "+remoteDir+"fileRemoteRfrename2"+ticket,"rfdir  "+remoteDir+"fileRemoteRfrename"+ticket,"rfdir "+remoteDir+"fileRemoteRfrename2"+ticket] 
		UtilityForCastorTest.saveOnFile(localDir+"RemoteRfrename"+myTag,cmd,caseScen)
		
                fi=open(localDir+"RemoteRfrename"+myTag+"2","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory")!= -1, "rfrename doesn't work with local files"
		
                fi=open(localDir+"RemoteRfrename"+myTag+"3","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory") == -1, "rfrename doesn't work with local files"
		


	def localRfrm(self):
	        cmd=["rfcp /etc/group "+localDir+"fileLocalRfrm"+ticket,"ls "+localDir+"fileLocalRfrm"+ticket,"rfrm "+localDir+"fileLocalRfrm"+ticket,"ls "+localDir+"fileLocalRfrm"+ticket] 
		UtilityForCastorTest.saveOnFile(localDir+"LocalRfrm"+myTag,cmd,caseScen)
		
                fi=open(localDir+"LocalRfrm"+myTag+"1","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory") == -1, "rfrm doesn't work with local files" 

		fi=open(localDir+"LocalRfrm"+myTag+"3","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory") != -1, "rfrm doesn't work with local files" 

	def remoteRfrm(self):
		
                cmd=["rfcp /etc/group "+remoteDir+"fileRemoteRfrm"+ticket,"rfdir "+remoteDir+"fileRemoteRfrm"+ticket,"rfrm "+remoteDir+"fileRemoteRfrm"+ticket,"rfdir "+remoteDir+"fileRemoteRfrm"+ticket] 
		UtilityForCastorTest.saveOnFile(localDir+"RemoteRfrm"+myTag,cmd,caseScen)
		
                fi=open(localDir+"RemoteRfrm"+myTag+"1","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory") == -1, "rfrm doesn't work with remote files" 

		fi=open(localDir+"RemoteRfrm"+myTag+"3","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory") != -1, "rfrm doesn't work with remote files" 
                fi.close()
		
	def localRfstat(self):

		cmd=["rfcp /etc/group "+localDir+"fileLocalRfstat"+ticket,"rfstat "+localDir+"fileLocalRfstat"+ticket] 
		UtilityForCastorTest.saveOnFile(localDir+"LocalRfstat"+myTag,cmd,caseScen)
		
                fi=open(localDir+"LocalRfstat"+myTag+"1","r")
                buffOut=fi.read()
                fi.close()
                assert buffOut.find("Device") != -1, "rfstat is not working for local files"
	        assert buffOut.find("Inode number") != -1, "rfstat is not working for local files"
		assert buffOut.find("Nb blocks") != -1, "rfstat is not working for local files"
		assert buffOut.find("Protection") != -1, "rfstat is not working for local files"
		assert buffOut.find("Hard Links") != -1, "rfstat is not working for local files"
		assert buffOut.find("Uid") != -1, "rfstat is not working for local files"
		assert buffOut.find("Gid") != -1, "rfstat is not working for local files"
		assert buffOut.find("Size (bytes) ") != -1, "rfstat is not working for local files"
		assert buffOut.find("Last access") != -1, "rfstat is not working for local files"
		assert buffOut.find("Last modify") != -1, "rfstat is not working for local files"
                assert buffOut.find("Last stat. mod.") != -1, "rfstat is not working for local files"


	def remoteRfstat(self):
		cmd=["rfcp /etc/group "+remoteDir+"fileRemoteRfstat"+ticket,"rfstat "+remoteDir+"fileRemoteRfstat"+ticket] 
		UtilityForCastorTest.saveOnFile(localDir+"RemoteRfstat"+myTag,cmd,caseScen)
		
                fi=open(localDir+"RemoteRfstat"+myTag+"1","r")
                buffOut=fi.read()
                fi.close()
                assert buffOut.find("Device") != -1, "rfstat is not working for remote files"
	        assert buffOut.find("Inode number") != -1, "rfstat is not working for remote files"
		assert buffOut.find("Nb blocks") != -1, "rfstat is not working for remote files"
		assert buffOut.find("Protection") != -1, "rfstat is not working for remote files"
		assert buffOut.find("Hard Links") != -1, "rfstat is not working for remote files"
		assert buffOut.find("Uid") != -1, "rfstat is not working for remote files"
		assert buffOut.find("Gid") != -1, "rfstat is not working for remote files"
		assert buffOut.find("Size (bytes) ") != -1, "rfstat is not working for remote files"
		assert buffOut.find("Last access") != -1, "rfstat is not working for remote files"
		assert buffOut.find("Last modify") != -1, "rfstat is not working for remote files"
                assert buffOut.find("Last stat. mod.") != -1, "rfstat is not working for remote files"
#	def localRftp(self):

#	def remoteRftp(self):




    ####################
    #                  # 
    # RFIO WITH CASTOR #
    #	               #
    ####################


# Scenaria Castor  # 
    
class RfioCastorScenarium1(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Env1"
	    global myTurl
	    myTurl=castorDir
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(-1,-1,-1,-1)
    
class RfioCastorScenarium2(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Env2"
	    global myTurl
	    myTurl=castorDir
	    global caseScen
	    caseScen=UtilityForCastorTest.createScenarium(stagerHost,-1,-1,stagerVersion)
            
class RfioCastorScenarium3(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Env3"
	    global myTurl
	    myTurl=castorDir
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(stagerHost,stagerPort,-1,stagerVersion)


class RfioCastorScenarium4(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Env4"
	    global myTurl
	    myTurl=castorDir
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(stagerHost,-1,stagerSvcClass,stagerVersion)

class RfioCastorScenarium5(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Env5"
	    global myTurl
	    myTurl=castorDir
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerSvcClass,stagerVersion)
	    
class RfioCastorFancyTurl1(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl1"
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerSvcClass,stagerVersion)
            global myTurl 
	    myTurl="rfio:///"+castorDir


class RfioCastorFancyTurl2(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl2"
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerSvcClass,stagerVersion)
            global myTurl
	    myTurl="rfio://"+castorDir

            
class RfioCastorFancyTurl3(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl3"
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerSvcClass,stagerVersion)
            global myTurl
	    myTurl="rfio:///castor\\?path="+castorDir
  


class RfioCastorFancyTurl4(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl4"
            global caseScen
            caseScen=UtilityForCastorTest.createScenarium(-1,stagerPort,stagerSvcClass,stagerVersion)
            global myTurl
	    myTurl="rfio://"+stagerHost+"/castor\\?path="+castorDir

            
class RfioCastorFancyTurl5(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl5"
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(-1,stagerPort,stagerSvcClass,stagerVersion)
            global myTurl
	    myTurl="rfio://"+stagerHost+"//castor\\?path="+castorDir

            
class RfioCastorFancyTurl6(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl6"
            UtilityForCastorTest.createScenarium(-1,-1,stagerSvcClass,stagerVersion)
            global myTurl
	    myTurl="rfio://"+stagerHost+":"+stagerPort+"/castor\\?path="+castorDir
          
            
class RfioCastorFancyTurl7(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl7"
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(-1,stagerPort,stagerSvcClass,stagerVersion)
            global myTurl
	    myTurl="rfio://"+stagerHost+"/castor\\?path="+castorDir
 

class RfioCastorFancyTurl8(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl8"
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(-1,-1,stagerSvcClass,stagerVersion)
            global myTurl
	    myTurl="rfio://"+stagerHost+":"+stagerPort+"/castor\\?path="+castorDir
    
 
class RfioCastorFancyTurl9(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl9"
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(-1,-1,stagerSvcClass,stagerVersion)
            global myTurl
	    myTurl="rfio://"+stagerHost+":"+stagerPort+"/"+castorDir

 
class RfioCastorFancyTurl10(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl10"
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(-1,-1,stagerSvcClass,stagerVersion)
            global myTurl
	    myTurl="rfio://"+stagerHost+":"+stagerPort+castorDir

class RfioCastorFancyTurl11(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl11"
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(-1,-1,-1,-1)
            global myTurl
	    myTurl="rfio://"+stagerHost+":"+stagerPort+"/castor\\?svcClass="+stagerSvcClass+"\\&castorVersion="+stagerVersion+"\\&path="+castorDir





         
###  RFCP   ###



class RfioRfcpCastor(unittest.TestCase):

        def localToCastor(self):
	        cmd=["rfcp /etc/group "+myTurl+"fileLocalToCastor"+myTag+ticket]
	
		UtilityForCastorTest.saveOnFile(localDir+"RfioRfcpLocalToCastor"+myTag,cmd,caseScen)	
		
                fi=open(localDir+"RfioRfcpLocalToCastor"+myTag,"r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from local to castor"
                
	def castorToLocal(self):
                cmd=["rfcp /etc/group "+myTurl+"fileCastorToLocal1"+myTag+ticket,"rfcp "+myTurl+"fileCastorToLocal1"+myTag+ticket+" "+localDir+"fileCastorToLocal2"+myTag+ticket,"diff /etc/group "+localDir+"fileCastorToLocal2"+myTag+ticket]
	
		UtilityForCastorTest.saveOnFile(localDir+"RfioRfcpCastorToLocal"+myTag,cmd,caseScen)
		
                
                fi=open(localDir+"RfioRfcpCastorToLocal"+myTag,"r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from local to castor"

                fi=open(localDir+"RfioRfcpCastorToLocal"+myTag+"1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and local (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from  castor to local"

                assert os.stat(localDir+"fileCastorToLocal2"+myTag+ticket)[6] != 0, "rfio doesn't work from castor to local"
                assert os.stat(localDir+"RfioRfcpCastorToLocal"+myTag+"2")[6] == 0, "rfio doesn't work from castor to local"

        def remoteToCastor(self):
                cmd=["rfcp /etc/group "+remoteDir+"fileRemoteToCastor1"+myTag+ticket,"rfcp "+remoteDir+"fileRemoteToCastor1"+myTag+ticket+" "+myTurl+"fileRemoteToCastor1"+myTag+ticket,"rfcp "+myTurl+"fileRemoteToCastor1"+myTag+ticket+" "+localDir+"fileRemoteToCastor"+myTag+ticket,"diff /etc/group "+localDir+"fileRemoteToCastor"+myTag+ticket]

		UtilityForCastorTest.saveOnFile(localDir+"RfioRfcpRemoteToCastor"+myTag,cmd,caseScen)
                
                fi=open(localDir+"RfioRfcpRemoteToCastor"+myTag,"r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from local to remote"

                fi=open(localDir+"RfioRfcpRemoteToCastor"+myTag+"1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and eth0 (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from  remote  to castor"

                fi=open(localDir+"RfioRfcpRemoteToCastor"+myTag+"2","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and local (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from  castor  to local"

                assert os.stat(localDir+"fileRemoteToCastor"+myTag+ticket)[6] != 0, "rfio doesn't work from remote to castor"
                assert os.stat(localDir+"RfioRfcpRemoteToCastor"+myTag+"3")[6] == 0, "rfio doesn't work from remote to castor"
                
        def castorToRemote(self):
                
		cmd=["rfcp /etc/group "+myTurl+"fileCastorToRemote1"+myTag+ticket,"rfcp "+myTurl+"fileCastorToRemote1"+myTag+ticket+" "+remoteDir+"fileCastorToRemote2"+myTag+ticket,"rfcp "+remoteDir+"fileCastorToRemote2"+myTag+ticket+" "+localDir+"fileCastorToRemote"+myTag+ticket,"diff /etc/group "+localDir+"fileCastorToRemote"+myTag+ticket]

		UtilityForCastorTest.saveOnFile(localDir+"RfioRfcpCastorToRemote"+myTag,cmd,caseScen)
                
                fi=open(localDir+"RfioRfcpCastorToRemote"+myTag,"r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1 , "rfio doesn't work from local to castor"

                fi=open(localDir+"RfioRfcpCastorToRemote"+myTag+"1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and eth0 (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from  castor  to remote"

                fi=open(localDir+"RfioRfcpCastorToRemote"+myTag+"2","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and local (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1 , "rfio doesn't work from  remote  to local"

                assert os.stat(localDir+"fileCastorToRemote"+myTag+ticket)[6] != 0, "rfio doesn't work from castor to remote"
                assert os.stat(localDir+"RfioRfcpCastorToRemote"+myTag+"3")[6] == 0, "rfio doesn't work from castor to remote"

        def castorToCastor(self):
                
                cmd=["rfcp /etc/group "+myTurl+"fileCastorToCastor1"+myTag+ticket,"rfcp "+myTurl+"fileCastorToCastor1"+myTag+ticket+" "+myTurl+"fileCastorToCastor2"+myTag+ticket,"rfcp "+myTurl+"fileCastorToCastor2"+myTag+ticket+" "+localDir+"fileCastorToCastor"+myTag+ticket,"diff /etc/group "+localDir+"fileCastorToCastor"+myTag+ticket]
	
		UtilityForCastorTest.saveOnFile(localDir+"RfioRfcpCastorToCastor"+myTag,cmd,caseScen)

                fi=open(localDir+"RfioRfcpCastorToCastor"+myTag,"r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1 , "rfio doesn't work from local to castor"
                fi=open(localDir+"RfioRfcpCastorToCastor"+myTag+"1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and eth0 (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from  castor  to castor"

                fi=open(localDir+"RfioRfcpCastorToCastor"+myTag+"2","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and local (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from  castor  to local"

                assert os.stat(localDir+"fileCastorToCastor"+myTag+ticket)[6] != 0, "rfio doesn't work from castor to castor"
                assert os.stat(localDir+"RfioRfcpCastorToCastor"+myTag+"3")[6] == 0, "rfio doesn't work from castor to castor"

### Rfcp with different enviroment ###

class RfioRfcpScenarium1(RfioCastorScenarium1,RfioRfcpCastor):
        pass
    
class RfioRfcpScenarium2(RfioCastorScenarium2,RfioRfcpCastor):
        pass
            
class RfioRfcpScenarium3(RfioCastorScenarium3,RfioRfcpCastor):
        pass

class RfioRfcpScenarium4(RfioCastorScenarium4,RfioRfcpCastor):
	pass

class RfioRfcpScenarium5(RfioCastorScenarium5,RfioRfcpCastor):
        pass
	    
class RfioRfcpFancyTurl1(RfioCastorFancyTurl1,RfioRfcpCastor):
        pass

class RfioRfcpFancyTurl2(RfioCastorFancyTurl2,RfioRfcpCastor):
	pass

class RfioRfcpFancyTurl3(RfioCastorFancyTurl3,RfioRfcpCastor):
	pass

class RfioRfcpFancyTurl4(RfioCastorFancyTurl4,RfioRfcpCastor):
        pass
            
class RfioRfcpFancyTurl5(RfioCastorFancyTurl5,RfioRfcpCastor):
        pass
            
class RfioRfcpFancyTurl6(RfioCastorFancyTurl6,RfioRfcpCastor):
        pass
            
class RfioRfcpFancyTurl7(RfioCastorFancyTurl7,RfioRfcpCastor):
        pass

class RfioRfcpFancyTurl8(RfioCastorFancyTurl8,RfioRfcpCastor):
        pass
 
class RfioRfcpFancyTurl9(RfioCastorFancyTurl9,RfioRfcpCastor):
        pass
 
class RfioRfcpFancyTurl10(RfioCastorFancyTurl10,RfioRfcpCastor):
        pass
class RfioRfcpFancyTurl11(RfioCastorFancyTurl11,RfioRfcpCastor):
        pass


### Other Cmd ####

class RfioOtherCmdCastor(unittest.TestCase):
	def castorRfcat(self):
		cmd=["rfcp /etc/group "+myTurl+"fileRfcatCastor"+myTag+ticket,"rfcat "+myTurl+"fileRfcatCastor"+myTag+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"CastorRfcat"+myTag,cmd,caseScen)
		
                fi=open(localDir+"CastorRfcat"+myTag+"1","r")
                buffOut=fi.read()
                fi.close()

		fi=open("/etc/group","r")
                buffOut2=fi.read()
                fi.close()

                assert buffOut.find(buffOut2) != -1, "rfcat is not working for castor files"
		
	def castorRfchmod(self):
		cmd=["rfcp /etc/group "+myTurl+"fileRfchmodCastor"+myTag+ticket,"nsls -l "+castorDir+"fileRfchmodCastor"+myTag+ticket,"rfchmod 777 "+myTurl+"fileRfchmodCastor"+myTag+ticket,"nsls -l "+castorDir+"fileRfchmodCastor"+myTag+ticket,"rfchmod 600 "+myTurl+"fileRfchmodCastor"+myTag+ticket,"nsls -l "+castorDir+"fileRfchmodCastor"+myTag+ticket]
		
		UtilityForCastorTest.saveOnFile(localDir+"CastorRfchmod"+myTag,cmd,caseScen)
		
                fi=open(localDir+"CastorRfchmod"+myTag+"1","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("-rw-r--r--")!= -1, "Rfchmod doesn't work with castor files"

                fi=open(localDir+"CastorRfchmod"+myTag+"3","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("-rwxrwxrwx") != -1, "Rfchmod doesn't work with castor files"

                fi=open(localDir+"CastorRfchmod"+myTag+"5","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("-rw-------") != -1 , "Rfchmod doesn't work with castor files"
                
		
	def castorRfdir(self):
	        cmd=["rfdir "+myTurl,"nsls -l"+castorDir]
		
	        UtilityForCastorTest.saveOnFile(localDir+"CastorRfdir"+myTag,cmd,caseScen)
		
                fi=open(localDir+"CastorRfdir"+myTag,"r")
	        buffOut=fi.read()
                fi.close()
                fi=open(localDir+"CastorRfdir"+myTag+"1","r")
	        buffOut2=fi.read()
                fi.close()
		assert buffOut.find(buffOut2), "rfdir is not working with remote dir"
		assert buffOut2.find(buffOut), "rfdir is not working with remote dir"
		

	def castorRfmkdir(self):
	        cmd=["rfmkdir "+myTurl+"fileCastorRfmkdir"+myTag+ticket,"nsls "+castorDir+"fileCastorRfmkdir"+myTag+ticket]
		
		UtilityForCastorTest.saveOnFile(localDir+"CastorRfmkdir"+myTag,cmd,caseScen)
		
                fi=open(localDir+"CastorRfmkdir"+myTag+"1","r")
		buffOut=fi.read()
		fi.close()
		
	        assert buffOut.find("No such file or directory") == -1, "rfmkdir is not working for castor files"
		
		
	def castorRfrename(self):
		cmd=["rfcp /etc/group "+myTurl+"fileCastorRfrename"+myTag+ticket,"rfrename "+myTurl+"fileCastorRfrename"+myTag+ticket+"  "+myTurl+"fileCastorRfrename2"+myTag+ticket,"nsls  "+castorDir+"fileCastorRfrename"+myTag+ticket,"nsls "+castorDir+"fileCastorRfrename2"+myTag+ticket] 
		
		UtilityForCastorTest.saveOnFile(localDir+"CastorRfrename"+myTag,cmd,caseScen)
		
                fi=open(localDir+"CastorRfrename"+myTag+"2","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory")!= -1, "rfrename doesn't work with local files"
		
		
                fi=open(localDir+"CastorRfrename"+myTag+"3","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory") == -1, "rfrename doesn't work with local files"
		

	def castorRfrm(self):
                cmd=["rfcp /etc/group "+myTurl+"fileCastorRfrm"+myTag+ticket,"nsls "+castorDir+"fileCastorRfrm"+myTag+ticket,"rfrm "+myTurl+"fileCastorRfrm"+myTag+ticket,"nsls "+castorDir+"fileCastorRfrm"+myTag+ticket] 
		
		UtilityForCastorTest.saveOnFile(localDir+"CastorRfrm"+myTag,cmd,caseScen)
		
                fi=open(localDir+"CastorRfrm"+myTag+"1","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory") == -1, "rfrm doesn't work with castor files" 

		fi=open(localDir+"CastorRfrm"+myTag+"3","r")
	        buffOut=fi.read()
		fi.close()
                assert buffOut.find("No such file or directory") != -1, "rfrm doesn't work with castor files"
		
                
        def castorRfstat(self):
	        cmd=["rfcp /etc/group "+myTurl+"fileCastorRfstat"+myTag+ticket,"rfstat "+myTurl+"fileCastorRfstat"+myTag+ticket]
		
		UtilityForCastorTest.saveOnFile(localDir+"CastorRfstat"+myTag,cmd,caseScen)
		
                fi=open(localDir+"CastorRfstat"+myTag+"1","r")
                buffOut=fi.read()
                fi.close()
                assert buffOut.find("Device") != -1, "rfstat is not working for castor files"
	        assert buffOut.find("Inode number") != -1, "rfstat is not working for castor files"
		assert buffOut.find("Nb blocks") != -1, "rfstat is not working for castor files"
		assert buffOut.find("Protection") != -1, "rfstat is not working for castor files"
		assert buffOut.find("Hard Links") != -1, "rfstat is not working for castor files"
		assert buffOut.find("Uid") != -1, "rfstat is not working for castor files"
		assert buffOut.find("Gid") != -1, "rfstat is not working for castor files"
		assert buffOut.find("Size (bytes) ") != -1, "rfstat is not working for castor files"
		assert buffOut.find("Last access") != -1, "rfstat is not working for castor files"
		assert buffOut.find("Last modify") != -1, "rfstat is not working for castor files"
                assert buffOut.find("Last stat. mod.") != -1, "rfstat is not working for castor files"
		
#	def castorRftp(self):

### other cmd with different enviroment ###
 
class RfioOtherCmdScenarium1(RfioCastorScenarium1,RfioOtherCmdCastor):
        pass
    
class RfioOtherCmdScenarium2(RfioCastorScenarium2,RfioOtherCmdCastor,):
        pass
            
class RfioOtherCmdScenarium3(RfioCastorScenarium3,RfioOtherCmdCastor):
        pass

class RfioOtherCmdScenarium4(RfioCastorScenarium4,RfioOtherCmdCastor):
	pass

class RfioOtherCmdScenarium5(RfioCastorScenarium5,RfioOtherCmdCastor):
        pass
	    
class RfioOtherCmdFancyTurl1(RfioCastorFancyTurl1,RfioOtherCmdCastor):
        pass

class RfioOtherCmdFancyTurl2(RfioCastorFancyTurl2,RfioOtherCmdCastor):
	pass

class RfioOtherCmdFancyTurl3(RfioCastorFancyTurl3,RfioOtherCmdCastor):
	pass

class RfioOtherCmdFancyTurl4(RfioCastorFancyTurl4,RfioOtherCmdCastor):
        pass
            
class RfioOtherCmdFancyTurl5(RfioCastorFancyTurl5,RfioOtherCmdCastor):
        pass
            
class RfioOtherCmdFancyTurl6(RfioCastorFancyTurl6,RfioOtherCmdCastor):
        pass
            
class RfioOtherCmdFancyTurl7(RfioCastorFancyTurl7,RfioOtherCmdCastor):
        pass

class RfioOtherCmdFancyTurl8(RfioCastorFancyTurl8,RfioOtherCmdCastor):
        pass
 
class RfioOtherCmdFancyTurl9(RfioCastorFancyTurl9,RfioOtherCmdCastor):
        pass
 
class RfioOtherCmdFancyTurl10(RfioCastorFancyTurl10,RfioOtherCmdCastor):
        pass

class RfioOtherCmdFancyTurl11(RfioCastorFancyTurl11,RfioOtherCmdCastor):
        pass




##########################################################################################################
##########################################################################################################

casesPreReq=("mainScenariumSetUp","stageMapSituation","castorConfSituation")
casesRfcpSimple=("localToLocal","localToRemote","remoteToLocal","remoteToRemote")
casesOtherCmdSimple=("localRfcat","remoteRfcat","localRfchmod","remoteRfchmod","localRfdir","remoteRfdir","localRfmkdir","remoteRfmkdir","localRfrename","remoteRfrename","localRfrm","remoteRfrm","localRfstat","remoteRfstat")
casesRfcpCastor=("localToCastor","castorToLocal","remoteToCastor","castorToRemote","castorToCastor")
casesOtherCmdCastor=("castorRfcat","castorRfchmod","castorRfdir","castorRfmkdir","castorRfrename","castorRfrm","castorRfstat")


## PREREQ AND SETTING

class RfioPreRequisitesSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioPreRequisitesCase,casesPreReq))
	

##  BASIC TEST RFIO

class RfioRfcpSimpleSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpSimpleCase,casesRfcpSimple))

class RfioOtherCmdSimpleSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdSimpleCase,casesOtherCmdSimple))
	
	
## DIFFERENT ENV POSSIBILITY FOR CASTOR

# Rfcp

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


# Other cmd

class RfioOtherCmdEnv1Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdScenarium1,casesOtherCmdCastor))
class RfioOtherCmdEnv2Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdScenarium2,casesOtherCmdCastor))

class RfioOtherCmdEnv3Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdScenarium3,casesOtherCmdCastor))

class RfioOtherCmdEnv4Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdScenarium4,casesOtherCmdCastor))
        
class RfioOtherCmdEnv5Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdScenarium5,casesOtherCmdCastor))
	

###  BIG TEST SUITE WITH ALL ENV CASES


class RfioRfcpEnvSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,(RfioRfcpEnv1Suite(),RfioRfcpEnv2Suite(),RfioRfcpEnv3Suite(),RfioRfcpEnv4Suite(),RfioRfcpEnv5Suite()))

class RfioOtherCmdEnvSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,(RfioOtherCmdEnv1Suite(),RfioOtherCmdEnv2Suite(),RfioOtherCmdEnv3Suite(),RfioOtherCmdEnv4Suite(),RfioOtherCmdEnv5Suite()))

## FANCY TURL POSSIBILITY

# rfcp

class RfioRfcpFancyTurl1Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpFancyTurl1,casesRfcpCastor))

class  RfioRfcpFancyTurl2Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpFancyTurl2,casesRfcpCastor))

class RfioRfcpFancyTurl3Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpFancyTurl3,casesRfcpCastor))

class  RfioRfcpFancyTurl4Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpFancyTurl4,casesRfcpCastor))
        
class  RfioRfcpFancyTurl5Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpFancyTurl5,casesRfcpCastor))

class  RfioRfcpFancyTurl6Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpFancyTurl6,casesRfcpCastor))

class  RfioRfcpFancyTurl7Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpFancyTurl7,casesRfcpCastor))

class  RfioRfcpFancyTurl8Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpFancyTurl8,casesRfcpCastor))

class  RfioRfcpFancyTurl9Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpFancyTurl9,casesRfcpCastor))

class  RfioRfcpFancyTurl10Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpFancyTurl10,casesRfcpCastor))

class  RfioRfcpFancyTurl11Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpFancyTurl11,casesRfcpCastor))

# other cmd

class RfioOtherCmdFancyTurl1Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdFancyTurl1,casesOtherCmdCastor))
	
class RfioOtherCmdFancyTurl2Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdFancyTurl2,casesOtherCmdCastor))
	
class RfioOtherCmdFancyTurl3Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdFancyTurl3,casesOtherCmdCastor))
	
class RfioOtherCmdFancyTurl4Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdFancyTurl4,casesOtherCmdCastor))

class RfioOtherCmdFancyTurl5Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdFancyTurl5,casesOtherCmdCastor))

class RfioOtherCmdFancyTurl6Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdFancyTurl6,casesOtherCmdCastor))

class RfioOtherCmdFancyTurl7Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdFancyTurl7,casesOtherCmdCastor))

class RfioOtherCmdFancyTurl8Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdFancyTurl8,casesOtherCmdCastor))

class RfioOtherCmdFancyTurl9Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdFancyTurl9,casesOtherCmdCastor))

class RfioOtherCmdFancyTurl10Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdFancyTurl10,casesOtherCmdCastor))
	
class RfioOtherCmdFancyTurl11Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdFancyTurl11,casesOtherCmdCastor))



###  BIG TEST SUITE WITH ALL FANCY TURL CASES

class RfioRfcpFancyTurlSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,(RfioRfcpFancyTurl1Suite(),RfioRfcpFancyTurl2Suite(),RfioRfcpFancyTurl3Suite(),RfioRfcpFancyTurl4Suite(),RfioRfcpFancyTurl5Suite(),RfioRfcpFancyTurl6Suite(),RfioRfcpFancyTurl7Suite(),RfioRfcpFancyTurl8Suite(),RfioRfcpFancyTurl9Suite(),RfioRfcpFancyTurl10Suite(),RfioRfcpFancyTurl11Suite()))

class RfioOtherCmdFancyTurlSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,(RfioOtherCmdFancyTurl1Suite(),RfioOtherCmdFancyTurl2Suite(),RfioOtherCmdFancyTurl3Suite(),RfioOtherCmdFancyTurl4Suite(),RfioOtherCmdFancyTurl5Suite(),RfioOtherCmdFancyTurl6Suite(),RfioOtherCmdFancyTurl7Suite(),RfioOtherCmdFancyTurl8Suite(),RfioOtherCmdFancyTurl9Suite(),RfioOtherCmdFancyTurl10Suite(),RfioOtherCmdFancyTurl11Suite()))
