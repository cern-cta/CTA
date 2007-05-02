import unittest
import UtilityForCastorTest
import os
import sys
import time
import threading

# Information to access Castor

stagerHost=""
stagerPort=""
stagerSvcClass=""
stagerVersion=""
(stagerHost,stagerPort,stagerSvcClass,stagerVersion)=UtilityForCastorTest.getCastorParameters()

#other parameters

remoteHost=""
stageMap="no"
castorConf="no"
castorEnv="no"
ticket= UtilityForCastorTest.getTicket()

# Variables used for different enviroment

myTurl=""
myTag=""
caseScen=""

# files and directories

inputFile=""
localDir="" 
remoteDir=""
castorDir=(UtilityForCastorTest.prepareCastorString())+"tmpRfioTest"+ticket+"/"
inputStressFiles=""



###################
#                 # 
#  GENERAL RFIO   # 
#                 #
###################


class RfioPreRequisitesCase(unittest.TestCase):
	def mainScenariumSetUp(self):
		assert (UtilityForCastorTest.checkUser != -1), "you don't have a valid castor directory"
		try:
			global castorDir
		        os.system("rfmkdir "+castorDir)
			
			f=open("./CASTORTESTCONFIG","r")
			configFileInfo=f.read()
			f.close

			index= configFileInfo.find("*** Rfio test specific parameters ***")
			configFileInfo=configFileInfo[index:]
			index=configFileInfo.find("***")
			if index != -1:
				index=index-1
				configFileInfo=configFileInfo[:index]

		        global inputFile
		 	inputFile=(configFileInfo[configFileInfo.find("INPUT_FILE"):]).split()[1]
			
                        global inputStressFiles
		 	inputStressFiles=UtilityForCastorTest.getListOfFiles((configFileInfo[configFileInfo.find("STRESS_INPUT_DIR"):]).split()[1])
			
			global localDir
		        localDir=(configFileInfo[configFileInfo.find("LOG_DIR"):]).split()[1]+ticket+"/"
			os.system("mkdir "+localDir)

		 	remoteHost=(configFileInfo[configFileInfo.find("REMOTE_HOST"):]).split()[1]

			global remoteDir
			remoteDir=(configFileInfo[configFileInfo.find("REMOTE_OUTPUT_DIR"):]).split()[1]
			remoteDir=remoteHost+":"+remoteDir+"/tmpRfio"+ticket
			os.system("rfmkdir "+remoteDir)
			
			
			global stageMap
		 	stageMap=((configFileInfo[configFileInfo.find("USE_STAGEMAP"):]).split()[1]).lower()

			global castorConf
		 	castorConf=((configFileInfo[configFileInfo.find("USE_CASTOR_CONF"):]).split()[1]).lower()
			global castorEnv
		 	castorEnv=((configFileInfo[configFileInfo.find("USE_CASTOR_ENV"):]).split()[1]).lower()
	        except IOError:
			assert 0==-1, "An error in the preparation of the main setting occurred ... test is not valid"
		
		
	def stageMapSituation(self):
		try:
		   f=open("/etc/castor/stagemap.conf","r")
		   buff=f.read()
		   f.close()
		   gid=UtilityForCastorTest.logGroup()
		   uid=UtilityForCastorTest.logUser()
		   if (buff.find("USTGMAP "+uid)!= -1 or buff.find("GSTGMAP "+gid) != -1):
			   if castorEnv == "no":
				   assert stageMap == "yes", "You might use values in /etc/castor/stagemap.conf even if you don't want." 
		   else:
			   assert stageMap == "no", "It might be not possible to use the stagemap.conf parameters."                	   		   
                except IOError:                     
                   assert stageMap == "no", "It is not possible to read /etc/castor/stagemap.conf."
		
	def castorConfSituation(self):
		try:
		   f=open("/etc/castor/castor.conf","r")
		   buff=f.read()
		   f.close()

		   if buff.find("#RH HOST") == -1  and  buff.find("RH HOST") != -1:
			   if  castorEnv == "no":
				   assert castorConf == "yes", "You might use value in /etc/castor/castor.conf even if you don't want."
		   else:
			   assert castorConf == "no", "It might be not possible to use the castor.conf parameters." 
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
		cmd=["rfcp "+inputFile+" "+localDir+"fileRfioLocalToLocal"+ticket,"rfcp "+localDir+"fileRfioLocalToLocal"+ticket+" "+localDir+"fileRfioLocalToLocalCopy"+ticket,"diff "+inputFile+" "+localDir+"fileRfioLocalToLocalCopy"+ticket]
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
                assert os.stat(localDir+"fileRfioLocalToLocalCopy"+ticket)[6] != 0, "rfio doesn't work local to local"

        def localToRemote(self):
                cmd=[caseScen+"rfcp "+inputFile+" "+remoteDir+"fileRfioLocalToRemote"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"RfioSimpleLocalToRemote",cmd)
                fi=open(localDir+"RfioSimpleLocalToRemote","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1, "rfio doesn't work from local to remote"
        def remoteToLocal(self):
                cmd=["rfcp "+inputFile+" "+remoteDir+"fileRfioRemoteToLocal"+ticket,"rfcp "+remoteDir+"fileRfioRemoteToLocal"+ticket+" "+localDir+"fileRfioRemoteToLocalCopy"+ticket,"diff "+inputFile+" "+localDir+"fileRfioRemoteToLocalCopy"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"RfioSimpleRemoteToLocal",cmd)
                
                fi=open(localDir+"RfioSimpleRemoteToLocal","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1, "rfio doesn't work from local to remote"

                fi=open(localDir+"RfioSimpleRemoteToLocal1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and local (out)") != -1, "rfio doesn't work from  remote  to local"

                assert os.stat(localDir+"fileRfioRemoteToLocalCopy"+ticket)[6] != 0, "rfio doesn't work from remote to local"
                assert os.stat(localDir+"RfioSimpleRemoteToLocal2")[6] == 0, "rfio doesn't work from remote to local"
                
        def remoteToRemote(self):
                cmd=["rfcp "+inputFile+" "+remoteDir+"fileRfioRemoteToRemote"+ticket,"rfcp "+remoteDir+"fileRfioRemoteToRemote"+ticket+" "+remoteDir+"fileRfioRemoteToRemoteCopy"+ticket,"rfcp "+remoteDir+"fileRfioRemoteToRemoteCopy"+ticket+" "+localDir+"fileRfioRemoteToRemoteCopyCopy"+ticket,"diff "+inputFile+" "+localDir+"fileRfioRemoteToRemoteCopyCopy"+ticket]

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

                assert os.stat(localDir+"fileRfioRemoteToRemoteCopyCopy"+ticket)[6] != 0, "rfio doesn't work from remote to remote"
                assert os.stat(localDir+"RfioSimpleRemoteToRemote3")[6] == 0, "rfio doesn't work from remote to remote"
                

### OTHER CMD RFIO ###


class RfioOtherCmdSimpleCase(unittest.TestCase):
	def localRfcat(self):
	        cmd=["rfcat "+inputFile]
		UtilityForCastorTest.saveOnFile(localDir+"RfioLocalRfcat",cmd,caseScen)
                fi=open(inputFile,"r")
                buffOut=fi.read()
                fi.close()
		fi=open(localDir+"RfioLocalRfcat","r")
                buffOut2=fi.read()
                fi.close()
        	assert buffOut2.find(buffOut) != -1, "rfcat is not working for local files"
                
	def remoteRfcat(self):
		cmd=["rfcp "+inputFile+" "+remoteDir+"fileRfioRfcatRemote"+ticket,"rfcat "+remoteDir+"fileRfioRfcatRemote"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"RfioRemoteRfcat",cmd,caseScen)
		
                fi=open(localDir+"RfioRemoteRfcat1","r")
                buffOut=fi.read()
                fi.close()

		fi=open(inputFile,"r")
                buffOut2=fi.read()
                fi.close()

                assert buffOut2.find(buffOut) != -1, "rfcat is not working for local files"
	
	def localRfchmod(self):
		cmd=["rfcp "+inputFile+" "+localDir+"fileRfioRfchmodLocal"+ticket,"ls -l "+localDir+"fileRfioRfchmodLocal"+ticket,"rfchmod 777 "+localDir+"fileRfioRfchmodLocal"+ticket,"ls -l "+localDir+"fileRfioRfchmodLocal"+ticket,"rfchmod 600 "+localDir+"fileRfioRfchmodLocal"+ticket,"ls -l "+localDir+"fileRfioRfchmodLocal"+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"RfioLocalRfchmod",cmd,caseScen)
		
                fi=open(localDir+"RfioLocalRfchmod1","r")
		buffOut=fi.read()
		fi.close()
                assert buffOut.find("-rw-r--r--")!= -1, "Rfchmod doesn't work with local files"

                fi=open(localDir+"RfioLocalRfchmod3","r")
		buffOut=fi.read()
		fi.close()
                assert buffOut.find("-rwxrwxrwx") != -1, "Rfchmod doesn't work with local files"
                

                fi=open(localDir+"RfioLocalRfchmod5","r")
		buffOut=fi.read()
		fi.close()
                assert buffOut.find("-rw-------") != -1 , "Rfchmod doesn't work with local files"
		
	def remoteRfchmod(self):
		cmd=["rfcp "+inputFile+" "+remoteDir+"fileRfioRfchmodRemote"+ticket,"rfdir "+remoteDir+"fileRfioRfchmodRemote"+ticket,"rfchmod 777 "+remoteDir+"fileRfioRfchmodRemote"+ticket,"rfdir "+remoteDir+"fileRfioRfchmodRemote"+ticket,"rfchmod 600 "+remoteDir+"fileRfioRfchmodRemote"+ticket,"rfdir "+remoteDir+"fileRfioRfchmodRemote"+ticket]
		
		UtilityForCastorTest.saveOnFile(localDir+"RfioRemoteRfchmod",cmd,caseScen)
		
                fi=open(localDir+"RfioRemoteRfchmod1","r")
		buffOut= fi.read()
		fi.close()
                assert buffOut.find("-rw-r--r--")!= -1, "Rfchmod doesn't work with remote files"
                

                fi=open(localDir+"RfioRemoteRfchmod3","r")
		buffOut=fi.read()
		fi.close()
                assert buffOut.find("-rwxrwxrwx") != -1, "Rfchmod doesn't work with remote files"
               
                fi=open(localDir+"RfioRemoteRfchmod5","r")
		buffOut=fi.read()
		fi.close()
                assert buffOut.find("-rw-------") != -1 , "Rfchmod doesn't work with remote files"
                

	def localRfdir(self):
		cmd=["rfdir "+localDir,"ls "+localDir]
	        UtilityForCastorTest.saveOnFile(localDir+"RfioLocalRfdir",cmd,caseScen)
		
                fi=open(localDir+"RfioLocalRfdir","r")
	        buffOut=fi.read()
                fi.close()
                fi=open(localDir+"RfioLocalRfdir1","r")
	        buffOut2=fi.read()
                fi.close()
		assert buffOut.find(buffOut2), "rfdir is not working with local dir"
		assert buffOut2.find(buffOut), "rfdir is not working with local dir"
		
	def remoteRfdir(self):
		cmd=["rfdir "+remoteDir]
	
	        UtilityForCastorTest.saveOnFile(localDir+"RfioRemoteRfdir",cmd,caseScen)
		
                fi=open(localDir+"RfioRemoteRfdir","r")
		buffOut=fi.read()
		fi.close()
	        assert buffOut.find("error") == -1, "rfdir is not working with remote dir"
		assert buffOut.find("No such file or directory") == -1, "rfdir is not working with remote dir"
			
	def localRfmkdir(self):
		cmd=["rfmkdir "+localDir+"fileRfioLocalRfmkdir"+ticket,"ls "+localDir+"fileRfioLocalRfmkdir"+ticket] 
		UtilityForCastorTest.saveOnFile(localDir+"RfioLocalRfmkdir",cmd,caseScen)
		
                fi=open(localDir+"RfioLocalRfmkdir1","r")
		buffOut=fi.read()
		fi.close()
                assert buffOut.find("No such file or directory") == -1, "rfmkdir is not working for local files"
		
	def remoteRfmkdir(self):
	        cmd=["rfmkdir "+remoteDir+"fileRfioRemoteRfmkdir"+ticket,"rfdir "+remoteDir+"fileRfioRemoteRfmkdir"+ticket] 
		UtilityForCastorTest.saveOnFile(localDir+"RfioRemoteRfmkdir",cmd,caseScen)
		
                fi=open(localDir+"RfioRemoteRfmkdir1","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory") == -1, "rfmkdir is not working for remote files"
		
	def localRfrename(self):
		cmd=["rfcp "+inputFile+" "+localDir+"fileRfioLocalRfrename"+ticket,"rfrename "+localDir+"fileRfioLocalRfrename"+ticket+"  "+localDir+"fileRfioLocalRfrename2"+ticket,"ls  "+localDir+"fileRfioLocalRfrename"+ticket,"ls "+localDir+"fileRfioLocalRfrename2"+ticket]
		
		UtilityForCastorTest.saveOnFile(localDir+"RfioLocalRfrename",cmd,caseScen)
		
                fi=open(localDir+"RfioLocalRfrename2","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory")!= -1, "rfrename doesn't work with local files"
		fi.close()
		
                fi=open(localDir+"RfioLocalRfrename3","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory") == -1, "rfrename doesn't work with local files"
	

	def remoteRfrename(self):
		cmd=["rfcp "+inputFile+" "+remoteDir+"fileRfioRemoteRfrename"+ticket,"rfrename "+remoteDir+"fileRfioRemoteRfrename"+ticket+"  "+remoteDir+"fileRfioRemoteRfrename2"+ticket,"rfdir  "+remoteDir+"fileRfioRemoteRfrename"+ticket,"rfdir "+remoteDir+"fileRfioRemoteRfrename2"+ticket] 
		UtilityForCastorTest.saveOnFile(localDir+"RfioRemoteRfrename",cmd,caseScen)
		
                fi=open(localDir+"RfioRemoteRfrename2","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory")!= -1, "rfrename doesn't work with local files"
		
                fi=open(localDir+"RfioRemoteRfrename3","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory") == -1, "rfrename doesn't work with local files"
		


	def localRfrm(self):
	        cmd=["rfcp "+inputFile+" "+localDir+"fileRfioLocalRfrm"+ticket,"ls "+localDir+"fileRfioLocalRfrm"+ticket,"rfrm "+localDir+"fileRfioLocalRfrm"+ticket,"ls "+localDir+"fileRfioLocalRfrm"+ticket] 
		UtilityForCastorTest.saveOnFile(localDir+"RfioLocalRfrm"+myTag,cmd,caseScen)
		
                fi=open(localDir+"RfioLocalRfrm1","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory") == -1, "rfrm doesn't work with local files" 

		fi=open(localDir+"RfioLocalRfrm3","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory") != -1, "rfrm doesn't work with local files" 

	def remoteRfrm(self):
		
                cmd=["rfcp "+inputFile+" "+remoteDir+"fileRfioRemoteRfrm"+ticket,"rfdir "+remoteDir+"fileRfioRemoteRfrm"+ticket,"rfrm "+remoteDir+"fileRfioRemoteRfrm"+ticket,"rfdir "+remoteDir+"fileRfioRemoteRfrm"+ticket] 
		UtilityForCastorTest.saveOnFile(localDir+"RfioRemoteRfrm"+myTag,cmd,caseScen)
		
                fi=open(localDir+"RfioRemoteRfrm1","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory") == -1, "rfrm doesn't work with remote files" 

		fi=open(localDir+"RfioRemoteRfrm3","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory") != -1, "rfrm doesn't work with remote files" 
                fi.close()
		
	def localRfstat(self):

		cmd=["rfcp "+inputFile+" "+localDir+"fileRfioLocalRfstat"+ticket,"rfstat "+localDir+"fileRfioLocalRfstat"+ticket] 
		UtilityForCastorTest.saveOnFile(localDir+"RfioLocalRfstat",cmd,caseScen)
		
                fi=open(localDir+"RfioLocalRfstat1","r")
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
		cmd=["rfcp "+inputFile+" "+remoteDir+"fileRfioRemoteRfstat"+ticket,"rfstat "+remoteDir+"fileRfioRemoteRfstat"+ticket] 
		UtilityForCastorTest.saveOnFile(localDir+"RfioRemoteRfstat",cmd,caseScen)
		
                fi=open(localDir+"RfioRemoteRfstat1","r")
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
            caseScen=UtilityForCastorTest.createScenarium(-1,-1,-1,-1,castorEnv)
    
class RfioCastorScenarium2(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Env2"
	    global myTurl
	    myTurl=castorDir
	    global caseScen
	    caseScen=UtilityForCastorTest.createScenarium(stagerHost,-1,-1,stagerVersion,castorEnv)
            
class RfioCastorScenarium3(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Env3"
	    global myTurl
	    myTurl=castorDir
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(stagerHost,stagerPort,-1,stagerVersion,castorEnv)


class RfioCastorScenarium4(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Env4"
	    global myTurl
	    myTurl=castorDir
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(stagerHost,-1,stagerSvcClass,stagerVersion,castorEnv)

class RfioCastorScenarium5(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Env5"
	    global myTurl
	    myTurl=castorDir
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerSvcClass,stagerVersion,castorEnv)
	    
class RfioCastorNewTurl1(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl1"
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerSvcClass,stagerVersion,castorEnv)
            global myTurl 
	    myTurl="rfio:///"+castorDir


class RfioCastorNewTurl2(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl2"
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerSvcClass,stagerVersion,castorEnv)
            global myTurl
	    myTurl="rfio://"+castorDir

            
class RfioCastorNewTurl3(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl3"
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerSvcClass,stagerVersion,castorEnv)
            global myTurl
	    myTurl="rfio:///castor\\?path="+castorDir  


class RfioCastorNewTurl4(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl4"
            global caseScen
            caseScen=UtilityForCastorTest.createScenarium(-1,stagerPort,stagerSvcClass,stagerVersion,castorEnv)
            global myTurl
	    myTurl="rfio://"+stagerHost+"/castor\\?path="+castorDir

            
class RfioCastorNewTurl5(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl5"
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(-1,stagerPort,stagerSvcClass,stagerVersion,castorEnv)
            global myTurl
	    myTurl="rfio://"+stagerHost+"//castor\\?path="+castorDir

            
class RfioCastorNewTurl6(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl6"
            UtilityForCastorTest.createScenarium(-1,-1,stagerSvcClass,stagerVersion,castorEnv)
            global myTurl
	    myTurl="rfio://"+stagerHost+":"+stagerPort+"/castor\\?path="+castorDir
          
            
class RfioCastorNewTurl7(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl7"
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(-1,stagerPort,stagerSvcClass,stagerVersion,castorEnv)
            global myTurl
	    myTurl="rfio://"+stagerHost+"/castor\\?path="+castorDir
 

class RfioCastorNewTurl8(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl8"
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(-1,-1,stagerSvcClass,stagerVersion,castorEnv)
            global myTurl
	    myTurl="rfio://"+stagerHost+":"+stagerPort+"/castor\\?path="+castorDir
    
 
class RfioCastorNewTurl9(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl9"
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(-1,-1,stagerSvcClass,stagerVersion,castorEnv)
            global myTurl
	    myTurl="rfio://"+stagerHost+":"+stagerPort+"/"+castorDir

 
class RfioCastorNewTurl10(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl10"
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(-1,-1,stagerSvcClass,stagerVersion,castorEnv)
            global myTurl
	    myTurl="rfio://"+stagerHost+":"+stagerPort+castorDir

class RfioCastorNewTurl11(unittest.TestCase):
        def setUp(self):
	    global myTag
	    myTag="Turl11"
	    global caseScen
            caseScen=UtilityForCastorTest.createScenarium(-1,-1,-1,-1,castorEnv)
            global myTurl
	    myTurl="rfio://"+stagerHost+":"+stagerPort+"/castor\\?svcClass="+stagerSvcClass+"\\&castorVersion="+stagerVersion+"\\&path="+castorDir



         
###  RFCP   ###



class RfioRfcpCastor(unittest.TestCase):

        def localToCastor(self):
	        cmd=["rfcp "+inputFile+" "+myTurl+"fileRfioLocalToCastor"+myTag+ticket]
	
		UtilityForCastorTest.saveOnFile(localDir+"RfioRfcpLocalToCastor"+myTag,cmd,caseScen)	
		
                fi=open(localDir+"RfioRfcpLocalToCastor"+myTag,"r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from local to castor"
                
	def castorToLocal(self):
                cmd=["rfcp "+inputFile+" "+myTurl+"fileRfioCastorToLocal"+myTag+ticket,"rfcp "+myTurl+"fileRfioCastorToLocal"+myTag+ticket+" "+localDir+"fileRfioCastorToLocalCopy"+myTag+ticket,"diff "+inputFile+" "+localDir+"fileRfioCastorToLocalCopy"+myTag+ticket]
	
		UtilityForCastorTest.saveOnFile(localDir+"RfioRfcpCastorToLocal"+myTag,cmd,caseScen)
		
                
                fi=open(localDir+"RfioRfcpCastorToLocal"+myTag,"r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through local (in) and eth0 (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from local to castor"

                fi=open(localDir+"RfioRfcpCastorToLocal"+myTag+"1","r")
                buffOut=fi.read()
                fi.close()
        	assert buffOut.rfind("through eth0 (in) and local (out)") != -1 or buffOut.rfind(" bytes ready for migration") != -1, "rfio doesn't work from  castor to local"

                assert os.stat(localDir+"fileRfioCastorToLocalCopy"+myTag+ticket)[6] != 0, "rfio doesn't work from castor to local"
                assert os.stat(localDir+"RfioRfcpCastorToLocal"+myTag+"2")[6] == 0, "rfio doesn't work from castor to local"

        def remoteToCastor(self):
                cmd=["rfcp "+inputFile+" "+remoteDir+"fileRfioRemoteToCastor"+myTag+ticket,"rfcp "+remoteDir+"fileRfioRemoteToCastor"+myTag+ticket+" "+myTurl+"fileRfioRemoteToCastorCopy"+myTag+ticket,"rfcp "+myTurl+"fileRfioRemoteToCastorCopy"+myTag+ticket+" "+localDir+"fileRfioRemoteToCastorCopyCopy"+myTag+ticket,"diff "+inputFile+" "+localDir+"fileRfioRemoteToCastorCopyCopy"+myTag+ticket]

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

                assert os.stat(localDir+"fileRfioRemoteToCastorCopyCopy"+myTag+ticket)[6] != 0, "rfio doesn't work from remote to castor"
                assert os.stat(localDir+"RfioRfcpRemoteToCastor"+myTag+"3")[6] == 0, "rfio doesn't work from remote to castor"
                
        def castorToRemote(self):
                
		cmd=["rfcp "+inputFile+" "+myTurl+"fileRfioCastorToRemote"+myTag+ticket,"rfcp "+myTurl+"fileRfioCastorToRemote"+myTag+ticket+" "+remoteDir+"fileRfioCastorToRemoteCopy"+myTag+ticket,"rfcp "+remoteDir+"fileRfioCastorToRemoteCopy"+myTag+ticket+" "+localDir+"fileRfioCastorToRemoteCopyCopy"+myTag+ticket,"diff "+inputFile+" "+localDir+"fileRfioCastorToRemoteCopyCopy"+myTag+ticket]

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

                assert os.stat(localDir+"fileRfioCastorToRemoteCopyCopy"+myTag+ticket)[6] != 0, "rfio doesn't work from castor to remote"
                assert os.stat(localDir+"RfioRfcpCastorToRemote"+myTag+"3")[6] == 0, "rfio doesn't work from castor to remote"

        def castorToCastor(self):
                
                cmd=["rfcp "+inputFile+" "+myTurl+"fileRfioCastorToCastor"+myTag+ticket,"rfcp "+myTurl+"fileRfioCastorToCastor"+myTag+ticket+" "+myTurl+"fileRfioCastorToCastorCopy"+myTag+ticket,"rfcp "+myTurl+"fileRfioCastorToCastorCopy"+myTag+ticket+" "+localDir+"fileRfioCastorToCastorCopyCopy"+myTag+ticket,"diff "+inputFile+" "+localDir+"fileRfioCastorToCastorCopyCopy"+myTag+ticket]
	
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

                assert os.stat(localDir+"fileRfioCastorToCastorCopyCopy"+myTag+ticket)[6] != 0, "rfio doesn't work from castor to castor"
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
	    
class RfioRfcpNewTurl1(RfioCastorNewTurl1,RfioRfcpCastor):
        pass

class RfioRfcpNewTurl2(RfioCastorNewTurl2,RfioRfcpCastor):
	pass

class RfioRfcpNewTurl3(RfioCastorNewTurl3,RfioRfcpCastor):
	pass

class RfioRfcpNewTurl4(RfioCastorNewTurl4,RfioRfcpCastor):
        pass
            
class RfioRfcpNewTurl5(RfioCastorNewTurl5,RfioRfcpCastor):
        pass
            
class RfioRfcpNewTurl6(RfioCastorNewTurl6,RfioRfcpCastor):
        pass
            
class RfioRfcpNewTurl7(RfioCastorNewTurl7,RfioRfcpCastor):
        pass

class RfioRfcpNewTurl8(RfioCastorNewTurl8,RfioRfcpCastor):
        pass
 
class RfioRfcpNewTurl9(RfioCastorNewTurl9,RfioRfcpCastor):
        pass
 
class RfioRfcpNewTurl10(RfioCastorNewTurl10,RfioRfcpCastor):
        pass
class RfioRfcpNewTurl11(RfioCastorNewTurl11,RfioRfcpCastor):
        pass


### Other Cmd ####

class RfioOtherCmdCastor(unittest.TestCase):
	def castorRfcat(self):
		cmd=["rfcp "+inputFile+" "+myTurl+"fileRfioRfcatCastor"+myTag+ticket,"rfcat "+myTurl+"fileRfioRfcatCastor"+myTag+ticket]
		UtilityForCastorTest.saveOnFile(localDir+"RfioCastorRfcat"+myTag,cmd,caseScen)
		
                fi=open(localDir+"RfioCastorRfcat"+myTag+"1","r")
                buffOut=fi.read()
                fi.close()

		fi=open(inputFile,"r")
                buffOut2=fi.read()
                fi.close()

                assert buffOut.find(buffOut2) != -1, "rfcat is not working for castor files"
		
	def castorRfchmod(self):
		cmd=["rfcp "+inputFile+" "+myTurl+"fileRfioRfchmodCastor"+myTag+ticket,"nsls -l "+castorDir+"fileRfioRfchmodCastor"+myTag+ticket,"rfchmod 777 "+myTurl+"fileRfioRfchmodCastor"+myTag+ticket,"nsls -l "+castorDir+"fileRfioRfchmodCastor"+myTag+ticket,"rfchmod 600 "+myTurl+"fileRfioRfchmodCastor"+myTag+ticket,"nsls -l "+castorDir+"fileRfioRfchmodCastor"+myTag+ticket]
		
		UtilityForCastorTest.saveOnFile(localDir+"RfioCastorRfchmod"+myTag,cmd,caseScen)
		
                fi=open(localDir+"RfioCastorRfchmod"+myTag+"1","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("-rw-r--r--")!= -1, "Rfchmod doesn't work with castor files"

                fi=open(localDir+"RfioCastorRfchmod"+myTag+"3","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("-rwxrwxrwx") != -1, "Rfchmod doesn't work with castor files"

                fi=open(localDir+"RfioCastorRfchmod"+myTag+"5","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("-rw-------") != -1 , "Rfchmod doesn't work with castor files"
                
		
	def castorRfdir(self):
	        cmd=["rfdir "+myTurl,"nsls -l"+castorDir]
		
	        UtilityForCastorTest.saveOnFile(localDir+"RfioCastorRfdir"+myTag,cmd,caseScen)
		
                fi=open(localDir+"RfioCastorRfdir"+myTag,"r")
	        buffOut=fi.read()
                fi.close()
                fi=open(localDir+"RfioCastorRfdir"+myTag+"1","r")
	        buffOut2=fi.read()
                fi.close()
		assert buffOut.find(buffOut2), "rfdir is not working with remote dir"
		assert buffOut2.find(buffOut), "rfdir is not working with remote dir"
		

	def castorRfmkdir(self):
	        cmd=["rfmkdir "+myTurl+"fileRfioCastorRfmkdir"+myTag+ticket,"nsls "+castorDir+"fileRfioCastorRfmkdir"+myTag+ticket]
		
		UtilityForCastorTest.saveOnFile(localDir+"RfioCastorRfmkdir"+myTag,cmd,caseScen)
		
                fi=open(localDir+"RfioCastorRfmkdir"+myTag+"1","r")
		buffOut=fi.read()
		fi.close()
		
	        assert buffOut.find("No such file or directory") == -1, "rfmkdir is not working for castor files"
		
		
	def castorRfrename(self):
		cmd=["rfcp "+inputFile+" "+myTurl+"fileRfioCastorRfrename"+myTag+ticket,"rfrename "+myTurl+"fileRfioCastorRfrename"+myTag+ticket+"  "+myTurl+"fileRfioCastorRfrename2"+myTag+ticket,"nsls  "+castorDir+"fileRfioCastorRfrename"+myTag+ticket,"nsls "+castorDir+"fileRfioCastorRfrename2"+myTag+ticket] 
		
		UtilityForCastorTest.saveOnFile(localDir+"RfioCastorRfrename"+myTag,cmd,caseScen)
		
                fi=open(localDir+"RfioCastorRfrename"+myTag+"2","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory")!= -1, "rfrename doesn't work with local files"
		
		
                fi=open(localDir+"RfioCastorRfrename"+myTag+"3","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory") == -1, "rfrename doesn't work with local files"
		

	def castorRfrm(self):
                cmd=["rfcp "+inputFile+" "+myTurl+"fileRfioCastorRfrm"+myTag+ticket,"nsls "+castorDir+"fileRfioCastorRfrm"+myTag+ticket,"rfrm "+myTurl+"fileRfioCastorRfrm"+myTag+ticket,"nsls "+castorDir+"fileRfioCastorRfrm"+myTag+ticket] 
		
		UtilityForCastorTest.saveOnFile(localDir+"RfioCastorRfrm"+myTag,cmd,caseScen)
		
                fi=open(localDir+"RfioCastorRfrm"+myTag+"1","r")
		buffOut=fi.read()
		fi.close()
		
                assert buffOut.find("No such file or directory") == -1, "rfrm doesn't work with castor files" 

		fi=open(localDir+"RfioCastorRfrm"+myTag+"3","r")
	        buffOut=fi.read()
		fi.close()
                assert buffOut.find("No such file or directory") != -1, "rfrm doesn't work with castor files"
		
                
        def castorRfstat(self):
	        cmd=["rfcp "+inputFile+" "+myTurl+"fileRfioCastorRfstat"+myTag+ticket,"rfstat "+myTurl+"fileRfioCastorRfstat"+myTag+ticket]
		
		UtilityForCastorTest.saveOnFile(localDir+"RfioCastorRfstat"+myTag,cmd,caseScen)
		
                fi=open(localDir+"RfioCastorRfstat"+myTag+"1","r")
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
	    
class RfioOtherCmdNewTurl1(RfioCastorNewTurl1,RfioOtherCmdCastor):
        pass

class RfioOtherCmdNewTurl2(RfioCastorNewTurl2,RfioOtherCmdCastor):
	pass

class RfioOtherCmdNewTurl3(RfioCastorNewTurl3,RfioOtherCmdCastor):
	pass

class RfioOtherCmdNewTurl4(RfioCastorNewTurl4,RfioOtherCmdCastor):
        pass
            
class RfioOtherCmdNewTurl5(RfioCastorNewTurl5,RfioOtherCmdCastor):
        pass
            
class RfioOtherCmdNewTurl6(RfioCastorNewTurl6,RfioOtherCmdCastor):
        pass
            
class RfioOtherCmdNewTurl7(RfioCastorNewTurl7,RfioOtherCmdCastor):
        pass

class RfioOtherCmdNewTurl8(RfioCastorNewTurl8,RfioOtherCmdCastor):
        pass
 
class RfioOtherCmdNewTurl9(RfioCastorNewTurl9,RfioOtherCmdCastor):
        pass
 
class RfioOtherCmdNewTurl10(RfioCastorNewTurl10,RfioOtherCmdCastor):
        pass

class RfioOtherCmdNewTurl11(RfioCastorNewTurl11,RfioOtherCmdCastor):
        pass






###################
#                 # 
# RFIO API        # 
#                 #
###################


class RfioApiCase(unittest.TestCase):
	def statTest(self):
		pass
	def readWriteTest(self):
		pass	
	def preSeekTest(self):
		pass
	def longPathTest(self):
		pass
	def unlinkTest(self):
		pass
	def regretionTest(self):
		pass

		
###################
#                 # 
#  RFIO STRESS    # 
#                 #
###################


class RfioStressCase(unittest.TestCase):
	def parallelFileAccess(self):
		pass
	def dataSource(self):
		pass


##########################################################################################################
##########################################################################################################

casesPreReq=("mainScenariumSetUp","stageMapSituation","castorConfSituation")
casesRfcpSimple=("localToLocal","localToRemote","remoteToLocal","remoteToRemote")
casesOtherCmdSimple=("localRfcat","remoteRfcat","localRfchmod","remoteRfchmod","localRfdir","remoteRfdir","localRfmkdir","remoteRfmkdir","localRfrename","remoteRfrename","localRfrm","remoteRfrm","localRfstat","remoteRfstat")
casesRfcpCastor=("localToCastor","castorToLocal","remoteToCastor","castorToRemote","castorToCastor")
casesOtherCmdCastor=("castorRfcat","castorRfchmod","castorRfdir","castorRfmkdir","castorRfrename","castorRfrm","castorRfstat")
casesApi=("statTest","readWriteTest","preSeekTest","longPathTest","unlinkTest")
casesStress=("parallelFileAccess","datasource")

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

## NEW TURL POSSIBILITY

# rfcp

class RfioRfcpNewTurl1Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpNewTurl1,casesRfcpCastor))

class  RfioRfcpNewTurl2Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpNewTurl2,casesRfcpCastor))

class RfioRfcpNewTurl3Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpNewTurl3,casesRfcpCastor))

class  RfioRfcpNewTurl4Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpNewTurl4,casesRfcpCastor))
        
class  RfioRfcpNewTurl5Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpNewTurl5,casesRfcpCastor))

class  RfioRfcpNewTurl6Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpNewTurl6,casesRfcpCastor))

class  RfioRfcpNewTurl7Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpNewTurl7,casesRfcpCastor))

class  RfioRfcpNewTurl8Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpNewTurl8,casesRfcpCastor))

class  RfioRfcpNewTurl9Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpNewTurl9,casesRfcpCastor))

class  RfioRfcpNewTurl10Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpNewTurl10,casesRfcpCastor))

class  RfioRfcpNewTurl11Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioRfcpNewTurl11,casesRfcpCastor))

# other cmd

class RfioOtherCmdNewTurl1Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdNewTurl1,casesOtherCmdCastor))
	
class RfioOtherCmdNewTurl2Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdNewTurl2,casesOtherCmdCastor))
	
class RfioOtherCmdNewTurl3Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdNewTurl3,casesOtherCmdCastor))
	
class RfioOtherCmdNewTurl4Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdNewTurl4,casesOtherCmdCastor))

class RfioOtherCmdNewTurl5Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdNewTurl5,casesOtherCmdCastor))

class RfioOtherCmdNewTurl6Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdNewTurl6,casesOtherCmdCastor))

class RfioOtherCmdNewTurl7Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdNewTurl7,casesOtherCmdCastor))

class RfioOtherCmdNewTurl8Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdNewTurl8,casesOtherCmdCastor))

class RfioOtherCmdNewTurl9Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdNewTurl9,casesOtherCmdCastor))

class RfioOtherCmdNewTurl10Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdNewTurl10,casesOtherCmdCastor))
	
class RfioOtherCmdNewTurl11Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioOtherCmdNewTurl11,casesOtherCmdCastor))

## RFIO API

class RfioApiSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioApiCase,casesApi))
	
## RFIO STRESS

class RfioStressSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioStressCase,casesStress))
	

###  BIG TEST SUITE WITH ALL NEW TURL CASES

class RfioRfcpNewTurlSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,(RfioRfcpNewTurl1Suite(),RfioRfcpNewTurl2Suite(),RfioRfcpNewTurl3Suite(),RfioRfcpNewTurl4Suite(),RfioRfcpNewTurl5Suite(),RfioRfcpNewTurl6Suite(),RfioRfcpNewTurl7Suite(),RfioRfcpNewTurl8Suite(),RfioRfcpNewTurl9Suite(),RfioRfcpNewTurl10Suite(),RfioRfcpNewTurl11Suite()))

class RfioOtherCmdNewTurlSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,(RfioOtherCmdNewTurl1Suite(),RfioOtherCmdNewTurl2Suite(),RfioOtherCmdNewTurl3Suite(),RfioOtherCmdNewTurl4Suite(),RfioOtherCmdNewTurl5Suite(),RfioOtherCmdNewTurl6Suite(),RfioOtherCmdNewTurl7Suite(),RfioOtherCmdNewTurl8Suite(),RfioOtherCmdNewTurl9Suite(),RfioOtherCmdNewTurl10Suite(),RfioOtherCmdNewTurl11Suite()))



## API

class RfioApiSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioApiCase,casesApi))


## STRESS

class RfioStressSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RfioStressCase,casesStress))
