import unittest
import UtilityForCastorTest
import os
import re
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
rootbin=""
rootsys=""
myCastor=UtilityForCastorTest.prepareCastorString()
dirCastor=myCastor+"tmpClientTest"+ticket+"/"


class PreRequisitesCase(unittest.TestCase):
    def mainScenarium(self):
        assert (UtilityForCastorTest.checkUser() != -1), "you don't have a valid castor directory"

        try:

          f=open("./CASTORTESTCONFIG","r")
          configFileInfo=f.read()
          f.close
  
          index= configFileInfo.find("*** Root test specific parameters ***")
          configFileInfo=configFileInfo[index:]
          index=configFileInfo.find("***")
          if index != -1:
              index=index-1
              configFileInfo=configFileInfo[:index]
  
          global localDir
          localDir=(configFileInfo[configFileInfo.find("LOG_DIR"):]).split()[1]
          localDir=localDir+ticket+"/"
          os.system("mkdir "+localDir)
  
          global rootbin, rootsys
          rootsys=(configFileInfo[configFileInfo.find("ROOTSYS"):]).split()[1]
          rootbin=rootsys+"/bin/root -b -l"

          global inputFile
          inputFile=(configFileInfo[configFileInfo.find("INPUT_FILE"):]).split()[1]
          
          global myScen
          myScen=UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerSvcClass,stagerVersion,None,[["STAGER_TRACE","3"]])
          myShell=os.popen('ls -l /bin/sh').read()
          if myShell.find("bash") != -1:
              myScen="export ROOTSYS="+rootsys+";"+myScen
          if myShell.find("tcsh") != -1:
              myScen="setenv ROOTSYS "+rootsys+";"+myScen

          UtilityForCastorTest.runOnShell(["nsmkdir "+dirCastor],myScen)
                
        except IOError:
          assert 0==-1, "An error in the preparation of the main setting occurred ... test is not valid"
    
    def rootHelloWorld(self):
        cmd=["echo 'printf(\"Hello World !\\n\");' | "+rootbin]
        UtilityForCastorTest.saveOnFile(localDir+"RootHelloWorld",cmd,myScen)
        fi=open(localDir+"RootHelloWorld","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.rfind("Hello World !") != -1, "root does not work"
    
class RootCase(unittest.TestCase):
    def rootWrite(self):
        fileContent = """
TFile *f = new T"""+self.protocol+"File(\""+self.protocol.lower()+"://"+dirCastor+"fileRoot"+self.protocol+ticket+"""\",\"RECREATE\");
TNtuple *ntuple = new TNtuple(\"ntuple\",\"test\",\"x:y:z\");
ntuple->Fill(1,2,3);
ntuple->Fill(4,5,6);
f->Write();
"""
        cmd=["echo '"+fileContent+"' | "+rootbin,"nsls -l "+dirCastor+"fileRoot"+self.protocol+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"RootWrite."+self.protocol,cmd,myScen)
        message = "root with "+self.protocol+" protocol does not work for writing"

        fi=open(localDir+"RootWrite."+self.protocol,"r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.rfind("Error:") == -1, message

        fi=open(localDir+"RootWrite."+self.protocol+"1","r")
        buffOut=fi.read().split()[4]
        fi.close()
        assert buffOut.isdigit() == 1, message
        assert int(buffOut) != 0, message

    def rootRead(self):
        fileContent = """
TFile *f = new T"""+self.protocol+"File(\""+self.protocol.lower()+"://"+dirCastor+"fileRoot"+self.protocol+ticket+"""\",\"READ\");
f->ls();
ntuple.Print()
"""
        cmd=["echo '"+fileContent+"' | "+rootbin]
        UtilityForCastorTest.saveOnFile(localDir+"RootRead."+self.protocol,cmd,myScen)
        fi=open(localDir+"RootRead."+self.protocol,"r")
        buffOut=fi.read()
        fi.close()
        message = "root with "+self.protocol+" protocol does not work for reading"
        assert re.search('KEY: TNtuple.*ntuple;1.*test',buffOut) != None, message
        assert buffOut.rfind("*Tree    :ntuple    : test") != -1, message
        assert buffOut.rfind("*Entries :        2") != -1, message

class RootRfioCase(RootCase):
    def __init__(self, methodName):
        self.protocol = "RFIO"
        RootCase.__init__(self, methodName)

class RootCastorCase(RootCase):
    def __init__(self, methodName):
        self.protocol = "Castor"
        RootCase.__init__(self, methodName)

casesPreClient=("mainScenarium","rootHelloWorld")
casesRoot=("rootWrite", "rootRead")

class RootPreRequisitesSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(PreRequisitesCase,casesPreClient))

class RootRFIOSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootRfioCase,casesRoot))

class RootCastorSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootCastorCase,casesRoot))
        
