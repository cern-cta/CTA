import unittest
import os
import re
import sys
import time
import threading
import UtilityForCastorTest
from UtilityForCastorTest import stagerHost,stagerPort,stagerSvcClass,stagerVersion,stagerTimeOut,stagerExtraSvcClass,stagerDiskOnlySvcClass,stagerForcedFileClass,quietMode,outputDir,configFile,ticket
import RfioTest
from RfioTest import myTurl, myTag

# parameters

myScen=""

# files and directories

localDir=""
rootbin=""
rootsys=""


dirCastor=outputDir+"/tmpRootTest"+ticket+"/"

class PreRequisitesCase(unittest.TestCase):
    def mainScenarium(self):
        assert (UtilityForCastorTest.checkUser() != -1), "you don't have acccess to directory \"" + outputDir + "\" where you wanted to run the test"
        try:
            global localDir,rootbin,rootsys,myScen
            params = UtilityForCastorTest.parseConfigFile(configFile, "Root")
            localDir = params["LOG_DIR"]
            localDir = localDir+ticket+"/"
            os.system("mkdir "+localDir)
            if os.environ.has_key("ROOTSYS"):
                rootsys = os.environ["ROOTSYS"]
            else:
                rootsys = params["ROOTSYS"]
            rootbin = rootsys+"/bin/root -b -l"
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
        fileName=RfioTest.myTurl.replace("\\&","&").replace("\\?","?").replace("tmpRfioTest","tmpRootTest")
        fileName=self.protocol.lower()+"://"+fileName+"fileRoot"+self.protocol+RfioTest.myTag+ticket
        fileContent = """
TFile *f = new T"""+self.protocol+"File(\""+fileName+"""\",\"RECREATE\");
TNtuple *ntuple = new TNtuple(\"ntuple\",\"test\",\"x:y:z\");
ntuple->Fill(1,2,3);
ntuple->Fill(4,5,6);
f->Write();
"""

        cmd=["echo '"+fileContent+"' | "+rootbin, "sleep 5", "nsls -l "+dirCastor+"fileRoot"+self.protocol+RfioTest.myTag+ticket]

        UtilityForCastorTest.saveOnFile(localDir+"RootWrite."+self.protocol+RfioTest.myTag,cmd,myScen)
        message = "root with "+self.protocol+" protocol does not work for writing"

        fi=open(localDir+"RootWrite."+self.protocol+RfioTest.myTag,"r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.rfind("Error:") == -1, message

        fi=open(localDir+"RootWrite."+self.protocol+RfioTest.myTag+"2","r")
        buffOut=fi.read().split()[4] # it contains a number of bytes transfered (or a part of an error message)
        fi.close()
        assert buffOut.isdigit() == 1, message
        assert int(buffOut) != 0, message

    def rootRead(self):
        fileName=RfioTest.myTurl.replace("\\&","&").replace("\\?","?").replace("tmpRfioTest","tmpRootTest")
        fileName=self.protocol.lower()+"://"+fileName+"fileRoot"+self.protocol+RfioTest.myTag+ticket
        fileContent = """
TFile *f = new T"""+self.protocol+"File(\""+fileName+"""\",\"READ\");
f->ls();
ntuple.Print()
"""

        cmd=["echo '"+fileContent+"' | "+rootbin]
        UtilityForCastorTest.saveOnFile(localDir+"RootRead."+self.protocol+RfioTest.myTag,cmd,myScen)
        fi=open(localDir+"RootRead."+self.protocol+RfioTest.myTag,"r")
        buffOut=fi.read()
        fi.close()
        message = "root with "+self.protocol+" protocol does not work for reading"
        assert re.search('KEY: TNtuple.*ntuple;1.*test',buffOut) != None, message
        assert buffOut.rfind("*Tree    :ntuple    : test") != -1, message
        assert buffOut.rfind("*Entries :        2") != -1, message


casesPreClient=("mainScenarium","rootHelloWorld")
casesRoot=("rootWrite", "rootRead")



class RootPreRequisitesSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(PreRequisitesCase,casesPreClient))


# TRFIO cases

class RootRfioNewTurl1(RfioTest.RfioCastorNewTurl1,RootCase):
    def __init__(self, methodName):
        self.protocol = "RFIO"
        RootCase.__init__(self, methodName)

class RootRfioNewTurl2(RfioTest.RfioCastorNewTurl2,RootCase):
    def __init__(self, methodName):
        self.protocol = "RFIO"
        RootCase.__init__(self, methodName)

class RootRfioNewTurl3(RfioTest.RfioCastorNewTurl3,RootCase):
    def __init__(self, methodName):
        self.protocol = "RFIO"
        RootCase.__init__(self, methodName)

class RootRfioNewTurl4(RfioTest.RfioCastorNewTurl4,RootCase):
    def __init__(self, methodName):
        self.protocol = "RFIO"
        RootCase.__init__(self, methodName)

class RootRfioNewTurl5(RfioTest.RfioCastorNewTurl5,RootCase):
    def __init__(self, methodName):
        self.protocol = "RFIO"
        RootCase.__init__(self, methodName)

class RootRfioNewTurl6(RfioTest.RfioCastorNewTurl6,RootCase):
    def __init__(self, methodName):
        self.protocol = "RFIO"
        RootCase.__init__(self, methodName)

class RootRfioNewTurl7(RfioTest.RfioCastorNewTurl7,RootCase):
    def __init__(self, methodName):
        self.protocol = "RFIO"
        RootCase.__init__(self, methodName)

class RootRfioNewTurl8(RfioTest.RfioCastorNewTurl8,RootCase):
    def __init__(self, methodName):
        self.protocol = "RFIO"
        RootCase.__init__(self, methodName)

class RootRfioNewTurl9(RfioTest.RfioCastorNewTurl9,RootCase):
    def __init__(self, methodName):
        self.protocol = "RFIO"
        RootCase.__init__(self, methodName)

class RootRfioNewTurl10(RfioTest.RfioCastorNewTurl10,RootCase):
    def __init__(self, methodName):
        self.protocol = "RFIO"
        RootCase.__init__(self, methodName)

class RootRfioNewTurl11(RfioTest.RfioCastorNewTurl11,RootCase):
    def __init__(self, methodName):
        self.protocol = "RFIO"
        RootCase.__init__(self, methodName)

# TRFIO suites

class RootRfioNewTurl1Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootRfioNewTurl1,casesRoot))

class RootRfioNewTurl2Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootRfioNewTurl2,casesRoot))

class RootRfioNewTurl3Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootRfioNewTurl3,casesRoot))

class RootRfioNewTurl4Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootRfioNewTurl4,casesRoot))

class RootRfioNewTurl5Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootRfioNewTurl5,casesRoot))

class RootRfioNewTurl6Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootRfioNewTurl6,casesRoot))

class RootRfioNewTurl7Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootRfioNewTurl7,casesRoot))

class RootRfioNewTurl8Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootRfioNewTurl8,casesRoot))

class RootRfioNewTurl9Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootRfioNewTurl9,casesRoot))

class RootRfioNewTurl10Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootRfioNewTurl10,casesRoot))

class RootRfioNewTurl11Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootRfioNewTurl11,casesRoot))


# TCastor cases

class RootCastorNewTurl1(RfioTest.RfioCastorNewTurl1,RootCase):
    def __init__(self, methodName):
        self.protocol = "Castor"
        RootCase.__init__(self, methodName)

class RootCastorNewTurl2(RfioTest.RfioCastorNewTurl2,RootCase):
    def __init__(self, methodName):
        self.protocol = "Castor"
        RootCase.__init__(self, methodName)

class RootCastorNewTurl3(RfioTest.RfioCastorNewTurl3,RootCase):
    def __init__(self, methodName):
        self.protocol = "Castor"
        RootCase.__init__(self, methodName)

class RootCastorNewTurl4(RfioTest.RfioCastorNewTurl4,RootCase):
    def __init__(self, methodName):
        self.protocol = "Castor"
        RootCase.__init__(self, methodName)

class RootCastorNewTurl5(RfioTest.RfioCastorNewTurl5,RootCase):
    def __init__(self, methodName):
        self.protocol = "Castor"
        RootCase.__init__(self, methodName)

class RootCastorNewTurl6(RfioTest.RfioCastorNewTurl6,RootCase):
    def __init__(self, methodName):
        self.protocol = "Castor"
        RootCase.__init__(self, methodName)

class RootCastorNewTurl7(RfioTest.RfioCastorNewTurl7,RootCase):
    def __init__(self, methodName):
        self.protocol = "Castor"
        RootCase.__init__(self, methodName)

class RootCastorNewTurl8(RfioTest.RfioCastorNewTurl8,RootCase):
    def __init__(self, methodName):
        self.protocol = "Castor"
        RootCase.__init__(self, methodName)

class RootCastorNewTurl9(RfioTest.RfioCastorNewTurl9,RootCase):
    def __init__(self, methodName):
        self.protocol = "Castor"
        RootCase.__init__(self, methodName)

class RootCastorNewTurl10(RfioTest.RfioCastorNewTurl10,RootCase):
    def __init__(self, methodName):
        self.protocol = "Castor"
        RootCase.__init__(self, methodName)

class RootCastorNewTurl11(RfioTest.RfioCastorNewTurl11,RootCase):
    def __init__(self, methodName):
        self.protocol = "Castor"
        RootCase.__init__(self, methodName)

# TCastor suites

class RootCastorNewTurl1Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootCastorNewTurl1,casesRoot))

class RootCastorNewTurl2Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootCastorNewTurl2,casesRoot))

class RootCastorNewTurl3Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootCastorNewTurl3,casesRoot))

class RootCastorNewTurl4Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootCastorNewTurl4,casesRoot))

class RootCastorNewTurl5Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootCastorNewTurl5,casesRoot))

class RootCastorNewTurl6Suite(unittest.TestSuite):
    def __init__(self):

        unittest.TestSuite.__init__(self,map(RootCastorNewTurl6,casesRoot))

class RootCastorNewTurl7Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootCastorNewTurl7,casesRoot))

class RootCastorNewTurl8Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootCastorNewTurl8,casesRoot))

class RootCastorNewTurl9Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootCastorNewTurl9,casesRoot))

class RootCastorNewTurl10Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootCastorNewTurl10,casesRoot))

class RootCastorNewTurl11Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(RootCastorNewTurl11,casesRoot))

## Test Suite with all the TRFIO cases

class RootRfioNewTurlSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,(RootRfioNewTurl1Suite(),RootRfioNewTurl2Suite(),RootRfioNewTurl3Suite(),RootRfioNewTurl4Suite(),RootRfioNewTurl5Suite(),RootRfioNewTurl6Suite(),RootRfioNewTurl7Suite(),RootRfioNewTurl8Suite(),RootRfioNewTurl9Suite(),RootRfioNewTurl10Suite(),RootRfioNewTurl11Suite()))

## Test Suite with all the TCastor cases

class RootCastorNewTurlSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,(RootCastorNewTurl1Suite(),RootCastorNewTurl2Suite(),RootCastorNewTurl3Suite(),RootCastorNewTurl4Suite(),RootCastorNewTurl5Suite(),RootCastorNewTurl6Suite(),RootCastorNewTurl7Suite(),RootCastorNewTurl8Suite(),RootCastorNewTurl9Suite(),RootCastorNewTurl10Suite(),RootCastorNewTurl11Suite()))

