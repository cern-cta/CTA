import unittest
import os
import re
import sys
import time
import threading
import UtilityForCastorTest
from UtilityForCastorTest import stagerHost,stagerPort,stagerSvcClass,stagerTimeOut,stagerExtraSvcClass,stagerDiskOnlySvcClass,stagerForcedFileClass,quietMode,outputDir,configFile,ticket
import RfioTest
from RfioTest import myTurl, myTag

# parameters

myScen=""

# files and directories

localDir=""
rootbin=""
rootsys=""

dirCastor=outputDir+"/tmpXRootTest"+ticket+"/"

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
            myScen=UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerSvcClass,None,[["STAGER_TRACE","3"]])
            myShell=os.popen('ls -l /bin/sh').read()
            if myShell.find("bash") != -1:
                myScen="export ROOTSYS="+rootsys+";"+myScen
            if myShell.find("tcsh") != -1:
                myScen="setenv ROOTSYS "+rootsys+";"+myScen
            UtilityForCastorTest.runOnShell(["nsmkdir "+dirCastor],myScen)
        except IOError:
          assert 0==-1, "An error in the preparation of the main setting occurred ... test is not valid"

    def xrootHelloWorld(self):
        cmd=["echo 'printf(\"Hello World !\\n\");' | "+rootbin]
        UtilityForCastorTest.saveOnFile(localDir+"XRootHelloWorld",cmd,myScen)
        fi=open(localDir+"XRootHelloWorld","r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.rfind("Hello World !") != -1, "root does not work"

class XRootCase(unittest.TestCase):
    def xrootWrite(self):
        fileName=RfioTest.myTurl.replace("\\&","&").replace("\\?","?").replace("tmpRfioTest","tmpXRootTest")
        fileName=self.protocol.lower()+"://"+stagerHost+"/"+fileName+"fileXRoot"+self.protocol+RfioTest.myTag+ticket +"?stagerHost="+stagerHost+"&svcClass="+stagerSvcClass+"&mkpath=1"
        fileContent = """
TFile *f = TFile::Open(\""""+fileName+"""\",\"RECREATE\");
TNtuple *ntuple = new TNtuple(\"ntuple\",\"test\",\"x:y:z\");
ntuple->Fill(1,2,3);
ntuple->Fill(4,5,6);
f->Write();
"""
        cmd=["echo '"+fileContent+"' | "+rootbin, "sleep 5", "nsls -l "+dirCastor+"fileXRoot"+self.protocol+RfioTest.myTag+ticket]
        UtilityForCastorTest.saveOnFile(localDir+"XRootWrite."+self.protocol+RfioTest.myTag,cmd,myScen)
        message = "root with "+self.protocol+" protocol does not work for writing"

        fi=open(localDir+"XRootWrite."+self.protocol+RfioTest.myTag,"r")
        buffOut=fi.read()
        fi.close()
        assert buffOut.rfind("Error:") == -1, message

        fi=open(localDir+"XRootWrite."+self.protocol+RfioTest.myTag+"2","r")
        buffOut=fi.read().split()[4] # it contains a number of bytes transfered (or a part of an error message)
        fi.close()
        assert buffOut.isdigit() == 1, message
        assert int(buffOut) != 0, message

    def xrootRead(self):
        fileName=RfioTest.myTurl.replace("\\&","&").replace("\\?","?").replace("tmpRfioTest","tmpXRootTest")
        fileName=self.protocol.lower()+"://"+stagerHost+"/"+fileName+"fileXRoot"+self.protocol+RfioTest.myTag+ticket+"?stagerHost="+stagerHost
        
        fileContent = """
TFile *f = TFile::Open(\""""+fileName+"""\",\"READ\");
f->ls();
ntuple.Print();
"""
        cmd=["echo '"+fileContent+"' | "+rootbin]
        UtilityForCastorTest.saveOnFile(localDir+"XRootRead."+self.protocol+RfioTest.myTag,cmd,myScen)
        fi=open(localDir+"XRootRead."+self.protocol+RfioTest.myTag,"r")
        buffOut=fi.read()
        fi.close()
        message = "root with "+self.protocol+" protocol does not work for reading"
        assert re.search('KEY: TNtuple.*ntuple;1.*test',buffOut) != None, message
        assert buffOut.rfind("*Tree    :ntuple    : test") != -1, message
        assert buffOut.rfind("*Entries :        2") != -1, message

casesPreClient=("mainScenarium","xrootHelloWorld")
casesXRoot=("xrootWrite", "xrootRead")

class XRootPreRequisitesSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(PreRequisitesCase,casesPreClient))

# TXNet cases

class XRootCastorNewTurl1(RfioTest.RfioCastorNewTurl1,XRootCase):
    def __init__(self, methodName):
        self.protocol = "root"
        XRootCase.__init__(self, methodName)

class XRootCastorNewTurl2(RfioTest.RfioCastorNewTurl2,XRootCase):
    def __init__(self, methodName):
        self.protocol = "root"
        XRootCase.__init__(self, methodName)

# TXNet suites

class XRootCastorNewTurl1Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(XRootCastorNewTurl1,casesXRoot))

class XRootCastorNewTurl2Suite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(XRootCastorNewTurl2,casesXRoot))

## Test Suite with all the TXNet cases

class XRootCastorNewTurlSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,(XRootCastorNewTurl1Suite(),XRootCastorNewTurl2Suite()))

