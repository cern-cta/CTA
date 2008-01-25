import unittest
import os
import sys
import time
from threading import Thread
import signal
import thread
import UtilityForCastorTest
from UtilityForCastorTest import stagerHost,stagerPort,stagerSvcClass,stagerVersion,stagerExtraSvcClass,stagerDiskOnlySvcClass,stagerForcedFileClass,configFile,quietMode,outputDir

endThread=0
# global variable to avoid the join, due to thread limitation in handling signals

# to be able to handle correctly the SIGINT from a thread different from the main one

def handlerKeyInt(signum, frame):
    raise KeyboardInterrupt, "Keyboard interrupt"

    
ticket="" 
dirCastor=""
inputFile=""
localDir=""
recallDir=""
myScen=""
myTag=""
numFiles=""
filesToBeRecalled=""

def getFilesToRecall(maxNum=None):
    tmpList=os.popen("nsls "+recallDir).read().split("\n")
    count=0
    defList=[]
    for file in tmpList:
        if maxNum == None:
            defList.append(recallDir+file)
        else:
            if count<maxNum:
                count=count+1
                defList.append(recallDir+file)
            else:
                return defList
    return defList

# This classes will be recycled and extended (optimization needed)... integrate the same in repackCheck

class RecallingThread(Thread):
   def __init__ (self,listFile,num,dirOut):
       Thread.__init__(self)
       self.listFile=listFile
       self.num=num
       self.response=""
       self.dirOut=dirOut
      
   def run(self):
       recalledRight=0
       recallFailed=0
       strFiles=""
       for file in self.listFile:
           strFiles=strFiles+" -M "+file
           
       UtilityForCastorTest.runOnShell(["stager_rm "+strFiles,"stager_get "+strFiles],myScen)
       
       while (recalledRight + recallFailed) != self.num:
           for i in range(self.num):
               if self.listFile[i]== -1:
                   continue
               qryOut=UtilityForCastorTest.runOnShell(["stager_qry -M "+self.listFile[i]],myScen)           
               print qryOut[0]
               if qryOut[0].find("No such file or directory")!=-1:
                   time.sleep(60)
                   qryOut=UtilityForCastorTest.runOnShell(["stager_qry -M "+self.listFile[i]],myScen)
                        
               if qryOut[0].find("INVALID")!=-1 or qryOut[0].find("No such file or directory")!=-1:
                   self.listFile[i]== -1
                   recallFailed= recallFailed+1
               else:
                   if qryOut[0].find("STAGED")!=-1:
                       outSize=((os.popen("nsls -l "+self.listFile[i]).read()).split())[4]
                       fileOut=UtilityForCastorTest.runOnShell(["rfcp "+self.listFile[i]+" "+self.dirOut+"file"+str(i)+"copy"],myScen)
                       
                       if fileOut[0].find(str(outSize)+" bytes") == -1:
                           recallFailed=recallFailed+1
                           self.listFile[i]=-1
                       else:
                           self.listFile[i]=-1
                           recalledRight=recalledRight+1
                           os.system("rm "+self.dirOut+"file"+str(i)+"copy")
               time.sleep(60)
                        
       if recalledRight == self.num:
           self.response="All files have been retrieved correctly"
       else:
           self.response=str(recalledRight)+" files recalled "+str(recallFailed)+" files not recalled"
       print self.response
       
       global endThread # to replace the join() in the main thread
       endThread=1

           
class MigratingThread(Thread):
   def __init__ (self,listFile,num):
       Thread.__init__(self)
       self.listFile=listFile
       self.num=num
       self.response=""

   def run(self):
       migratedRight=0
       migratedFailed=0
       time.sleep(30*self.num);
       while (migratedRight+migratedFailed) != self.num:
           for i in range(self.num):
               if self.listFile[i]== -1:
                   continue
               qryOut=UtilityForCastorTest.runOnShell(["stager_qry -M "+self.listFile[i]],myScen)
               print qryOut[0]
               if qryOut[0].find("INVALID")!=-1 or  qryOut[0].find("No such file or directory")!=-1:
                   migratedFailed=migratedFailed+1
                   self.listFile[i]= -1
			
               else:
                   if qryOut[0].find("STAGED")!=-1:
                       self.listFile[i]= -1
                       migratedRight=migratedRight+1
               time.sleep(30)
           
       if migratedRight == self.num:
           self.response="All files have been migrated correctly"
       else:
           self.response=str(migratedRight)+" files migrated and "+str(migratedFailed)+" didn't migrate"

       print self.response
       global endThread  # to replace the join in the main thread
       endThread=1
       
class PreRequisitesCase(unittest.TestCase):
    def mainScenarium(self):
        assert (UtilityForCastorTest.checkUser() != -1), "you don't have a valid castor directory"
        global ticket
        ticket=UtilityForCastorTest.getTicket()
        global dirCastor
        dirCastor=outputDir+"tmpTapeTest"+ticket+"/"
        global myScen
        myScen=UtilityForCastorTest.createScenarium(stagerHost,stagerPort,stagerSvcClass,stagerVersion)
        try:
            f=open(configFile,"r")
            configFileInfo=f.read()
            f.close
        except IOError:
            assert 0==-1, "An error in the preparation of the main setting occurred ... test is not valid"
                        
        index= configFileInfo.find("*** Tape test specific parameters ***")
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
        global recallDir
        recallDir=(configFileInfo[configFileInfo.find("INPUT_CASTOR_DIR"):]).split()[1]
        UtilityForCastorTest.runOnShell(["nsmkdir "+dirCastor],myScen)
        
    def stagerRfioFine(self):
		[ret,out]=UtilityForCastorTest.testCastorBasicFunctionality(inputFile,dirCastor+"TapePreReq"+ticket,localDir,myScen)
		assert ret==0, out
                

class TapeBasicMigrationCase(unittest.TestCase):
        def simpleMigration(self):
            global endThread
            endThread=0
            signal.signal(signal.SIGINT,handlerKeyInt) #to handle the SIGINT correctly
            
            cmd=[]
            inputMigration=[]
            for i in range(numFiles):
                cmd.append("rfcp "+inputFile+" "+dirCastor+"fileSimpleMigr"+str(i)+myTag+ticket)               
                inputMigration.append(dirCastor+"fileSimpleMigr"+str(i)+myTag+ticket)
            UtilityForCastorTest.runOnShell(cmd,myScen)

            migration=MigratingThread(inputMigration,numFiles)
            migration.start()
            
            while endThread == 0:
                time.sleep(60)
            endThread = 0
             
            assert migration.response.find("All files have been migrated correctly") != -1, "Problems in migrating files: "+migration.response
	
        def completeMigration(self):
            global endThread
            endThread=0
            signal.signal(signal.SIGINT,handlerKeyInt) #to handle the SIGINT correctly
            
            cmd=[]
            inputMigration=[]
            inputForCastor=""
            for i in range(numFiles):
                cmd=["rfcp "+inputFile+" "+dirCastor+"fileMultMigr"+str(i)+myTag+ticket]
                inputMigration.append(dirCastor+"fileMultMigr"+str(i)+myTag+ticket)
                inputForCastor=" -M "+dirCastor+"fileMultMigr"+str(i)+myTag+ticket
                cmdComplete=["stager_put "+ inputForCastor]+cmd+["stager_putdone "+inputForCastor]
                
                UtilityForCastorTest.runOnShell(cmdComplete,myScen)

            signal.signal(signal.SIGINT, handlerKeyInt)
            migration=MigratingThread(inputMigration,numFiles)
            migration.start()
            
            while endThread == 0:
                time.sleep(60)
            endThread = 0
            
            assert migration.response.find("All files have been migrated correctly") != -1, "Problems in migrating files: "+migration.response
	
class TapeBasicRecallCase(unittest.TestCase):
    def simpleRecall(self):
        global endThread
        endThread=0
        signal.signal(signal.SIGINT,handlerKeyInt) #to handle the SIGINT correctly        
        recalling=RecallingThread(filesToBeRecalled,numFiles,localDir)
        recalling.start()
        while endThread == 0:
            time.sleep(60)
        endThread=0 
        
	assert recalling.response.find("All files have been retrieved correctly") != -1,"Problems in recalling files"
           
class TapeBasicMigrationAndRecallCase(unittest.TestCase):
    def simpleMigrationAndRecall(self):
        global endThread
        endThread=0
        signal.signal(signal.SIGINT,handlerKeyInt) #to handle the SIGINT correctly
        
        cmd=[]
        inputMigration=[]
        inputRecall=[]
        inputRecallForCastor=""

        for i in range(numFiles):
            cmd.append("rfcp "+inputFile+" "+dirCastor+"fileMigrRec"+str(i)+myTag+ticket)
            inputMigration.append(dirCastor+"fileMigrRec"+str(i)+myTag+ticket)
            inputRecall.append(dirCastor+"fileMigrRec"+str(i)+myTag+ticket)

        UtilityForCastorTest.runOnShell(cmd,myScen)

        migration=MigratingThread(inputMigration,numFiles)
        migration.start()
        
        while endThread == 0:
            time.sleep(60)
        endThread=0
        
        assert migration.response.find("All files have been migrated correctly") != -1, "Problems in migrating files"

        recalling=RecallingThread(inputRecall,numFiles,localDir)
        recalling.start()

        while endThread == 0:
            time.sleep(60)
        endThread=0

        assert recalling.response.find("All files have been retrieved correctly") != -1, "Problems in migrating files"        
    def completeMigrationAndRecall(self):
        global endThread
        endThread=0
        signal.signal(signal.SIGINT,handlerKeyInt) #to handle the SIGINT correctly
        
        cmd=[]
        inputMigration=[]
        inputRecall=[]
        inputRecallForCastor=""
                
        for i in range(numFiles):
            cmd=["rfcp "+inputFile+" "+dirCastor+"fileMigrRecComplete"+str(i)+myTag+ticket]
            inputMigration.append(dirCastor+"fileMigrRecComplete"+str(i)+myTag+ticket)
            inputRecall.append(dirCastor+"fileMigrRecComplete"+str(i)+myTag+ticket)
            cmdComplete=["stager_put -M "+dirCastor+"fileMigrRecComplete"+str(i)+myTag+ticket]+cmd+["stager_putdone -M "+dirCastor+"fileMigrRecComplete"+str(i)+myTag+ticket]
            UtilityForCastorTest.runOnShell(cmdComplete,myScen)

        
        migration=MigratingThread(inputMigration,numFiles)
        migration.start()

        while endThread == 0:
            time.sleep(60)
        endThread=0
        
        assert migration.response.find("All files have been migrated correctly") != -1, "Problems in migrating files"
    
        recalling=RecallingThread(inputRecall,numFiles,localDir)
        recalling.start()

        while endThread == 0:
            time.sleep(60)
        endThread=0
        
        assert recalling.response.find("All files have been retrieved correctly") != -1, "Problems in migrating files"

class TapeOneFileCase(unittest.TestCase):
        def setUp(self):
            global tag
            tag="TapeOneFile"
            global filesToBeRecalled
            filesToBeRecalled=getFilesToRecall(1)
            global numFiles
            numFiles=len(filesToBeRecalled)
            global myTag
            myTag="OneFile"
            
class TapeStandardCase(unittest.TestCase):
        def setUp(self):
            global tag
            tag="TapeBasicFile"
            global filesToBeRecalled
            global numFiles
            filesToBeRecalled=getFilesToRecall(10)
            numFiles=len(filesToBeRecalled)
            global myTag
            myTag="Standard"
            
            
class TapeStressCase(unittest.TestCase):
        def setUp(self):
            global tag
            tag="TapeStressFile"
            global filesToBeRecalled
            filesToBeRecalled=getFilesToRecall()
            global numFiles
            numFiles=len(filesToBeRecalled)
            global myTag
            myTag="Stress"

# test with one file

class  TapeMigrationOneCase(TapeOneFileCase,TapeBasicMigrationCase):
    pass
class  TapeRecallOneCase(TapeOneFileCase, TapeBasicRecallCase):
    pass
class  TapeMigrationAndRecallOneCase(TapeOneFileCase,TapeBasicMigrationAndRecallCase):
    pass

# test with some files (maximum 10)

class  TapeMigrationStandardCase(TapeStandardCase,TapeBasicMigrationCase):
    pass
class  TapeRecallStandardCase(TapeStandardCase,TapeBasicRecallCase):
    pass
class  TapeMigrationAndRecallStandardCase(TapeStandardCase,TapeBasicMigrationAndRecallCase):
    pass

# test with a lot of files (same number in the one given in the directory of files to recall)

class  TapeMigrationStressCase(TapeStressCase,TapeBasicMigrationCase):
    pass
class  TapeRecallStressCase(TapeStressCase,TapeBasicRecallCase):
    pass
class  TapeMigrationAndRecallStressCase(TapeStressCase,TapeBasicMigrationAndRecallCase):
    pass

# test for tape only


class TapeTapeOnlyCase(unittest.TestCase):
    pass
	
casesPreTape=("mainScenarium","stagerRfioFine")
casesMigration=("simpleMigration","completeMigration")
casesRecall="simpleRecall"
casesMigrationAndRecall=("simpleMigrationAndRecall","completeMigrationAndRecall")
casesTapeOnly=("")

# Migration Suites

class TapeMigrationOneFileSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(TapeMigrationOneCase,casesMigration))

class TapeMigrationStandardSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(TapeMigrationStandardCase,casesMigration))

class TapeMigrationStressSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(TapeMigrationStressCase,casesMigration))
        

# Recall Suites

class TapeRecallOneFileSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,[TapeRecallOneCase(casesRecall)])

class TapeRecallStandardSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,[TapeRecallStandardCase(casesRecall)])

class TapeRecallStressSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,[TapeRecallStressCase(casesRecall)])

# Migration and Recall Suites 

class TapeMigrationAndRecallOneFileSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(TapeMigrationAndRecallOneCase,casesMigrationAndRecall))

class TapeMigrationAndRecallStandardSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(TapeMigrationAndRecallStandardCase,casesMigrationAndRecall))

class TapeMigrationAndRecallStressSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(TapeMigrationAndRecallStressCase,casesMigrationAndRecall))


# Global Suite used from CastorTestSuite.py

# PREREQ

class TapePreSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(PreRequisitesCase,casesPreTape))
        
# MIGRATION

class TapeMigrationSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,(TapeMigrationOneFileSuite(),TapeMigrationStandardSuite()))
        
# RECALL

class TapeRecallSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,(TapeRecallOneFileSuite(),TapeRecallStandardSuite()))

# MIGRATION_AND_RECALL
        
class TapeMigrationAndRecallSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,(TapeMigrationAndRecallOneFileSuite(),TapeMigrationAndRecallStandardSuite()))

# STRESS_CASTOR

class TapeStressCastorSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,(TapeMigrationStressSuite(), TapeRecallStressSuite(),TapeMigrationAndRecallStressSuite()))

# TAPE_ONLY

class TapeTapeOnlySuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(TapeTapeOnlyCase,casesTapeOnly))
	
