import os
import sys
import unittest
import string

import ClientTest
import RfioTest
import TapeTest
#import DlfTest
#import CommonTest
#import CoreTest

import signal

########################### for each module its own test suite ##########################

class ClientSuite(unittest.TestSuite):
    def __init__(self):    
	unittest.TestSuite.__init__(self)

class RfioSuite(unittest.TestSuite):
    def __init__(self):    
	unittest.TestSuite.__init__(self)

class TapeSuite(unittest.TestSuite):
    def __init__(self):    
	unittest.TestSuite.__init__(self)

class DlfSuite(unittest.TestSuite):
    def __init__(self):    
	unittest.TestSuite.__init__(self)

class CommonSuite(unittest.TestSuite):
    def __init__(self):    
	unittest.TestSuite.__init__(self)
	
class CoreSuite(unittest.TestSuite):
    def __init__(self):    
	unittest.TestSuite.__init__(self)		

#####  allCastorSuites is a dictonary with a test suite for each module ###########
	
allCastorSuites= {'CLIENT':ClientSuite(),'RFIO':RfioSuite(),'TAPE':TapeSuite(),'DLF':DlfSuite(),'COMMON':CommonSuite(),'CORE':CoreSuite()}

################ for each module all the possible test suites included #######################

clientTest={'PREREQ': ClientTest.StagerPreClientSuite(),'PUT':ClientTest.StagerPutSuite(),'PUTDONE':ClientTest.StagerPutDoneSuite(),'GET':ClientTest.StagerGetSuite(),'RM':ClientTest.StagerRmSuite(),'EXTRAQRY':ClientTest.StagerQuerySpecialSuite(),'EXTRATEST':ClientTest.StagerExtraTestSuite()}

rfioTest={'PREREQ':RfioTest.RfioPreRequisitesSuite(),'BASIC_RFCP':RfioTest.RfioRfcpSimpleSuite(),'BASIC_OTHERCMD':RfioTest.RfioOtherCmdSimpleSuite(),'CASTOR_RFCP':RfioTest.RfioRfcpEnvSuite() ,'CASTOR_RFCP_NEW_TURL': RfioTest.RfioRfcpNewTurlSuite(),'CASTOR_OTHERCMD':RfioTest.RfioOtherCmdEnvSuite() ,'CASTOR_OTHERCMD_NEW_TURL': RfioTest.RfioOtherCmdNewTurlSuite(),'API': RfioTest.RfioApiSuite(), 'STRESS':RfioTest.RfioApiSuite()}

tapeTest={'PREREQ':TapeTest.TapePreSuite(),'MIGRATION':TapeTest.TapeMigrationSuite(),'RECALL':TapeTest.TapeRecallSuite(),'MIGRATION_AND_RECALL':TapeTest.TapeMigrationAndRecallSuite(), 'STRESS_CASTOR':TapeTest.TapeStressCastorSuite(),'TAPE_ONLY':TapeTest.TapeTapeOnlySuite()}

dlfTest={'PREREQ':0,'TEST':0}


commonTest={'PREREQ':0,'TEST':0}

coreTest={'PREREQ':0,'TEST':0}

listOfTest={'CLIENT':clientTest,'RFIO':rfioTest,'TAPE':tapeTest,'DLF':dlfTest,'COMMON':commonTest,'CORE':coreTest}


#################### opening the file with the list of tests wanted  ################################

f=open("./CASTORTESTCONFIG","r")
configFileInfo=f.read()
f.close

index= configFileInfo.find("*** Test choice ***")
configFileInfo=configFileInfo[index:]
configFileInfo=configFileInfo.strip("*** Test choice ***\n\n")
index=configFileInfo.find("***")
if index != -1:
    index=index-1
    configFileInfo=configFileInfo[:index]

newSuites=configFileInfo.split("\n\n")

for mySuite in newSuites:
    # at least a test of that group => I run the PreReqTests
    if mySuite.find("YES") !=-1:
         testSuiteName=(mySuite.split())[0]
         testToAdd=(listOfTest[testSuiteName])["PREREQ"]
         if testToAdd != 0:
             (allCastorSuites[testSuiteName]).addTest(testToAdd)
             
    newCases=mySuite.splitlines()         
    for myCase in newCases:
       	if myCase.find("YES") != -1:
            elem=myCase.split()
            testToAdd=(listOfTest[elem[0]])[elem[1]]
            if testToAdd != 0:
                (allCastorSuites[elem[0]]).addTest(testToAdd)
            

############### this is the global test suite with all the tests required in the CASTORTESTCONFIG ####################

class CastorGlobalSuite(unittest.TestSuite):
    def __init__(self):    
	unittest.TestSuite.__init__(self)


try:
    myGlobalSuite=CastorGlobalSuite()
    for differentSuite in allCastorSuites:
	myGlobalSuite.addTest(allCastorSuites[differentSuite])
		
    runner=unittest.TextTestRunner(verbosity=2)
    runner.run(myGlobalSuite)
    os._exit(0)
except KeyboardInterrupt:
    print "\nTests have been interrupted by a keyboard interrupt."
    os._exit(-1)
