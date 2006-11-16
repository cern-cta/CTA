import os
import sys
import unittest
import string
import ClientTest
import RfioTest


########################### for each module its own test suite ##########################

class CommonSuite(unittest.TestSuite):
    def __init__(self):    
	unittest.TestSuite.__init__(self)

class ServerSuite(unittest.TestSuite):
    def __init__(self):    
	unittest.TestSuite.__init__(self)

class ClientSuite(unittest.TestSuite):
    def __init__(self):    
	unittest.TestSuite.__init__(self)

class RfioSuite(unittest.TestSuite):
    def __init__(self):    
	unittest.TestSuite.__init__(self)

class DlfSuite(unittest.TestSuite):
    def __init__(self):    
	unittest.TestSuite.__init__(self)
	
class CsvcsSuite(unittest.TestSuite):
    def __init__(self):    
	unittest.TestSuite.__init__(self)	
	
class DisksvrSuite(unittest.TestSuite):
    def __init__(self):    
	unittest.TestSuite.__init__(self)	
	
class TapeSuite(unittest.TestSuite):
    def __init__(self):    
	unittest.TestSuite.__init__(self)	
	
class AdminSuite(unittest.TestSuite):
    def __init__(self):    
	unittest.TestSuite.__init__(self)
	

#####  allCastorSuites is a dictonary with a test suite for each module ###########
	
allCastorSuites={'COMMON':CommonSuite(),'SERVER':ServerSuite(),'CLIENT':ClientSuite(),'RFIO':RfioSuite(),'DLF':DlfSuite(),'CSVC':CsvcsSuite(),'DISKSVR':DisksvrSuite(),'TAPE':TapeSuite(),'ADMIN':AdminSuite()}


################ for each module all the possible test suites included #######################

commonTest={'TEST':0}

serverTest={'TEST':0}

clientTest={'PREREQ': ClientTest.StagerPreClientSuite(),'PUT':ClientTest.StagerPutSuite(),'PUTDONE':ClientTest.StagerPutDoneSuite(),'GET':ClientTest.StagerGetSuite(),'RM':ClientTest.StagerRmSuite(),'EXTRAQRY':ClientTest.StagerQuerySpecialSuite()}

rfioTest={'PREREQ':RfioTest.RfioPreRequisitesSuite(),'BASIC_RFCP':RfioTest.RfioRfcpSimpleSuite(),'BASIC_OTHERCMD':RfioTest.RfioOtherCmdSimpleSuite(),'CASTOR_RFCP':RfioTest.RfioRfcpEnvSuite() ,'CASTOR_RFCP_FANCY_TURL': RfioTest.RfioRfcpFancyTurlSuite(),'CASTOR_OTHERCMD':RfioTest.RfioOtherCmdEnvSuite() ,'CASTOR_OTHERCMD_FANCY_TURL': RfioTest.RfioOtherCmdFancyTurlSuite()}

dlfTest={'TEST':0}

csvcsTest={'TEST':0}

disksvrTest={'TEST':0}

tapeTest={'TEST':0}

adminTest={'TEST':0}

listOfTest={'COMMON':commonTest,'SERVER':serverTest,'CLIENT':clientTest,'RFIO':rfioTest,'DLF':dlfTest,'CSVCS':csvcsTest,'DISKSVR':disksvrTest,'TAPE':tapeTest,'ADMIN':adminTest}


#################### opening the file with the list of tests wanted  ################################

f=open("/etc/castor/CASTORTESTCONFIG","r")
configFileInfo=f.read()
f.close

index= configFileInfo.find("*** Test choice ***")
configFileInfo=configFileInfo[index:]
configFileInfo=configFileInfo.strip("*** Test choice ***\n\n")
index=configFileInfo.find("***")
index=index-1
configFileInfo=configFileInfo[:index]


newSuites=configFileInfo.split("\n\n")

for mySuite in newSuites:
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


myGlobalSuite=CastorGlobalSuite()
for differentSuite in allCastorSuites:
	myGlobalSuite.addTest(allCastorSuites[differentSuite])
		
runner=unittest.TextTestRunner(verbosity=2)
runner.run(myGlobalSuite)
os._exit(0)
