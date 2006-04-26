
import unittest
import os
import sys



class StagerTestCase(unittest.TestCase):
        
    def checkCopy(self):
        assert os.stat("./tmpTest/diff1")[6] == 0, "Rfcp doesn't work"
        assert os.stat("./tmpTest/diff2")[6] == 0, "Rfcp doesn't work"
        assert os.stat("./tmpTest/diff3")[6] == 0, "Rfcp doesn't work"
        
    def checkQuery(self):

        fi=open("./tmpTest/rem1","r")
        flagR1=1
        if (fi.read().find("SUBREQUEST_READY")== -1):
            flagR1=0
        fi.close()

        fi=open("./tmpTest/rem2","r")
        flagR2=1
        if (fi.read().find("SUBREQUEST_READY")== -1):
            flagR2=0
        fi.close()
        
        fi=open("./tmpTest/putDone1","r")
        flagP=1
        if (fi.read().find("error=0")== -1):
            flagP=0
        fi.close()
        
        fi=open("./tmpTest/query0","r")
        assert fi.read().find("not in stager")!= -1, "stager_qry doesn't work if there isn't the file"
        fi.close()
        
        fi=open("./tmpTest/query1","r")
        assert fi.read().find("CANBEMIGR") != -1, "stager_qry doesn't work"
        fi.close()
             
        fi=open("./tmpTest/query2","r")
        assert fi.read().find("No such file or directory") != -1, "stager_qry doesn't work"
        fi.close()

        
        fi=open("./tmpTest/query7","r")
        assert fi.read().find("STAGEOUT") != -1, "stager_qry doesn't work"
        fi.close()

        fi=open("./tmpTest/query8","r")
        assert fi.read().find("CANBEMIGR") != -1, "stager_qry doesn't work"
        fi.close()

        fi=open("./tmpTest/query10","r")
        flag=0
        resp=fi.read()
        if (resp.find("CANBEMIGR") != -1 or resp.find("CANBEMIGR") != -1):
           flag=1
        if (resp.find("STAGEOUT")!= -1 and not flagP):
           flag=1                                                          
        assert flag==1, "stager_qry doesn't work"
        fi.close()


        fi=open("./tmpTest/query9","r")
        flag=0
        resp=fi.read()
        if ((resp.find("CANBEMIGR") != -1) and flagP):
            flag=1
        if (resp.find("STAGEOUT")!= -1 and not flagP):
            flag=1                                                          
        assert flag==1, "stager_qry doesn't work"
        fi.close()
            
        fi=open("./tmpTest/query11","r")
        flag=0
        if flagR1 and (fi.read().find("No such file")):
            flag=1
        if (not flagR1):
            flag=1
        assert flag==1, "stager_qry query doesn't work"
        fi.close()

        fi=open("./tmpTest/query12","r")
        flag=0
        if flagR2 and (fi.read().find("No such file")):
            flag=1
        if (not flagR2):
            flag=1    
        assert flag==1, "stager_qry query doesn't work"
        fi.close()

    def checkPut(self):
        fi=open("./tmpTest/put1","r")
        assert fi.read().find("SUBREQUEST_READY") != -1, "put doesn't work"
        fi.close()
        
    def checkPutDone(self):
        fi=open("./tmpTest/putDone1","r")
        assert fi.read().find("error=0") != -1, "putDone doesn't work"
        fi.close()
    
    def checkGet(self):
        for n in (1,2,3):
            fi=open("./tmpTest/get"+str(n),"r")
            assert fi.read().find("SUBREQUEST_READY") != -1, "stager_get doesn't work"
            fi.close()
    def checkRm(self):
        
	fir=open("./tmpTest/query1","r")
        flagR1=0
        if (fir.read().find("STAGED")!= -1):
            flagR1=1
        fir.close()
	
	fir=open("./tmpTest/query10","r")
        flagR2=0
	resp=fir.read()
        if (resp.find("STAGED")!= -1 or resp.find("STAGEOUT") !=-1):
            flagR2=1
        fir.close()
	
	fi=open("./tmpTest/rem1","r")
	resp=fi.read()
        assert (resp.find("SUBREQUEST_READY") != -1 and flagR1==1)or(resp.find("SUBREQUEST_FAILED") != -1 and flagR1==0), "stager_rm doesn't work"
	
        fi.close()
	
        fi=open("./tmpTest/rem2","r")
        resp=fi.read()
	assert (resp.find("SUBREQUEST_READY") != -1 and flagR2==1)or(resp.find("SUBREQUEST_FAILED") != -1 and flagR2==0), "stager_rm doesn't work"
        fi.close()
        
cases=("checkCopy","checkQuery","checkPut","checkRm","checkPutDone","checkGet")

class StagerTestSuite(unittest.TestSuite):
    def __init__(self):
        unittest.TestSuite.__init__(self,map(StagerTestCase,cases))



#print "start the test"

#if (sys.argv[1]!=0):
#    dirRem="/castor/cern.ch/user/"+sys.argv[1][0]+"/"+sys.argv[1]
#    if (os.popen("nsls "+dirRem).read().find("No such file or directory")!= -1):
#        print sys.argv[1]+" does not exist as user."
#        print "Try again"
#        os._exit(0)
#else:
#    print "You have to insert your user id"
#    print "Usage: python myTestSuite.py userId"
#    print "e.g. python myTestSuite.py gtaur"
#    os._exit(0)

#os.system("python test1.py "+dirRem)

mySuite=StagerTestSuite()
runner=unittest.TextTestRunner()
runner.run(mySuite)

print "For more details, look in ./tmpTest, it will be deleted if you run test again." 

#os.system("rm -r tmpTest")

