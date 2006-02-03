
import os
import threading
import time
import sys
import signal

newTime=time.gmtime()
ticket=str(((newTime[7]*24+newTime[3])*60 + newTime[4])*60 +newTime[5])  

def timeOutTest():
    print "Time Out"
    listProc=os.popen("ps").read().split("\n")
    for item in listProc:
        if item.find("stager_qry") != -1:
            os.system("kill -9 "+item[0:5])
            print "stager_qry is blocked"
        if item.find("rfcp") != -1:
            os.system("kill -9 "+item[0:5])
            print "rfcp is blocked"
        if item.find("stager_putdone") != -1:
            os.system("kill -9 "+item[0:5])
            print "stager_putdone is blocked"
    os._exit(0)  
        

def saveOnFile(namefile,myCmd):
    t=threading.Timer(120.0,timeOutTest)
    fin2=open("./tmpTest/readMe","a")
    fin2.write("executed "+myCmd+", the output is in ./tmpTest/"+namefile+"\n")
    fin2.close()
    t.start()
    fin=open("./tmpTest/"+namefile,"wb")
    fin.write(os.popen(myCmd).read())
    fin.close()
    t.cancel()
      
def runOnShell(cmdS):
    t=threading.Timer(120.0,timeOutTest)
    t.start()
    os.system(cmdS)
    t.cancel()

## START MAIN ##



if os.popen("ls").read().find("tmpTest") != -1:
    os.system("rm -r ./tmpTest") #delete old test information
     
os.mkdir("tmpTest") 
dirCastor=sys.argv[1]
tmpCastor=dirCastor+"Test"+ticket+"/"


if os.popen("nsls "+dirCastor).read().find("Test"+ticket) == -1:
    os.system("rfmkdir "+tmpCastor)
else:
    os.system("yes|rfrm -r"+tmpCastor)
    os.system("rfmkdir "+tmpCastor)
    


fiS=open("./tmpTest/readMe","wb")
fiS.write("\n\nThe sequence of commands is this:\n\n")
fiS.close()

# test base rfcp e stager_qry

saveOnFile("query0","stager_qry -M "+tmpCastor+"file1"+ticket)
saveOnFile("rfcp1","rfcp /etc/group "+tmpCastor+"file1"+ticket)
saveOnFile("rfcp2","rfcp "+tmpCastor+"file1"+ticket+" "+tmpCastor+"file2"+ticket)

saveOnFile("rfcp3","rfcp "+tmpCastor+"file1"+ticket+"  ./tmpTest/file1Copy")
saveOnFile("rfcp4","rfcp "+tmpCastor+"file2"+ticket+"  ./tmpTest/file2Copy")

saveOnFile("diff1","diff /etc/group ./tmpTest/file1Copy")
saveOnFile("diff2","diff /etc/group ./tmpTest/file2Copy")

saveOnFile("query1","stager_qry -M "+tmpCastor+"file1"+ticket)

# test stager_ put stager_get stager_putdone

saveOnFile("put1","stager_put -M "+tmpCastor+"file3"+ticket+" -U tag"+ticket)
saveOnFile("query2","stager_qry -M "+tmpCastor+"file3"+ticket)
saveOnFile("query3","stager_qry -U tag"+ticket)

saveOnFile("rfcp5","rfcp /etc/group "+tmpCastor+"file3"+ticket)


# otherwise permission are --- --- --- instead of -rw -r- -r- 

runOnShell("rfchmod 644 "+tmpCastor+"file3"+ticket)


saveOnFile("rfcp6","rfcp "+tmpCastor+"file3"+ticket+" ./tmpTest/file3Copy")
saveOnFile("diff3","diff /etc/group ./tmpTest/file3Copy")

saveOnFile("query4","stager_qry -M "+tmpCastor+"file3"+ticket) 
saveOnFile("query5","stager_qry -U tag"+ticket)

saveOnFile("get1","stager_get -M "+tmpCastor+"file3"+ticket+" -U getTag"+ticket)
saveOnFile("query6","stager_qry -U getTag"+ticket)
saveOnFile("query7","stager_qry  -M "+tmpCastor+"file3"+ticket)

saveOnFile("putDone1","stager_putdone -M "+tmpCastor+"file3"+ticket)
saveOnFile("query8","stager_qry -U getTag"+ticket)
saveOnFile("query9","stager_qry -U tag"+ticket)

saveOnFile("get2","stager_get -M"+tmpCastor+"file3"+ticket)
saveOnFile("get3","stager_get -M"+tmpCastor+"file3"+ticket+" -U getTag2"+ticket)
saveOnFile("query10","stager_qry -U getTag2"+ticket)


# test stager_rm

saveOnFile("rem1","stager_rm -M"+tmpCastor+"file1"+ticket)

fi=open("./tmpTest/rem1","r")
if(fi.read().find("being replicated") != -1): # wait that the copy is done
    fi.close()
    time.sleep(5)
    saveOnFile("rem1","stager_rm -M"+tmpCastor+"file1"+ticket)
else:
    fi.close()

saveOnFile("rem2","stager_rm -M"+tmpCastor+"file3"+ticket)

fi=open("./tmpTest/rem2","r")
if(fi.read().find("being replicated") != -1): # wait that the copy is done
    fi.close()
    time.sleep(5)
    saveOnFile("rem2","stager_rm -M"+tmpCastor+"file1"+ticket)
else:
    fi.close()
    
saveOnFile("query11","stager_qry -M"+tmpCastor+"file1"+ticket)
saveOnFile("query12","stager_qry -M"+tmpCastor+"file3"+ticket)

signal.signal(signal.SIGPIPE, signal.SIG_DFL) # against broken pipe
runOnShell("yes|rfrm -r "+tmpCastor)
