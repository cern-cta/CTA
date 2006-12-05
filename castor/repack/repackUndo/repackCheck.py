import os
import sys
import wrapcns
import getopt


try:
  import cx_Oracle
except:
  print "Fatal: couldn't load module cx_Oracle.Make sure it is installed and $PYTHONPATH includes the directory where cx_Oracle.so resides.\n"
  sys.exit(2)
  

# Retrieving tapes involved 


listOfTape=""
tapePool=""
reqId=""

try:
  res= getopt.getopt(sys.argv[1:],"R:V:P")
except:
  print "Error ... wrong parameters sintax"
  print "one and only one of the following options must be given:"
  print "-V vid1[:vid2:...]"
  print "-P tapePool"
  sys.exit(2)

if  len(res[0]) !=1:
  print "Error ... wrong parameters sintax"
  print "one and only one of the following options must be given:"
  print "-V vid1[:vid2:...]"
  print "-P tapePool"
  print "-R requestId"
  sys.exit(2)


if res[0][0][0] == '-V':
  listOfTape= res[0][0][1].split(":")
if res[0][0][0] == '-P':
  tapePool=res[0][0][1]
if res[0][0][0] == '-R':
  reqId=res[0][0][1]


# Retrieve information to access repack db

f=open("/etc/castor/REPACKCHECKCONFIG","r")
configFileInfo=f.read()
f.close
differentElem=configFileInfo.splitlines()
repackUser=(differentElem[0].split(" "))[1]
repackPass=(differentElem[1].split(" "))[1]
repackDb=(differentElem[2].split(" "))[1]


nsUser=(differentElem[3].split(" "))[1]
nsPass=(differentElem[4].split(" "))[1]
nsDb=(differentElem[5].split(" "))[1]

repackConnection= cx_Oracle.connect(repackUser+"/"+repackPass+"@"+repackDb)
repackCursor=repackConnection.cursor()

nsConnection= cx_Oracle.connect(nsUser+"/"+nsPass+"@"+nsDb)
nsCursor=nsConnection.cursor()

if tapePool!="" and listOfTape == "" and reqId == "":
  (inTape,outTape,errTape)=os.popen3("vmgrlisttape -P "+tapePool)
  errorMessage=errTape.read()
  if errorMessage != "":
    print "Error:"
    print errorMessage
    sys.exit(2)
  else:
    lineTape=outTape.read().splitlines()
    listOfTape=[]
    for singleLine in lineTape:
      listOfTape.append(singleLine.split(" ")[0])

if tapePool=="" and listOfTape == "" and reqId != "":
    requestQuery="select vid from repacksubrequest where cuuid = :1"
    repackCursor.execute(repackQry,(reqId,))
    requestResult=repackCursor.fetchall()
    listOfTape=[]
    for tape in requestResult:
      listOfTape.append(tape)
 
repackCheck1="select * from cns_seg_metadata  where vid =:1" 
repackCheck2="select * from repacksubrequest  where vid =:1"
repackCheck3="select fileid from repacksegment where vid in (select requestid from repacksubrequest where vid=:1)"
repackCheck4="select * from  cns_seg_metadata where s_fileid=:1 "

for tape in listOfTape:
  print
  print "*** TAPE involved: ",tape," ***"
  print

# is the tape repacked? => it is in the repack db

  repackCheck1="select * from repacksubrequest  where vid =:1"
  repackCursor.execute(repackCheck1,(tape,))
  repackResult=repackCursor.fetchall()
  if repackResult == []:
      print "tape not repacked"
      continue
  
# is the repacked tape empty now?
  nsCheck1="select * from cns_seg_metadata  where vid =:1"
  nsCursor.execute(nsCheck1,(tape,))
  nsResult=nsCursor.fetchall()
  if nsResult != []:
      print "tape not repacked completely ... still files on it"
      continue
  
# are all the files that where in the repacked tape still in a tape somewhere?


  repackCheck2="select fileid,copyno from repacksegment where vid in (select id from repacksubrequest where vid=:1)"
  repackCursor.execute(repackCheck2,(tape,))
  repackResult=repackCursor.fetchall()
  ok=0
  for file in repackResult:
      nsCheck2="select * from  cns_seg_metadata where s_fileid=:1 "
      nsCursor.execute(nsCheck2,(file[0],))
      nsResult=nsCursor.fetchall()
      if nsResult == []:
          print "Sorry file ",file[0]," is lost after the repack operation"
          ok=-1
      if len(nsResult) == 1 and nsResult[0][1] != file[1]:
          print "Sorry one of the two copies of file ",file[0]," is lost after  the repack process."
          ok=-1    
  if ok==0:
      print "No files lost after the repack of this tape."
      print
      
  reqQuery="select repacksubrequest.id  from repacksubrequest,repackrequest where vid= :1 and repacksubrequest.requestid=repackrequest.id and repackrequest.creationtime in (select max(creationtime) from (select * from repackrequest where id in (select requestid from repacksubrequest where vid=:1)))"    
  repackCursor.execute(reqQuery,(tape,))
  repackResult=repackCursor.fetchall()
  myReq=repackResult[0][0]
  
  queryMin="select fileid,fileseq from repacksegment where vid = :1 and fileseq in (select min(fileseq) from repacksegment where vid=:1)"
  repackCursor.execute(queryMin,(myReq,))
  minResult=repackCursor.fetchall()
  fileidMin=minResult[0][0]

  minFileName=((os.popen("nsGetPath lxs5012 %i"% (fileidMin)).read()).split())[0]
  minSize=((os.popen("nsls -l "+minFileName).read()).split())[4]
  print  minFileName+" chosen"
  
  #minOut=(os.popen4("rfcp "+ minFileName+" /tmp/minFile"+tape))[1].read()
  #if minOut.find(minSize+" bytes")== -1:
  #    print "sorry it is not possible to read the first file of the tape anymore."


  queryMax="select fileid,fileseq from repacksegment where vid = :1 and fileseq in (select max(fileseq) from repacksegment where vid=:1)"
  repackCursor.execute(queryMax,(myReq,))
  maxResult=repackCursor.fetchall()
  fileidMax=maxResult[0][0]

  maxFileName=((os.popen("nsGetPath lxs5012 %i"% (fileidMax)).read()).split())[0]
  maxSize=((os.popen("nsls -l "+maxFileName).read()).split())[4]
  print maxFileName+" chosen"
      
  #maxOut=(os.popen4("rfcp "+ maxFileName+" /tmp/maxFile"+tape))[1].read()
  #if maxOut.find(maxSize+" bytes")== -1:
  #    print "sorry it is not possible to read the last file of the tape anymore."
  
  fileSeqMiddle= (maxResult[0][1] - minResult[0][1])/2

  queryMiddle="select fileid from repacksegment where vid = :1 and fileseq= :2"
  repackCursor.execute(queryMiddle,(myReq,fileSeqMiddle,))
  middleResult=repackCursor.fetchall()
  fileidMiddle= middleResult[0][0]

  middleFileName=((os.popen("nsGetPath lxs5012 %i"% (fileidMiddle)).read()).split())[0]
  middleSize=((os.popen("nsls -l "+middleFileName).read()).split())[4]
  print  middleFileName+" chosen"
      

  #middleOut=(os.popen4("rfcp "+ middleFileName+" /tmp/middleFile"+tape))[1].read()
  #if middleOut.find(middleSize+" bytes")== -1:
  #    print "sorry it is not possible to read the file in the middle of the tape anymore."

       
print
