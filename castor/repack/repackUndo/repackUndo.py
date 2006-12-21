import os
import sys
import wrapcns
import getopt

try:
  import cx_Oracle
except:
  print "Fatal: couldn't load module cx_Oracle.Make sure it is installed and $PYTHONPATH includes the directory where cx_Oracle.so resides.\n"
  sys.exit(2)
  
def segmentByFileId(elem1,elem2):
  return elem1[1]-elem2[1]
  


# Retrieving tapes involved into the repack undo


listOfTape=""
tapePool=""


try:
  res= getopt.getopt(sys.argv[1:],"V:P:")
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
  sys.exit(2)


if res[0][0][0] == '-V':
  listOfTape= res[0][0][1].split(":")
if res[0][0][0] == '-P':
  tapePool=res[0][0][1]

if tapePool!="" and listOfTape == "":
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

# Retrieve information to access repack db

f=open("/etc/castor/REPACKUNDOCONFIG","r")
configFileInfo=f.read()
f.close
differentElem=configFileInfo.splitlines()
repackUser=(differentElem[0].split(" "))[1]
repackPass=(differentElem[1].split(" "))[1]
repackDb=(differentElem[2].split(" "))[1]


repackConnection= cx_Oracle.connect(repackUser+"/"+repackPass+"@"+repackDb)
repackCursor=repackConnection.cursor()

repackQry="select fileid,compression,segsize,filesec,copyno,blockid,fileseq,id,vid  from repacksegment where vid in (select repacksubrequest.id from repacksubrequest,repackrequest where repacksubrequest.requestid=repackrequest.id and repackrequest.creationtime in (select max(creationtime) from (select * from repackrequest where id in (select requestid from repacksubrequest where vid=:1))) and repacksubrequest.requestid in (select requestid from repacksubrequest where vid=:2))order by fileid"


for tape in listOfTape:
  print
  print "*** TAPE involved: ",tape," ***"
  print

  # for each tape I retrieve the fileid saved into the repack db

  repackCursor.execute(repackQry,(tape,tape))
  repackResult=repackCursor.fetchall()
  listSegmentBeforeUndo=[]
  listSegmentAfterUndo=[]
  for (fileidRep,compressionRep,segsizeRep,filesecRep,copynoRep,blockidRep,fileseqRep,idRep,vidRep) in repackResult:

  # Getting information from the ns for one file at the time
    (ret,infoSeg)=wrapcns.cnsGetSegInfo(fileidRep,copynoRep)
    
    if ret <0:
      listSegmentBeforeUndo.append((-1,fileidRep))
    else:
      listSegmentBeforeUndo.append((0,fileidRep,infoSeg))


  retSetOperation=wrapcns.cnsSetSegInfo(tape)

  for (fileidRep,compressionRep,segsizeRep,filesecRep,copynoRep,blockidRep,fileseqRep,idRep,vidRep)in repackResult:

  # Getting information from the ns for one file at the time

    (ret,infoSeg)=wrapcns.cnsGetSegInfo(fileidRep,copynoRep)
    
    if ret <0:
      listSegmentAfterUndo.append((-1,fileidRep))
    else:
      listSegmentAfterUndo.append((0,fileidRep,infoSeg))

  #print the output FINIRE 

  listSegmentBeforeUndo.sort(segmentByFileId)
  listSegmentAfterUndo.sort(segmentByFileId)
  retSetOperation.sort(segmentByFileId)

  i=0;
  for elem in  listSegmentBeforeUndo:
    print  "* File Id ",elem[1]
    print
    print "   Values from nameserver:"
    if elem[0]==-1:
      print "   problems to retrieve this file"
    else:
      print "   ",elem[2]
    if  retSetOperation[0][i][0]== -1 :
      print "   problems in setting information for file ",retSetOperation[0][i][1]
    else:
       print "   Values from repack for fileid ",repackResult[i][0],":"
       print "    [[",repackResult[i][3],", ",repackResult[i][4],", ",repackResult[i][2],", ",repackResult[i][1],", ?, ",repackResult[i][8],", ?, ",repackResult[i][6],", ",repackResult[i][5],", ?, ? ]]"
       
    print "   Values from nameserver after the repack undo:"
    if listSegmentAfterUndo[i][0]== -1:  
      print "   problems to retrieve this file"
    else:
      print "   ",listSegmentAfterUndo[i][2]
    print
    i=i+1
    
    

    
      
    
