import os
import sys
import wrapcns
import getopt

try:
  import cx_Oracle
except:
  print "Fatal: couldn't load module cx_Oracle.Make sure it is installed and $PYTHONPATH includes the directory where cx_Oracle.so resides.\n"
  sys.exit(2)
  

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
  (inTape,outTape,errTape)=os.popen3("/usr/local/src/CASTOR2/WORKDIR/PROTO2/vmgr/vmgrlisttape -P "+tapePool)
  if errTape.read() != "":
    print "Error:"
    print errTape.read()
    sys.exit(2)
  else:
    lineTape=outTape.read().splitlines()
    listOfTape=[]
    for singleLine in lineTape:
      listOfTape.append(singleLine.split(" ")[0])


f=open("/etc/castor/REPACKUNDOCONFIG","r")
configFileInfo=f.read()
f.close
differentElem=configFileInfo.splitlines()
repackUser=(differentElem[0].split(" "))[1]
repackPass=(differentElem[1].split(" "))[1]
repackDb=(differentElem[2].split(" "))[1]


repackConnection= cx_Oracle.connect(repackUser+"/"+repackPass+"@"+repackDb)
repackCursor=repackConnection.cursor()


repackQry="select *  from repacksegment where vid in (select repacksubrequest.id from repacksubrequest,repackrequest where repacksubrequest.requestid=repackrequest.id and repackrequest.creationtime in (select max(creationtime) from (select * from repackrequest where id in (select requestid from repacksubrequest where vid=:1))) and repacksubrequest.requestid in (select requestid from repacksubrequest where vid=:2))"

for tape in listOfTape:
  print
  print "*** TAPE involved: ",tape," ***"
  print
  repackCursor.execute(repackQry,(tape,tape))
  repackResult=repackCursor.fetchall()
  listFile=[]

  for (fileidRep,compressionRep,segsizeRep,filesecRep,copynoRep,blockidRep,fileseqRep,idRep,vidRep) in repackResult:
    listFile.append((fileidRep,copynoRep))
    
    repackQryForVid="select vid  from repacksubrequest where id=:1"
    repackCursor.execute(repackQryForVid,(vidRep,))
    vidRep=repackCursor.fetchall()[0][0]

    print "* FileID ",fileidRep
    print

    (ret,infoSeg)=wrapcns.cnsGetSegInfo(fileidRep)
    
    print "   Values from nameserver:"
    print "   ",infoSeg

    (ret,infoSeg)=wrapcns.cnsSetSegInfo(fileidRep,compressionRep,segsizeRep,filesecRep,copynoRep,blockidRep,fileseqRep,vidRep)

    print "   Values from repack db (? mark for the not provided information):"
    print "   ",infoSeg
        
    (ret,infoSeg)=wrapcns.cnsGetSegInfo(fileidRep)

    
    print "   Values from nameserver after the repack undo:"
    print "   ",infoSeg
    print
    
    
