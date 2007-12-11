import os
import sys


tapeVid=sys.argv[1]
cmd="nslisttape -V "+tapeVid
myOut=os.popen4(cmd)
buff=(myOut[1].read()).splitlines()

sizeMatchProblem=0
other=0
strToRm=""
numFiles=0

for elem in buff:
    fields=elem.split()
    strToRm= strToRm+" -M "+fields[8]
    numFiles=numFiles+1

if numFiles !=0:
    cmd="stager_rm -S \'*\'"+strToRm
    print "Executing stager_rm for files on tape "+tapeVid
    myOut=os.popen4(cmd)
    buff=myOut[1].read()
    cmd="stager_qry -S \'*\'"+strToRm
    myOut=os.popen4(cmd)
    buff=myOut[1].read()
    diskcopy=buff.count("No such file or directory")
    print "Queried the stager to look for diskcopies left"
    if numFiles ==  diskcopy:
        print "no diskcopy in the stager"
    else:
        print  str(numFiles-diskcopy)+" left on the stager"
        print buff
else:
    print "tape empty"
