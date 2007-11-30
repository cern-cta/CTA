import os
import sys
import time
import getopt


if len(sys.argv) != 2:
	print 'Please give the tape VID'
        print 'usage: python fileHuntingForTape.py VID'
	os._exit(0)

myShell=os.popen('ls -l /bin/sh').read()
instances=['c2cmsrh','c2atlasrh','c2alicerh','c2lhcbrh','c2publicrh']
env=""

if myShell.find("bash") != -1:
   env="unset STAGER_TRACE;export STAGE_HOST="
else:
   env="unsetenv STAGERTRACE;setenv STAGE_HOST "

instances=['c2cmsrh','c2atlasrh','c2alicerh','c2lhcbrh','c2publicrh']

tapeVid=sys.argv[1]
cmd="nslisttape -V "+tapeVid

myOut=os.popen4(cmd)
buff=(myOut[1].read()).splitlines()

sizeMatchProblem=0
other=0
strForQry=""
sizeZeroList=""
problematicFile=""

for elem in buff:
    fields=elem.split()
    cmd="nsls -l "+fields[8]
    myOut2=os.popen4(cmd)
    size=((myOut2[1].read()).split())[4]
    #print "file "+fields[8]
    #print "size segment = "+fields[6]
    #print "size file = "+size
    if fields[6] != size:
        sizeMatchProblem=sizeMatchProblem+1
	if size==0:
		sizeZeroList=sizeZeroList+fields[8]+"\n"	
	else:
	        problematicFile=problematicFile+fields[8]+"\n"
        strForQry=strForQry+" -M "+fields[8]
    else:
        other=other+1
	
if problematicFile!="" :
	print " \n* problematic files with file size bigger than zero:\n"
	print problematicFile
if sizeZeroList !="" :
	print " \n* problematic files with zero file size:\n"
	print sizeZeroList
print "*** Summary ***\n"
print str(other)+" can be recalled without file size error"
print str(sizeMatchProblem)+" suffered from file size error\n"

if sizeMatchProblem == 0:
    os._exit(0)

print "***  diskcopies\' research in the different instances ***\n"

#print strForQry        

strForQry="stager_qry -S \'*\' "+strForQry

for stager in instances:
    cmd=env+stager+";"+strForQry
    myOut=os.popen4(cmd)
    buff=(myOut[1].read()).splitlines()
    for file in buff:
        if file.find('not in stager') == -1:
            print "file "+file+" found on "+stager

print 



