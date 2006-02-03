

import Tkinter
import threading
import os


# function called if one of the request takes too much time

def timeOut():
    print "TIME OUT !!!"
    print "Castor is not working :("
    listProc=os.popen("ps").read().split("\n")

# I retrieve and  kill the process that created problems
 
    for item in listProc:
        if item.find("stager_qry") != -1:
            os.system("kill -9 "+item[0:5])
            print "stager_qry was blocked"
        if item.find("rfcp") != -1:
            os.system("kill -9 "+item[0:5])
            print "rfcp was blocked"
        if item.find("stager_putdone") != -1:
            os.system("kill -9 "+item[0:5])
            print "stager_putdone was blocked"
        if item.find("nsls") != -1:
            os.system("kill -9 "+item[0:5])
            print "nsls was blocked"
	    
    os._exit(0)  

def runSafe(cmdS):

# I run the command on the shell checking that the request doesn't take more then the time out

    t=threading.Timer(10.0,timeOut)
    t.start()
    myOutput.insert(Tkinter.END,os.popen4(cmdS)[1].read())
    t.cancel()

def prepareStringStager():

# I use information of name file and user tag to built the right string for the stager command
    
    castorDir="/castor/cern.ch/user/"+userId[0]+"/"+userId+"/"
    nameFile1=nameFile1E.get()
    nameTag=nameTagE.get()
    cmdQuery=""
    if nameFile1:
	if nameFile1.find("castor:")!=-1:
              cmdQuery=cmdQuery+"-M "+castorDir+nameFile1[(nameFile1.find("castor:"))+7:]+" "
	else:
	      cmdQuery=cmdQuery+"-M "+nameFile1
    if nameTag:
        cmdQuery=cmdQuery+"-U "+nameTag+" "
    cmdQuery= cmdQuery+nameOptionE.get()+" "
    return cmdQuery

def startQuery():

# function executed with the stager_qry button

    myCmd=prepareStringStager()
    myCmd="stager_qry "+myCmd
    runSafe(myCmd)
    return 0
    
def prepareToPut():

# function executed with the stager_put  button
    
    myCmd=prepareStringStager()
    myCmd="stager_put "+myCmd
    runSafe(myCmd)
    return 0

def prepareToGet():

# function executed with the stager_get button
    
    myCmd=prepareStringStager()
    myCmd="stager_get "+myCmd
    runSafe(myCmd)
    return 0
    
def toBeGarbageCollected():

# function executed with the stager_rm button

    myCmd=prepareStringStager()
    if myCmd.find("-U")!= -1:
        myCmd=myCmd[0:(myCmd.find("-U")-1)]
    myCmd="stager_rm "+myCmd
    runSafe(myCmd)
    return 0

def putDone():

# function executed with the stager_putdone button
    
    myCmd=prepareStringStager()
    myCmd="stager_putdone "+myCmd
    runSafe(myCmd)
    return 0

def prepareString(myFile,myOpt):
    if myFile.find("castor:")!=-1:
        castorDir="/castor/cern.ch/user/"+userId[0]+"/"+userId+"/"
        myFile=castorDir+myFile[(myFile.find("castor:"))+7:]
    if myOpt:
	myFile=myOpt+" "+myFile
    return myFile
	    
def execRfcp():

# function executed with the rfcp  button
    
    nameFile1=prepareString(nameFile1E.get(),nameOptionE.get())
    nameFile2=prepareString(nameFile2E.get(),"")
    myCmd="rfcp "+nameFile1+" "+nameFile2
    runSafe(myCmd)
    return 0

def execRfrm():

# function executed with the Rfrm button    
    
    myCmd=prepareString(nameFile1E.get(),nameOptionE.get())
    if myCmd.find("-r") !=-1:
	myCmd="yes|rfrm "+myCmd
	   
    else:
	   myCmd="rfrm "+myCmd
    runSafe(myCmd)
    return 0

def execNsls():

# function executed with the Nsls button 
    
    myCmd=prepareString(nameFile1E.get(),nameOptionE.get())
    myCmd="nsls "+myCmd
    runSafe(myCmd)
    
    
def myUsage():

# function executed with the help button 
    
    myOutput.insert(Tkinter.END,"SYNTAX:\nTo work with a file in your castor dir write castor:nameFile,")
    myOutput.insert(Tkinter.END,"\ne.g. castor:myFile.txt or castor:tmp/MyNewFile.txt.\n")
    myOutput.insert(Tkinter.END,"\nSTAGER_QRY:\nInsert the name of the file in File 1 or the user tag in Tag.\n")
    myOutput.insert(Tkinter.END,"You can add more option with the following syntax.\n")
    nout=os.popen4("stager_qry -h")[1].read()
    nout=nout[nout.find("[-M"):]  
    myOutput.insert(Tkinter.END,nout)

    myOutput.insert(Tkinter.END,"\nSTAGER_PUT:\nInsert the name of the file in File 1  and, if you want, the user tag in Tag.\n")
    myOutput.insert(Tkinter.END,"You can add more option with the following syntax.\n")
    nout=os.popen4("stager_put -h")[1].read()
    nout=nout[nout.find("[-M"):]  
    myOutput.insert(Tkinter.END,nout)

    myOutput.insert(Tkinter.END,"\nSTAGER_PUTDONE:\nInsert the name of the file in File 1.\n")
    myOutput.insert(Tkinter.END,"You can add more option with the following syntax.\n")
    nout=os.popen4("stager_putdone -h")[1].read()
    nout=nout[nout.find("[-M"):]  
    myOutput.insert(Tkinter.END,nout)


    myOutput.insert(Tkinter.END,"\nSTAGER_GET:\nInsert the name of the file in File 1 or the user tag in Tag.\n")
    myOutput.insert(Tkinter.END,"You can add more option with the following syntax.\n")
    nout=os.popen4("stager_get -h")[1].read()
    nout=nout[nout.find("[-M"):]  
    myOutput.insert(Tkinter.END,nout)

    myOutput.insert(Tkinter.END,"\nSTAGER_RM:\nInsert the name of the file in File 1.\n")
    myOutput.insert(Tkinter.END,"You can add more option with the following syntax.\n")
    nout=os.popen4("stager_rm -h")[1].read()
    nout=nout[nout.find("-M")+6:]  
    myOutput.insert(Tkinter.END,nout)

    myOutput.insert(Tkinter.END,"\nRFCP:\n")
    myOutput.insert(Tkinter.END,"To copy the File 1 into File 2.\n")
    myOutput.insert(Tkinter.END,"As option you can use [-s maxsize][-v2]. \n") 

    myOutput.insert(Tkinter.END,"\nRFRM:\n")
    myOutput.insert(Tkinter.END,"To remove File 1. \n")
    myOutput.insert(Tkinter.END,"As option you can use [-r]. \n")
    
    myOutput.insert(Tkinter.END,"\nNSLS:\n")
    myOutput.insert(Tkinter.END,"To look your castor dir or in its folders using the path in File 1.\n")
    myOutput.insert(Tkinter.END,"Possible options are:\n")
    nout=os.popen4("nsls -h")[1].read()
    nout=nout[nout.find("[-"):nout.find("path")-1]  
    myOutput.insert(Tkinter.END,nout)
  
    
def logUser():
    
#function to find the right directory on castor 
    myUid= os.geteuid()
   
    strUid=0
   
    fin=open("/etc/passwd",'r')
    for line in fin:
        elem=line.split(":")
        if elem[2] == str(myUid):
            strUid=elem[0]
    fin.close()
    return strUid

def cleanBoard():

# to clean the window with the output text

      myOutput.delete(1.0,Tkinter.END)
      
      
def runTestSuite():

	os.system("python ./test1.py "+"/castor/cern.ch/user/"+userId[0]+"/"+userId+"/")
	myOutput.insert(Tkinter.END,os.popen4("python ./myTestSuite.py "+userId)[1].read())


############ MAIN #####################

# main window and 3 frames


myWin=Tkinter.Tk()
myWin.title("Stager Test")
myWin.geometry("%dx%d%+d%+d" % (830,675 , 0, 0))
myWin.maxsize(width=830 ,height=675)
myWin.minsize(width=830 ,height=675)

top=Tkinter.Frame(padx=128,pady=30,width=615,bg="light blue")
top.grid(row=0)
medium1=Tkinter.Frame(padx=171,width=615,bg="light blue",pady=20)
medium1.grid(row=1)
medium2=Tkinter.Frame(padx=342,pady=20,width=615,bg="light blue")
medium2.grid(row=2)
bottom=Tkinter.Frame(padx=60,pady=20,width=615, bg="light blue")
bottom.grid(row=3)
bottom2=Tkinter.Frame(padx=320,pady=25,width=615, bg="light blue")
bottom2.grid(row=4)
# Labels for the Entries

#Tkinter.Label(top,text="User name",bg="light blue",fg="dark blue").grid(row=0, column=0)

Tkinter.Label(top,text="File 1",bg="light blue", fg="dark blue").grid(row=0,column=0)
Tkinter.Label(top,text="File 2",bg="light blue",fg="dark blue").grid(row=0, column=1)
Tkinter.Label(top,text="Tag",bg="light blue",fg="dark blue").grid(row=0, column=2)
Tkinter.Label(top,text="Options",bg="light blue",fg="dark blue").grid(row=0, column=3)


# Entry for input information

#userE=Tkinter.Entry(top,text="user name",bg="white")
#userE.grid(row=1, column=0)
nameFile1E=Tkinter.Entry(top,text="File 1",bg="white")
nameFile1E.grid(row=1,column=0)
nameFile2E=Tkinter.Entry(top,text="File 2",bg="white")
nameFile2E.grid(row=1, column=1)
nameTagE=Tkinter.Entry(top,text="Tag",bg="white")
nameTagE.grid(row=1, column=2)
nameOptionE=Tkinter.Entry(top,text="Option",bg="white")
nameOptionE.grid(row=1, column=3)

# Window for the Output

myScroll=Tkinter.Scrollbar(bottom,bg="Peach puff",activebackground="moccasin",troughcolor="peach puff")
myOutput=Tkinter.Text(bottom,width=100,bg="white",yscrollcommand=myScroll.set)
myOutput.grid(row=0,column=0)
myScroll.grid(row=0,column=1,columnspan=1,sticky="ns")
myScroll.config(command=myOutput.yview)

# Different buttons to call the different functions

Tkinter.Button(medium1,text="Stager_qry",bg="peach puff",fg="dark blue",activebackground="moccasin",command=startQuery).grid(row=0, column=0)
Tkinter.Button(medium1,text="Stager_put",bg="peach puff",fg="dark blue",activebackground="moccasin",command=prepareToPut).grid(row=0,column=1)
Tkinter.Button(medium1,text="Stager_get",bg="peach puff",fg="dark blue",activebackground="moccasin",command=prepareToGet).grid(row=0,column=2)
Tkinter.Button(medium1,text="Stager_rm",bg="peach puff",fg="dark blue",activebackground="moccasin",command=toBeGarbageCollected).grid(row=0,column=3)
Tkinter.Button(medium1,text="Stager_putdone",bg="peach puff",fg="dark blue",activebackground="moccasin",command=putDone).grid(row=0, column=4)
Tkinter.Button(medium2,text="rfcp",bg="peach puff",fg="dark blue",activebackground="moccasin",command=execRfcp).grid(row=0,column=0)
Tkinter.Button(medium2,text="rfrm",bg="peach puff",fg="dark blue",activebackground="moccasin",command=execRfrm).grid(row=0,column=1)
Tkinter.Button(medium2,text="nsls",bg="peach puff",fg="dark blue",activebackground="moccasin",command=execNsls).grid(row=0,column=2)

Tkinter.Button(bottom2,text="Clean!",bg="peach puff",fg="dark blue",activebackground="moccasin",command=cleanBoard).grid(row=0,column=0)
Tkinter.Button(bottom2,text="help",bg="peach puff",fg="dark blue",activebackground="moccasin",command=myUsage).grid(row=0,column=1)

Tkinter.Button(bottom2,text="Run Test!",bg="peach puff",fg="dark blue",activebackground="moccasin",command=runTestSuite).grid(row=0,column=2)
# operation to find the user of the program.

userId=logUser()
execNsls()
cleanBoard()

if myOutput.get(1.0,Tkinter.END).find("No such file or directory") != -1:
    print "You are "+userId+" and you are not a Castor user. Go away!"
    os._exit(0)

myWin.mainloop()



