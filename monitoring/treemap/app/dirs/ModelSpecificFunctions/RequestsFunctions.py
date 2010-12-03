'''
Collection of model specific functions for Requestsatlas, Requestscms, Requestslhcb...]
These functions are supposed to help or extend with model specific functionality
@author: kblaszcz
'''



from app.dirs.DateOption import DateOption
from app.errors.NoDataAvailableError import NoDataAvailableError
from app.tools.StatusTools import generateStatusFile
from app.treemap.BasicTree import BasicTree
import copy
import datetime
import math
#!!!app.dirs.models is imported at the end of the file!!!

#an empty urlrest must be accepted and it should define the very root of the tree
#in case there is no default root you have to pick a random valid object
def findRequestObjectByIdReplacementSuffix(urlrest, statusfilename, modelname):
    path = None
    if urlrest.rfind('/') == (len(urlrest)-1): 
        path = urlrest[:len(urlrest)-1] #can be empty which will lead to root
    else:
        path = urlrest
        
    modelobject = globals()[modelname];
        
#        don't generate the tree again if it's already generated with the right parameters
    if (modelobject.treeprops['start'] != modelobject.start) or (modelobject.treeprops['stop'] != modelobject.stop):
        generateRequestsTree(modelobject.start , modelobject.stop, modelname, statusfilename)
        modelobject.treeprops['start'] = modelobject.start
        modelobject.treeprops['stop'] = modelobject.stop
        
    found = traverseToRequestInTree(path, modelname).getCurrentObject()
    return found

def generateRequestsTree(start, stop, reqmodel, statusfilename):
    def addRequestToTree(tree, requestdata):
        def findSplittingPos(name, start = 0):
            pos = name.find('/', start)
            
            if start > 0:
                #deal with multiple slashes like '//'
                posbefore = name.find('/', start-1)
                while posbefore == (pos -1):
                    #print pos, posbefore
                    start = start + 1
                    pos = name.find('/', start)
                    posbefore = name.find('/', start-1)

            return pos
            
        name = requestdata.filename
        pos = findSplittingPos(name)
            
        assert (pos >=0)
        oldpos = 0
        pos = 0
        
        if not tree.hasRoot():
            entry = copy.copy(requestdata)
            entry.filename = name[:pos]
            entry.requestscount = 1
            tree.setRoot(entry)
            tree.traverseToRoot()
            oldpos = pos + 1
            pos = findSplittingPos(name, oldpos)
        else:
            tree.traverseToRoot()
            oldpos = 0
            pos = findSplittingPos(name)
            
        thefilename = ''
        therequestcount = 1
            
        while (pos >=0):
            #todo: split and traverse
            filename = name[:pos]
            #entry = copy.copy(requestdata)
            
            thefilename = filename
            therequestcount = 1
            
            childintree = False
            for child in tree.getChildren():
                chnp = child.filename
                enp = thefilename
                chnplen = len(chnp)
                enplen = len(enp)
                a = 1
                b = 2
                if chnplen > 1 and enplen > 1:
                    a = chnp[chnplen-2]
                    b = enp[enplen-2]
                # a and b for speeding up
                if a == b:
                    if chnp == enp:
                        child.requestscount = child.requestscount + 1
                        childintree = True
                        tree.traverseInto(child)
                        break
            
            if not childintree:
                #for root: if this is the current parent
                parent = tree.getCurrentObject()
                if thefilename == parent.filename:
                    parent.requestscount = parent.requestscount + 1
                else:
                    entrytoadd = copy.copy(requestdata)
                    entrytoadd.filename = thefilename
                    entrytoadd.requestscount = therequestcount
                    tree.addChild(entrytoadd)
                    tree.traverseInto(entrytoadd)
            
            oldpos = pos + 1
            pos = findSplittingPos(name, pos + 1)
            
    model_class = globals()[reqmodel]
    model_class.generatedtree = BasicTree()        
    tree = model_class.generatedtree
    
    #preventing django from reading all data sets at once, read a part and update tree, read a part and update tree...
    secondsperreading = 1800 #this value is empirical and seems bring good speed
    nbsteps = int(math.ceil(((stop - start).seconds + (((stop - start).days)*24*60*60) )/float(secondsperreading)))
    print "Generating Tree of Requests"
    timemeasurement = datetime.datetime.now()
    
    for i in range(nbsteps):
        if i != (nbsteps -1):
            fromstring = DateOption.valueToString(DateOption("dummy","object",''), start + (i*datetime.timedelta(seconds = secondsperreading)) )
            tostring = DateOption.valueToString(DateOption("dummy","object",''), start + ((i+1)*datetime.timedelta(seconds = secondsperreading)) )
        elif i == (nbsteps -1):
            fromstring = DateOption.valueToString(DateOption("dummy","object",''), start + (i*datetime.timedelta(seconds = secondsperreading)))
            tostring = DateOption.valueToString(DateOption("dummy","object",''), stop)
            
        print "Reading Atlas Requests. Time: ", fromstring, " " ,tostring
        requestarray = list(model_class.objects.filter(filename__isnull=False).extra(where=["timestamp BETWEEN TO_DATE('"+ fromstring +"','DD.MM.YYYY_HH24:MI:SS') and TO_DATE('" + tostring + "','DD.MM.YYYY HH24:MI:SS')"]))
    
        for dataset in requestarray:
            addRequestToTree(tree, dataset)
            
        status = ((float(i+1)/float(nbsteps)))
        generateStatusFile(statusfilename, status)
    
    print "time: ", datetime.datetime.now() - timemeasurement
    if tree.getRoot() is None:
        raise NoDataAvailableError("Server didn't returned any Request records")
    print tree.getRoot().requestscount
    print ''
    
def traverseToRequestInTree(name, reqmodel):

    model_class = globals()[reqmodel]     
    tree = model_class.generatedtree

    assert ((name.find('/') >=0) or name == '')
    pos = 0
    
    if not tree.hasRoot():
        raise NoDataAvailableError("Data tree is empty!")
    else:
        tree.traverseToRoot()
        pos = name.find('/')
        if name == '': pos = 0
    
    lastiterationfollows = False #name is missing a slash at the very end, that is why the last iteration must be handled a bit different    
    while (pos >=0) or lastiterationfollows :
        filename = name[:pos]
        
        if tree.getCurrentObject().filename == name:
            return tree
        
        for child in tree.getChildren():
            if child.filename == name:
                tree.traverseInto(child)
                return tree
            if child.filename == filename:
                tree.traverseInto(child)
                break
            
        pos = name.find('/', pos + 1)
        
        if lastiterationfollows: break
        
        if pos == -1 and not lastiterationfollows:
            pos = len(name)
            lastiterationfollows = True
        
    #raise NoDataAvailableError(name + ": No such record in the tree")
    return tree #if directory doesn't exist any more, return on the last successful traversal

#this import has to be here because of circular dependency with app.dirs.models!
from app.dirs.models import *