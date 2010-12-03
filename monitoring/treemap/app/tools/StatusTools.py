'''
Created on May 4, 2010
set of useful functions used by TreeBuilder and getRootObjectForTreemap() to report their progress into a file

The response speed is mainly dependend on the network speed. 
Therefore TreeBuilder and getRootObjectForTreemap() are the only places where it makes sense to report,
because the rest of the tree generation doesn't request any data from the db. 
Once the data is received it takes milliseconds to process it and respond.

If the tree structure is already in the db, usually TreeBuilder reports the relevant progress, 
because then it is the one who has to read the data from the db.
getRootObjectForTreemap() reads only one record from the db.
This is the case for the namespace metrics.

If the tree structure is not in the db, it has to be generated first before getRootObjectForTreemap() can return the root object.
In this case getRootObjectForTreemap() has to read the data from the db to generate the tree and reports the relevant progress.
TreeBuilder only accesses the ready tree structure (children methods simply redirect to the methods in the already generated tree)
This is the case for all Requests.
@author: kblaszcz
'''

from django.template.loader import render_to_string
from app import settings
import os

def deleteStatusFile(statusfilename):
    if statusfilename != '':
        try:
            statusfilefullpath = getStatusFileFullPath(statusfilename)
            os.remove(statusfilefullpath)
        except:
            pass
        
def getStatusFileFullPath(statusfilename):
    return settings.LOCAL_APACHE_DICT + settings.REL_STATUS_DICT + "/"+ statusfilename

def generateStatusFile(statusfilename, status):
    try:
        filecontent = render_to_string('status.html', {'status':"%f"%status})
        
        statusfilefullpath = getStatusFileFullPath(statusfilename)
        statusfile = open(statusfilefullpath, 'w')
        statusfile.truncate(0)
        statusfile.write(filecontent)
        statusfile.close()
    except Exception, e:
        if(statusfilename != ''): raise Warning("Status could not be written to" + getStatusFileFullPath(statusfilename))