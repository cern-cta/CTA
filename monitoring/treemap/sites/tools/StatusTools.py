from django.template.loader import render_to_string
from sites import settings
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
        filecontent = render_to_string('dirs/status.html', {'status':"%f"%status})
        
        statusfilefullpath = getStatusFileFullPath(statusfilename)
        statusfile = open(statusfilefullpath, 'w')
        statusfile.truncate(0)
        statusfile.write(filecontent)
        statusfile.close()
    except Exception, e:
        if(statusfilename != ''): raise Warning("Status could not be written to" + getStatusFileFullPath(statusfilename))