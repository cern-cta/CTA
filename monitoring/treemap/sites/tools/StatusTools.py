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
        progressbarsizepx = 200.0
        progressbarsizepxstr = "%.0f"%progressbarsizepx
        progresspx = "%.0f"%(progressbarsizepx*float(status))
        if(progresspx=='0'): progresspx='1'
        
        filecontent = render_to_string('dirs/status.html', {'progresspx':progresspx, 'progressbarsizepx':progressbarsizepxstr, 'progressbardict':settings.PUBLIC_APACHE_URL + settings.REL_PBARIMG_DICT + '/'})
        
        statusfilefullpath = getStatusFileFullPath(statusfilename)
        statusfile = open(statusfilefullpath, 'w')
        statusfile.truncate(0)
        statusfile.write(filecontent)
        statusfile.close()
    except Exception, e:
        if(statusfilename != ''): raise Warning("Status could not be written to" + getStatusFileFullPath(statusfilename))