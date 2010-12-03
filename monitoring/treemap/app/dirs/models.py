# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#     * Rearrange models' order
#     * Make sure each model has one field with primary_key=True
# Feel free to rename the models, but don't rename db_table values or field names.
#
# Also note: You'll have to insert the output of 'django-admin.py sqlcustom [appname]'
# into your database.
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ObjectDoesNotExist
from django.db import connection, transaction, models
from django.db.models.query import QuerySet
from app import settings
from app.dirs.presets.options.DateOption import DateOption
from app.dirs.ModelInterface import ModelInterface
from app.errors.NoDataAvailableError import NoDataAvailableError
from app.tools.Inspections import *
from app.tools.StatusTools import generateStatusFile
from app.treemap.BasicTree import BasicTree
from app.treemap.defaultproperties.TreeMapProperties import *
from app.treemap.drawing.TreeDesigner import SquaredTreemapDesigner
from app.treemap.drawing.TreemapDrawers import SquaredTreemapDrawer
from app.treemap.objecttree.ObjectTree import ObjectTree
from app.treemap.objecttree.TreeBuilder import TreeBuilder
from app.treemap.objecttree.TreeRules import LevelRules
from app.treemap.objecttree.Wrapper import Wrapper
from app.treemap.objecttree.columntransformation import *
from app.treemap.viewtree.TreeCalculators import SquaredTreemapCalculator
import copy
import datetime
import profile
#!!!app.dirs.ModelSpecificFunctions.RequestsFunctions is imported at the end of the file!!!
#if it is in the beginning of the file you will get a KeyError in RequestsFunctions, globals()[modelname]

def lookForMissingNonInterfaceImplementations(modelname):
    #for now some nonsense here
    try:
        print globals()[modelname].__dict__["getChildren"]
        print "getChildren!"
    except KeyError:
        pass
        

class Dirs(models.Model, ModelInterface):
    fileid = models.DecimalField(unique=True, max_digits=127, decimal_places=0, primary_key=True)
    parent = models.ForeignKey('self', blank=True, null=True, related_name='dirs_set', db_column='parent')#models.DecimalField(max_digits=127, decimal_places=0, )
    name = models.CharField(max_length=255, blank=True)
    depth = models.IntegerField(null=True, blank=True)
    fullname = models.CharField(max_length=2048, blank=True)
    totalsize = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
    sizeontape = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
    dataontape = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
    nbfiles = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
    nbtapes = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
    nbfilesontape = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
    nbfilecopiesontape = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
    nbsubdirs = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
    oldestfilelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
    avgfilelastmod = models.FloatField(null=True, blank=True)
    sigfilelastmod = models.FloatField(null=True, blank=True)
    newestfilelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
    oldestfileontapelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
    avgfileontapelastmod = models.FloatField(null=True, blank=True)
    sigfileontapelastmod = models.FloatField(null=True, blank=True)
    newestfileontapelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
    timetomigrate = models.FloatField(null=True, blank=True)
    timetorecall = models.FloatField(null=True, blank=True)
    timelostintapemarks = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
    opttimetorecall = models.FloatField(null=True, blank=True)
    
    def __init__(self, *args, **kwargs):
        models.Model.__init__(self, *args, **kwargs)
        
    children = QuerySet(model = 'self')
    children_cached = False
    
    files = QuerySet(model = 'CnsFileMetadata')
    files_cached = False
    
    parent_cached = False
    theparent = None
    
    class Meta:
        db_table = u'dirs'
        ordering = ["-totalsize"]
        
    def __unicode__(self):
        return unicode(self.__str__())
    
    def __str__(self):
        return self.fullname
    
    def __hash__(self):
        return models.Model.__hash__(self)
    
    def getDirs(self):
        if self.children_cached:
            return self.children
        else:
            deeper = self.depth + 1
            self.children = Dirs.objects.filter(parent=self.fileid, depth = deeper)
            self.children_cached = True
        return self.children
    
    #EXISTS is faster but not supported by Django 1.1
    #Therefore executing raw command
    #That one would be a lot slower but not Oracle specific:
    #    the_children = Dirs.objects.filter(parent=self.fileid).count() #.count(parent=self.fileid)[:1]
    
    #DUAL is Oracle's fake Table. It is needed to make the EXISTS Statement work, because EXISTS works only in WHERE
            
    def countDirs(self):
        if self.children_cached:
            return len(self.children)
        else:
            cursor = connection.cursor()
            cursor.execute('SELECT count(*) FROM CNS_FILE_METADATA f WHERE f.parent_fileid = %s AND EXISTS (select * from DIRS d where f.parent_fileid = d.parent)', [self.fileid])
            cnt = cursor.fetchone()
            return cnt[0]
        
    def countFiles(self):
        if self.files_cached:
            return len(self.files)
        else:
            try:
                cursor = connection.cursor()
                cursor.execute('SELECT count(*) FROM CNS_FILE_METADATA f WHERE f.parent_fileid = %s AND NOT EXISTS (select * from DIRS d where f.parent_fileid = d.parent)', [self.fileid])
                cnt = cursor.fetchone()
            except:
                print "exception!"
            return cnt[0]
        
        
#            cnt = CnsFileMetadata.objects.filter(parent_fileid=id).extra(where=['bitand(filemode, 16384) = 0']).count()
#            return cnt
        
    def countFilesAndDirs(self):
        return self.countFiles() + self.countDirs()
    
    #expensive operation if you call it too often (example: 243 calls makes 6.5 seconds)
    def getDirParent(self):
        try:
            if not self. parent_cached:
                self.theparent = self.parent
            return self.theparent
        except Dirs.DoesNotExist:
            return self
        
    def refreshChildren(self):
        self.children_cached = False
        self.children = self.getDirs()
       
    def getFiles(self):
        if self.files_cached:
            return self.files
        else:
            self.files = self.getFilesOf(self.fileid)#self.cnsfilemetadata_set.all()
            self.files_cached = True
        return self.files
    
    def getFilesAndDirectories(self):

        d = self.getDirs()
        if not d: 
            d = []
        else:
            d = list(d)
#            
        f = self.getFiles()
        if not f: 
            f = []
        else:
            f = list(f)
        
        return d+f

    
    def getFilesOf(self, id):
        prnt = Dirs.objects.get(pk=id)
        if prnt is None:
            return None
        children = CnsFileMetadata.objects.filter(parent_fileid=id).extra(where=['bitand(filemode, 16384) = 0'])   #exclude(filemode=16384)
        return children
            
    def refreshFiles(self):
        self.files_cached = False
        self.files = self.getFiles()
    
    def getUserFriendlyName(self):
        return "Directory"
    
    #defines how to find an object, no matter in what process or physical address
    def getIdReplacement(self):
        return ''.join([bla for bla in [self.__class__.__name__, "_", self.fullname.__str__()]])
    
    def getNaviName(self):
        return self.name  

    #an empty urlrest must be accepted and it should define the very root of the tree
    #in case there is no default root you have to pick a random valid object
    def findObjectByIdReplacementSuffix(self, urlrest, statusfilename = None):
        dirname = None
        if urlrest.rfind('/') == (len(urlrest)-1): 
            dirname = urlrest[:len(urlrest)-1]
        else:
            dirname = urlrest
        if dirname == '': dirname = '/castor'#if empty choose a root Directory that makes sense
        found = getDirByName(dirname)#Dirs.objects.get(fullname=dirname)
        return found
    
    def childrenMethodsPairs(self):
        return [
                {'childrenmethod': 'getDirs', 'childrencounter': 'countDirs'},
                {'childrenmethod': 'getFiles', 'childrencounter': 'countFiles'},
                {'childrenmethod': 'getFilesAndDirectories', 'childrencounter': 'countFilesAndDirs'},
               ]
    
    def parentMethods(self):
        return['getDirParent']
        
    def metricAttributes(self):
        return []
        
class CnsFileMetadata(models.Model, ModelInterface):
    fileid = models.DecimalField(max_digits=127, decimal_places=0, primary_key=True)
    parent_fileid = models.ForeignKey('Dirs', blank=True, null=True, db_column='parent_fileid')#models.DecimalField(unique=True, null=True, max_digits=127, decimal_places=0, blank=True)
    name = models.CharField(unique=True, max_length=255, blank=True)
    filemode = models.IntegerField(null=True, blank=True)
    nlink = models.IntegerField(null=True, blank=True)
    owner_uid = models.IntegerField(null=True, blank=True)
    gid = models.IntegerField(null=True, blank=True)
    filesize = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
    atime = models.IntegerField(null=True, blank=True)
    mtime = models.IntegerField(null=True, blank=True)
    ctime = models.IntegerField(null=True, blank=True)
    status = models.CharField(max_length=1, blank=True)
    fileclass = models.IntegerField(null=True, blank=True)
    guid = models.CharField(unique=True, max_length=36, blank=True)
    csumtype = models.CharField(max_length=2, blank=True)
    csumvalue = models.CharField(max_length=32, blank=True)
    acl = models.CharField(max_length=3900, blank=True)
    class Meta:
        db_table = u'cns_file_metadata'
        ordering = ["-filesize"]
        
    def __unicode__(self):
        return unicode(self.__str__())
    
    def __str__(self):
        return self.name
    
    def __hash__(self):
        return models.Model.__hash__(self)
    
    def countChildren(self):
        print "countChildren for files"
        return 0
    
    def getChildren(self):
        return []
    
    def getDirParent(self):
        try:
            return self.parent_fileid
        except ObjectDoesNotExist:
            return self
        
    def getUserFriendlyName(self):
        return "File"
    
    #defines how to find an object, no matter in what process or physical address
    def getIdReplacement(self):
        return ''.join([bla for bla in [self.__class__.__name__, "_", str(self.parent_fileid.fullname), "/" ,self.name.__str__()]])
    
    #an empty urlrest must be accepted and it should define the very root of the tree
    #in case there is no default root you have to pick a random valid object
    def findObjectByIdReplacementSuffix(self, urlrest, statusfilename = None):
        fullpath = None
        if urlrest.rfind('/') == (len(urlrest)-1): 
            fullpath = urlrest[:len(urlrest)-1]
        else:
            fullpath = urlrest
        
        pos = fullpath.rfind('/')
        if pos == -1:
            fname = fullpath   
        else:  
            fname = fullpath[pos+1:]
            dirname = fullpath[:pos]
            directoryobject = getDirByName(dirname)
            
        if(fname == ''):#if empty choose a random valid object
            found = CnsFileMetadata.objects.filter().extra(where=['bitand(filemode, 16384) = 0'])[0]
        else:
            found  = CnsFileMetadata.objects.filter(parent_fileid = directoryobject.fileid, name=fname).extra(where=['bitand(filemode, 16384) = 0'])[0]
        if found is None: raise NoDataAvailableError ("no such object")    
        return found
    
    def getNaviName(self):
        nvtext = str(self.name)
        pos = nvtext.rfind('/')
        if pos == -1: pos = 0
        nvtext = nvtext[pos:]  
        
        if len(nvtext)>0:
            while (nvtext[(len(nvtext)-1):(len(nvtext))] == '/'): 
                nvtext = nvtext[:(len(nvtext)-1)]
                if len(nvtext) == 0: break
            while (nvtext[0] == '/') and len(nvtext)>0: 
                nvtext = nvtext[1:]   
                if len(nvtext) == 0: break
                
        return nvtext 
    
    def childrenMethodsPairs(self):
        return [
                {'childrenmethod': 'getChildren', 'childrencounter': 'countChildren'},
               ]
    
    def parentMethods(self):
        return['getDirParent']
        
    def metricAttributes(self):
        return []

class Requestsatlas(models.Model, ModelInterface):
    subreqid = models.CharField(unique=True, max_length=36)
    timestamp = models.DateField(blank=True)
    reqid = models.CharField(max_length=36, primary_key=True)
    nsfileid = models.DecimalField(max_digits=127, decimal_places=0)
    type = models.CharField(max_length=255, blank=True)
    svcclass = models.CharField(max_length=255, blank=True)
    username = models.CharField(max_length=255, blank=True)
    state = models.CharField(max_length=255, blank=True)
    filename = models.CharField(max_length=2048, blank=True)
    filesize = models.DecimalField(null=True, default = 0, max_digits=127, decimal_places=0, blank=True)
    
    #fake (not in db)
#    requestscount = models.IntegerField(default = 1.0)
    
    class Meta:
        db_table = u'requestsatlas'
    
    def __init__(self, *args, **kwargs):
        models.Model.__init__(self, *args, **kwargs)
        self.requestscount = None
        
    def __unicode__(self):
        return unicode(self.__str__())
    
    def __str__(self):
        txt = ''
        if(self.filename == ''): 
            txt = "/"
        else:
            txt = self.filename
        return txt
    
    #This is needed because the networkx libraray used by BasicTree uses the hash function to tell if objects are different
    def __hash__(self):
        return hash(self._get_pk_val().__str__() + self.filename)
        
    def getUserFriendlyName(self):
        return "Requests Atlas"
    
    #defines how to find an object, no matter in what process or physical address
    def getIdReplacement(self):
        try:
            self.filename
        except:
            raise Exception("No attribute filename in current object")
        return ''.join([bla for bla in [self.__class__.__name__, "_", self.filename]])
    
    #finds the closest Object in the tree if the requested one doesn't exist
    def findObjectByIdReplacementSuffix(self, urlrest, statusfilename):
        return findRequestObjectByIdReplacementSuffix(urlrest, statusfilename, 'Requestsatlas')
    
    def getChildren(self):
        tree = traverseToRequestInTree(self.filename, self.__class__.__name__)
        return tree.getChildren()
    
    def countChildren(self):
        tree = traverseToRequestInTree(self.filename, self.__class__.__name__)
        return tree.countChildren()
    
    def getParent(self):
        tree = traverseToRequestInTree(self.filename, self.__class__.__name__)
        try:
            tree.traverseBack()
        except:
            pass
        return tree.getCurrentObject()
    
    def getNaviName(self):
        nvtext = str(self.filename)
        pos = nvtext.rfind('/')
        if pos == -1: pos = 0
        nvtext = nvtext[pos:]  
        
        if len(nvtext)>0:
            while (nvtext[(len(nvtext)-1):(len(nvtext))] == '/'): 
                nvtext = nvtext[:(len(nvtext)-1)]
                if len(nvtext) == 0: break
            while (nvtext[0] == '/') and len(nvtext)>0: 
                nvtext = nvtext[1:]   
                if len(nvtext) == 0: break
                
        return nvtext 
    
    def childrenMethodsPairs(self):
        return [
                {'childrenmethod': 'getChildren', 'childrencounter': 'countChildren'},
               ]
    
    def parentMethods(self):
        return['getParent']
        
    def metricAttributes(self):
        return ['requestscount']

Requestsatlas.generatedtree = None
#current parameters of the generated tree
Requestsatlas.treeprops = {'start': None, 'stop': None}
#wish parameters for the generated tree
Requestsatlas.start = datetime.datetime.now()-datetime.timedelta(minutes=120) #time relative to now
Requestsatlas.stop = datetime.datetime.now() #time relative to now

class Requestscms(models.Model, ModelInterface):
    subreqid = models.CharField(unique=True, max_length=36)
    timestamp = models.DateField(blank=True)
    reqid = models.CharField(max_length=36, primary_key=True)
    nsfileid = models.DecimalField(max_digits=127, decimal_places=0)
    type = models.CharField(max_length=255, blank=True)
    svcclass = models.CharField(max_length=255, blank=True)
    username = models.CharField(max_length=255, blank=True)
    state = models.CharField(max_length=255, blank=True)
    filename = models.CharField(max_length=2048, blank=True)
    filesize = models.DecimalField(null=True, default = 0, max_digits=127, decimal_places=0, blank=True)
    
    #fake (not in db)
#    requestscount = models.IntegerField(default = 1.0)
    
    class Meta:
        db_table = u'requestscms'
    
    def __init__(self, *args, **kwargs):
        models.Model.__init__(self, *args, **kwargs)
        self.requestscount = None
        
    def __unicode__(self):
        return unicode(self.__str__())
    
    def __str__(self):
        txt = ''
        if(self.filename == ''): 
            txt = "/"
        else:
            txt = self.filename
        return txt
    
    #This is needed because the networkx libraray used by BasicTree uses the hash function to tell if objects are different
    def __hash__(self):
        return hash(self._get_pk_val().__str__() + self.filename)
        
    def getUserFriendlyName(self):
        return "Requests CMS"
    
    #defines how to find an object, no matter in what process or physical address
    def getIdReplacement(self):
        try:
            self.filename
        except:
            raise Exception("No attribute filename in current object")
        return ''.join([bla for bla in [self.__class__.__name__, "_", self.filename]])
    
    #finds the closest Object in the tree if the requested one doesn't exist
    def findObjectByIdReplacementSuffix(self, urlrest, statusfilename):
        return findRequestObjectByIdReplacementSuffix(urlrest, statusfilename, 'Requestscms')
    
    def getChildren(self):
        tree = traverseToRequestInTree(self.filename, self.__class__.__name__)
        return tree.getChildren()
    
    def countChildren(self):
        tree = traverseToRequestInTree(self.filename, self.__class__.__name__)
        return tree.countChildren()
    
    def getParent(self):
        tree = traverseToRequestInTree(self.filename, self.__class__.__name__)
        try:
            tree.traverseBack()
        except:
            pass
        return tree.getCurrentObject()
    
    def getNaviName(self):
        nvtext = str(self.filename)
        pos = nvtext.rfind('/')
        if pos == -1: pos = 0
        nvtext = nvtext[pos:]  
        
        if len(nvtext)>0:
            while (nvtext[(len(nvtext)-1):(len(nvtext))] == '/'): 
                nvtext = nvtext[:(len(nvtext)-1)]
                if len(nvtext) == 0: break
            while (nvtext[0] == '/') and len(nvtext)>0: 
                nvtext = nvtext[1:]   
                if len(nvtext) == 0: break
                
        return nvtext 

    def childrenMethodsPairs(self):
        return [
                {'childrenmethod': 'getChildren', 'childrencounter': 'countChildren'},
               ]
    
    def parentMethods(self):
        return['getParent']
        
    def metricAttributes(self):
        return ['requestscount']

Requestscms.generatedtree = None

#current parameters of the generated tree
Requestscms.treeprops = {'start': None, 'stop': None}
#wish parameters of the generated tree
Requestscms.start = datetime.datetime.now()-datetime.timedelta(minutes=120) #time relative to now
Requestscms.stop = datetime.datetime.now() #time relative to now

class Requestsalice(models.Model, ModelInterface):
    subreqid = models.CharField(unique=True, max_length=36)
    timestamp = models.DateField(blank=True)
    reqid = models.CharField(max_length=36, primary_key=True)
    nsfileid = models.DecimalField(max_digits=127, decimal_places=0)
    type = models.CharField(max_length=255, blank=True)
    svcclass = models.CharField(max_length=255, blank=True)
    username = models.CharField(max_length=255, blank=True)
    state = models.CharField(max_length=255, blank=True)
    filename = models.CharField(max_length=2048, blank=True)
    filesize = models.DecimalField(null=True, default = 0, max_digits=127, decimal_places=0, blank=True)
    
    #fake (not in db)
#    requestscount = models.IntegerField(default = 1.0)
    
    class Meta:
        db_table = u'requestsalice'
    
    def __init__(self, *args, **kwargs):
        models.Model.__init__(self, *args, **kwargs)
        self.requestscount = None
        
    def __unicode__(self):
        return unicode(self.__str__())
    
    def __str__(self):
        txt = ''
        if(self.filename == ''): 
            txt = "/"
        else:
            txt = self.filename
        return txt
    
    #This is needed because the networkx libraray used by BasicTree uses the hash function to tell if objects are different
    def __hash__(self):
        return hash(self._get_pk_val().__str__() + self.filename)
        
    def getUserFriendlyName(self):
        return "Requests Alice"
    
    #defines how to find an object, no matter in what process or physical address
    def getIdReplacement(self):
        try:
            self.filename
        except:
            raise Exception("No attribute filename in current object")
        return ''.join([bla for bla in [self.__class__.__name__, "_", self.filename]])
    
    #finds the closest Object in the tree if the requested one doesn't exist
    def findObjectByIdReplacementSuffix(self, urlrest, statusfilename):
        return findRequestObjectByIdReplacementSuffix(urlrest, statusfilename, 'Requestsalice')
    
    def getChildren(self):
        tree = traverseToRequestInTree(self.filename, self.__class__.__name__)
        return tree.getChildren()
    
    def countChildren(self):
        tree = traverseToRequestInTree(self.filename, self.__class__.__name__)
        return tree.countChildren()
    
    def getParent(self):
        tree = traverseToRequestInTree(self.filename, self.__class__.__name__)
        try:
            tree.traverseBack()
        except:
            pass
        return tree.getCurrentObject()

    def getNaviName(self):
        nvtext = str(self.filename)
        pos = nvtext.rfind('/')
        if pos == -1: pos = 0
        nvtext = nvtext[pos:]  
        
        if len(nvtext)>0:
            while (nvtext[(len(nvtext)-1):(len(nvtext))] == '/'): 
                nvtext = nvtext[:(len(nvtext)-1)]
                if len(nvtext) == 0: break
            while (nvtext[0] == '/') and len(nvtext)>0: 
                nvtext = nvtext[1:]   
                if len(nvtext) == 0: break
                
        return nvtext 
    
    def childrenMethodsPairs(self):
        return [
                {'childrenmethod': 'getChildren', 'childrencounter': 'countChildren'},
               ]
    
    def parentMethods(self):
        return['getParent']

    def metricAttributes(self):
        return ['requestscount']

Requestsalice.generatedtree = None

#current parameters of the generated tree
Requestsalice.treeprops = {'start': None, 'stop': None}
#wish parameters of the generated tree
Requestsalice.start = datetime.datetime.now()-datetime.timedelta(minutes=120) #time relative to now
Requestsalice.stop = datetime.datetime.now() #time relative to now

class Requestslhcb(models.Model, ModelInterface):
    subreqid = models.CharField(unique=True, max_length=36)
    timestamp = models.DateField(blank=True)
    reqid = models.CharField(max_length=36, primary_key=True)
    nsfileid = models.DecimalField(max_digits=127, decimal_places=0)
    type = models.CharField(max_length=255, blank=True)
    svcclass = models.CharField(max_length=255, blank=True)
    username = models.CharField(max_length=255, blank=True)
    state = models.CharField(max_length=255, blank=True)
    filename = models.CharField(max_length=2048, blank=True)
    filesize = models.DecimalField(null=True, default = 0, max_digits=127, decimal_places=0, blank=True)
    
    #fake (not in db)
#    requestscount = models.IntegerField(default = 1.0)
    
    class Meta:
        db_table = u'requestslhcb'
    
    def __init__(self, *args, **kwargs):
        models.Model.__init__(self, *args, **kwargs)
        self.requestscount = None
        
    def __unicode__(self):
        return unicode(self.__str__())
    
    def __str__(self):
        txt = ''
        if(self.filename == ''): 
            txt = "/"
        else:
            txt = self.filename
        return txt
    
    #This is needed because the networkx libraray used by BasicTree uses the hash function to tell if objects are different
    def __hash__(self):
        return hash(self._get_pk_val().__str__() + self.filename)
        
    def getUserFriendlyName(self):
        return "Requests LHCb"
    
    #defines how to find an object, no matter in what process or physical address
    def getIdReplacement(self):
        try:
            self.filename
        except:
            raise Exception("No attribute filename in current object")
        return ''.join([bla for bla in [self.__class__.__name__, "_", self.filename]])
    
    #finds the closest Object in the tree if the requested one doesn't exist
    def findObjectByIdReplacementSuffix(self, urlrest, statusfilename):
        return findRequestObjectByIdReplacementSuffix(urlrest, statusfilename, 'Requestslhcb')
    
    def getChildren(self):
        tree = traverseToRequestInTree(self.filename, self.__class__.__name__)
        return tree.getChildren()
    
    def countChildren(self):
        tree = traverseToRequestInTree(self.filename, self.__class__.__name__)
        return tree.countChildren()
    
    def getParent(self):
        tree = traverseToRequestInTree(self.filename, self.__class__.__name__)
        try:
            tree.traverseBack()
        except:
            pass
        return tree.getCurrentObject()
    
    def getNaviName(self):
        nvtext = str(self.filename)
        pos = nvtext.rfind('/')
        if pos == -1: pos = 0
        nvtext = nvtext[pos:]  
        
        if len(nvtext)>0:
            while (nvtext[(len(nvtext)-1):(len(nvtext))] == '/'): 
                nvtext = nvtext[:(len(nvtext)-1)]
                if len(nvtext) == 0: break
            while (nvtext[0] == '/') and len(nvtext)>0: 
                nvtext = nvtext[1:]   
                if len(nvtext) == 0: break
                
        return nvtext 
    
    def childrenMethodsPairs(self):
        return [
                {'childrenmethod': 'getChildren', 'childrencounter': 'countChildren'},
               ]
    
    def parentMethods(self):
        return['getParent']

    def metricAttributes(self):
        return ['requestscount']

Requestslhcb.generatedtree = None

#current parameters of the generated tree
Requestslhcb.treeprops = {'start': None, 'stop': None}
#wish parameters of the generated tree
Requestslhcb.start = datetime.datetime.now()-datetime.timedelta(minutes=120) #time relative to now
Requestslhcb.stop = datetime.datetime.now() #time relative to now

class Requestspublic(models.Model, ModelInterface):
    subreqid = models.CharField(unique=True, max_length=36)
    timestamp = models.DateField(blank=True)
    reqid = models.CharField(max_length=36, primary_key=True)
    nsfileid = models.DecimalField(max_digits=127, decimal_places=0)
    type = models.CharField(max_length=255, blank=True)
    svcclass = models.CharField(max_length=255, blank=True)
    username = models.CharField(max_length=255, blank=True)
    state = models.CharField(max_length=255, blank=True)
    filename = models.CharField(max_length=2048, blank=True)
    filesize = models.DecimalField(null=True, default = 0, max_digits=127, decimal_places=0, blank=True)
    
    #fake (not in db)
#    requestscount = models.IntegerField(default = 1.0)
    
    class Meta:
        db_table = u'requestspublic'
    
    def __init__(self, *args, **kwargs):
        models.Model.__init__(self, *args, **kwargs)
        self.requestscount = None
        
    def __unicode__(self):
        return unicode(self.__str__())
    
    def __str__(self):
        txt = ''
        if(self.filename == ''): 
            txt = "/"
        else:
            txt = self.filename
        return txt
    
    #This is needed because the networkx libraray used by BasicTree uses the hash function to tell if objects are different
    def __hash__(self):
        return hash(self._get_pk_val().__str__() + self.filename)

        
    def getUserFriendlyName(self):
        return "Requests public"
    
    #defines how to find an object, no matter in what process or physical address
    def getIdReplacement(self):
        try:
            self.filename
        except:
            raise Exception("No attribute filename in current object")
        return ''.join([bla for bla in [self.__class__.__name__, "_", self.filename]])
    
    #finds the closest Object in the tree if the requested one doesn't exist
    def findObjectByIdReplacementSuffix(self, urlrest, statusfilename):
        return findRequestObjectByIdReplacementSuffix(urlrest, statusfilename, 'Requestspublic')
    
    def getChildren(self):
        tree = traverseToRequestInTree(self.filename, self.__class__.__name__)
        return tree.getChildren()
    
    def countChildren(self):
        tree = traverseToRequestInTree(self.filename, self.__class__.__name__)
        return tree.countChildren()
    
    def getParent(self):
        tree = traverseToRequestInTree(self.filename, self.__class__.__name__)
        try:
            tree.traverseBack()
        except:
            pass
        return tree.getCurrentObject()
    
    def getNaviName(self):
        nvtext = str(self.filename)
        pos = nvtext.rfind('/')
        if pos == -1: pos = 0
        nvtext = nvtext[pos:]  
        
        if len(nvtext)>0:
            while (nvtext[(len(nvtext)-1):(len(nvtext))] == '/'): 
                nvtext = nvtext[:(len(nvtext)-1)]
                if len(nvtext) == 0: break
            while (nvtext[0] == '/') and len(nvtext)>0: 
                nvtext = nvtext[1:]   
                if len(nvtext) == 0: break
                
        return nvtext 
    
    def childrenMethodsPairs(self):
        return [
                {'childrenmethod': 'getChildren', 'childrencounter': 'countChildren'},
               ]
    
    def parentMethods(self):
        return['getParent']

    def metricAttributes(self):
        return ['requestscount']

Requestspublic.generatedtree = None
#parameters of currently generated tree
Requestspublic.treeprops = {'start': None, 'stop': None}
#parameters defining how to generate the next tree
Requestspublic.start = datetime.datetime.now()-datetime.timedelta(minutes=120) #time relative to now
Requestspublic.stop = datetime.datetime.now() #time relative to now

#this import has to be here because of circular dependency with app.dirs.ModelSpecificFunctions.RequestsFunctions
#Warning!!! If you use eclipse the menu Source->Organize Imports will move these entries to the top of the file, which is wrong!
from app.dirs.ModelSpecificFunctions.RequestsFunctions import generateRequestsTree, traverseToRequestInTree, findRequestObjectByIdReplacementSuffix
from app.dirs.ModelSpecificFunctions.DirsFunctions import getDirByName
    
#class Ydirs(models.Model):
#    fileid = models.DecimalField(unique=True, max_digits=127, decimal_places=0)
#    parent = models.DecimalField(max_digits=127, decimal_places=0)
#    name = models.CharField(max_length=255, blank=True)
#    depth = models.IntegerField(null=True, blank=True)
#    fullname = models.CharField(max_length=2048, blank=True)
#    totalsize = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    sizeontape = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    dataontape = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    nbfiles = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    nbtapes = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    nbfilesontape = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    nbfilecopiesontape = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    oldestfilelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    avgfilelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    sigfilelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    newestfilelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    oldestfileontapelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    avgfileontapelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    sigfileontapelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    newestfileontapelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    timetomigrate = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    timetorecall = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    timelostintapemarks = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    opttimetorecall = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    class Meta:
#        db_table = u'ydirs'
#
#class Mdirs(models.Model):
#    fileid = models.DecimalField(unique=True, max_digits=127, decimal_places=0)
#    parent = models.DecimalField(max_digits=127, decimal_places=0)
#    name = models.CharField(max_length=255, blank=True)
#    depth = models.IntegerField(null=True, blank=True)
#    fullname = models.CharField(max_length=2048, blank=True)
#    totalsize = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    sizeontape = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    dataontape = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    nbfiles = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    nbtapes = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    nbfilesontape = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    nbfilecopiesontape = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    oldestfilelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    avgfilelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    sigfilelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    newestfilelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    oldestfileontapelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    avgfileontapelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    sigfileontapelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    newestfileontapelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    timetomigrate = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    timetorecall = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    timelostintapemarks = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    opttimetorecall = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    class Meta:
#        db_table = u'mdirs'
#
#class Wdirs(models.Model):
#    fileid = models.DecimalField(unique=True, max_digits=127, decimal_places=0)
#    parent = models.DecimalField(max_digits=127, decimal_places=0)
#    name = models.CharField(max_length=255, blank=True)
#    depth = models.IntegerField(null=True, blank=True)
#    fullname = models.CharField(max_length=2048, blank=True)
#    totalsize = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    sizeontape = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    dataontape = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    nbfiles = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    nbtapes = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    nbfilesontape = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    nbfilecopiesontape = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    oldestfilelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    avgfilelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    sigfilelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    newestfilelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    oldestfileontapelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    avgfileontapelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    sigfileontapelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    newestfileontapelastmod = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    timetomigrate = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    timetorecall = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    timelostintapemarks = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    opttimetorecall = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    class Meta:
#        db_table = u'wdirs'
#
#class Tapehelp(models.Model):
#    fileid = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    parent = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    vid = models.CharField(max_length=6, blank=True)
#    fsize = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    lastmodtime = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    class Meta:
#        db_table = u'tapehelp'
#
#class CnsClassMetadata(models.Model):
#    classid = models.IntegerField(primary_key=True)
#    name = models.CharField(unique=True, max_length=15, blank=True)
#    owner_uid = models.IntegerField(null=True, blank=True)
#    gid = models.IntegerField(null=True, blank=True)
#    min_filesize = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    max_filesize = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    flags = models.IntegerField(null=True, blank=True)
#    maxdrives = models.IntegerField(null=True, blank=True)
#    max_segsize = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    migr_time_interval = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    mintime_beforemigr = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    nbcopies = models.IntegerField(null=True, blank=True)
#    nbdirs_using_class = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    retenp_on_disk = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    class Meta:
#        db_table = u'cns_class_metadata'
#

#class CnsTpPool(models.Model):
#    classid = models.IntegerField(unique=True, null=True, blank=True)
#    tape_pool = models.CharField(unique=True, max_length=15, blank=True)
#    class Meta:
#        db_table = u'cns_tp_pool'
#
#class CnsUserMetadata(models.Model):
#    u_fileid = models.DecimalField(unique=True, max_digits=127, decimal_places=0)
#    comments = models.CharField(max_length=255, blank=True)
#    class Meta:
#        db_table = u'cns_user_metadata'
#
#class Experiments(models.Model):
#    name = models.CharField(max_length=8, blank=True)
#    gid = models.IntegerField(primary_key=True)
#    class Meta:
#        db_table = u'experiments'
#
#class StatusValues(models.Model):
#    status_string = models.CharField(max_length=100, blank=True)
#    status_number = models.DecimalField(unique=True, null=True, max_digits=127, decimal_places=0, blank=True)
#    stat = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    class Meta:
#        db_table = u'status_values'
#
#class TapePool(models.Model):
#    vid = models.CharField(unique=True, max_length=10)
#    model = models.CharField(max_length=8)
#    density = models.CharField(max_length=8)
#    pool = models.CharField(max_length=20)
#    library = models.CharFitoo bigeld(max_length=20)
#    capacity = models.DecimalField(max_digits=127, decimal_places=0)
#    status = models.IntegerField(null=True, blank=True)
#    used = models.DecimalField(null=True, max_digits=8, decimal_places=3, blank=True)
#    free_space = models.DecimalField(null=True, max_digits=8, decimal_places=3, blank=True)
#    effective_free_space = models.DecimalField(null=True, max_digits=8, decimal_places=3, blank=True)
#    nbfiles = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    noseg = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    class Meta:
#        db_table = u'tape_pool'
#
#class TapePools(models.Model):
#    vid = models.CharField(unique=True, max_length=10)
#    model = models.CharField(max_length=8)
#    density = models.CharField(max_length=8)
#    pool = models.CharField(max_length=20)
#    library = models.CharField(max_length=20)
#    capacity = models.DecimalField(max_digits=127, decimal_places=0)
#    status = models.IntegerField(null=True, blank=True)
#    used = models.DecimalField(null=True, max_digits=8, decimal_places=3, blank=True)
#    free_space = models.DecimalField(null=True, max_digits=8, decimal_places=3, blank=True)
#    effective_free_space = models.DecimalField(null=True, max_digits=8, decimal_places=3, blank=True)
#    class Meta:
#        db_table = u'tape_pools'
#
#class UserPrivilege(models.Model):
#    u_id = models.IntegerField(unique=True)
#    g_id = models.IntegerField(unique=True)
#    src_host = models.CharField(unique=True, max_length=63)
#    tgt_host = models.CharField(unique=True, max_length=63)
#    priv_cat = models.IntegerField(unique=True)
#    class Meta:
#        db_table = u'user_privilege'
#
#class VmgrTapeDenmap(models.Model):
#    md_model = models.CharField(max_length=6, primary_key=True)
#    md_media_letter = models.CharField(max_length=1, primary_key=True)
#    md_density = models.CharField(max_length=8, primary_key=True)
#    native_capacity = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    class Meta:
#        db_table = u'vmgr_tape_denmap'
#
#class VmgrTapeDgnmap(models.Model):
#    dgn = models.CharField(max_length=6, blank=True)
#    model = models.CharField(max_length=6, primary_key=True)
#    library = models.CharField(max_length=8, primary_key=True)
#    class Meta:
#        db_table = u'vmgr_tape_dgnmap'
#
#class VmgrTapeInfo(models.Model):
#    vid = models.CharField(max_length=6, primary_key=True)
#    vsn = models.CharField(max_length=6, blank=True)
#    library = models.CharField(max_length=8, blank=True)
#    density = models.CharField(max_length=8, blank=True)
#    lbltype = models.CharField(max_length=3, blank=True)
#    model = models.CharField(max_length=6, blank=True)
#    media_letter = models.CharField(max_length=1, blank=True)
#    manufacturer = models.CharField(max_length=12, blank=True)
#    sn = models.CharField(max_length=24, blank=True)
#    nbsides = models.IntegerField(null=True, blank=True)
#    etime = models.IntegerField(null=True, blank=True)
#    rcount = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    wcount = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    rhost = models.CharField(max_length=10, blank=True)
#    whost = models.CharField(max_length=10, blank=True)
#    rjid = models.IntegerField(null=True, blank=True)
#    wjid = models.IntegerField(null=True, blank=True)
#    rtime = models.IntegerField(null=True, blank=True)
#    wtime = models.IntegerField(null=True, blank=True)
#    class Meta:
#        db_table = u'vmgr_tape_info'
#
#class VmgrTapeLibrary(models.Model):
#    name = models.CharField(max_length=8, primary_key=True)
#    capacity = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    nb_free_slots = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    status = models.IntegerField(null=True, blank=True)
#    class Meta:
#        db_table = u'vmgr_tape_library'
#
#class VmgrTapeMedia(models.Model):
#    m_model = models.CharField(max_length=6, primary_key=True)
#    m_media_letter = models.CharField(max_length=1, blank=True)
#    media_cost = models.IntegerField(null=True, blank=True)
#    class Meta:
#        db_table = u'vmgr_tape_media'
#
#class VmgrTapePool(models.Model):
#    name = models.CharField(max_length=15, primary_key=True)
#    owner_uid = models.IntegerField(null=True, blank=True)
#    gid = models.IntegerField(null=True, blank=True)
#    tot_free_space = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    capacity = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    class Meta:
#        db_table = u'vmgr_tape_pool'
#
#class VmgrTapePoolBack(models.Model):
#    name = models.CharField(max_length=15, blank=True)
#    owner_uid = models.IntegerField(null=True, blank=True)
#    gid = models.IntegerField(null=True, blank=True)
#    tot_free_space = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    capacity = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    class Meta:
#        db_table = u'vmgr_tape_pool_back'
#
#class VmgrTapeSide(models.Model):
#    vid = models.CharField(unique=True, max_length=6)
#    poolname = models.CharField(max_length=15, blank=True)
#    status = models.IntegerField(null=True, blank=True)
#    estimated_free_space = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    nbfiles = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    side = models.IntegerField(primary_key=True)
#    class Meta:
#        db_table = u'vmgr_tape_side'
#
#class VmgrTapeTag(models.Model):
#    vid = models.CharField(max_length=6, primary_key=True)
#    text = models.CharField(max_length=255, blank=True)
#    class Meta:
#        db_table = u'vmgr_tape_tag'
#
#class CnsSymlinks(models.Model):
#    fileid = models.DecimalField(unique=True, max_digits=127, decimal_places=0)
#    linkname = models.CharField(max_length=1023, blank=True)
#    class Meta:
#        db_table = u'cns_symlinks'
#
#class SchemaVersion(models.Model):
#    major = models.IntegerField(null=True, blank=True)
#    minor = models.IntegerField(null=True, blank=True)
#    patch = models.IntegerField(null=True, blank=True)
#    class Meta:
#        db_table = u'schema_version'
#
#class Pbfilesncopiesbadck(models.Model):
#    fileid = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    class Meta:
#        db_table = u'pbfilesncopiesbadck'
#
#class CnsVersion(models.Model):
#    schemaversion = models.CharField(max_length=20, blank=True)
#    release = models.CharField(max_length=20, blank=True)
#    class Meta:
#        db_table = u'cns_version'
#
#class Upgradelog(models.Model):
#    username = models.CharField(max_length=64)
#    machine = models.CharField(max_length=64)
#    program = models.CharField(max_length=48)
#    startdate = models.DateTimeField(null=True, blank=True)
#    enddate = models.DateTimeField(null=True, blank=True)
#    failurecount = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    type = models.CharField(max_length=20, blank=True)
#    state = models.CharField(max_length=20, blank=True)
#    schemaversion = models.CharField(max_length=20)
#    release = models.CharField(max_length=20)
#    class Meta:
#        db_table = u'upgradelog'
#
##class PlanTable(models.Model):
##    statement_id = models.CharField(max_length=30, blank=True)
##    plan_id = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
##    timestamp = models.DateField(null=True, blank=True)
##    remarks = models.CharField(max_length=4000, blank=True)
##    operation = models.CharField(max_length=30, blank=True)
##    options = models.CharField(max_length=255, blank=True)
##    object_node = models.CharField(max_length=128, blank=True)
##    object_owner = models.CharField(max_length=30, blank=True)
##    object_name = models.CharField(max_length=30, blank=True)
##    object_alias = models.CharField(max_length=65, blank=True)
##    object_instance = models.IntegerField(null=True, blank=True)
##    object_type = models.CharField(max_length=30, blank=True)
##    optimizer = models.CharField(max_length=255, blank=True)
##    search_columns = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
##    id = models.IntegerField(null=True, blank=True)
##    parent_id = models.IntegerField(null=True, blank=True)
##    depth = models.IntegerField(null=True, blank=True)
##    position = models.IntegerField(null=True, blank=True)
##    cost = models.IntegerField(null=True, blank=True)
##    cardinality = models.IntegerField(null=True, blank=True)
##    bytes = models.IntegerField(null=True, blank=True)
##    other_tag = models.CharField(max_length=255, blank=True)
##    partition_start = models.CharField(max_length=255, blank=True)
##    partition_stop = models.CharField(max_length=255, blank=True)
##    partition_id = models.IntegerField(null=True, blank=True)
##    other = models.TextField(blank=True) # This field type is a guess.
##    distribution = models.CharField(max_length=30, blank=True)
##    cpu_cost = models.IntegerField(null=True, blank=True)
##    io_cost = models.IntegerField(null=True, blank=True)
##    temp_space = models.IntegerField(null=True, blank=True)
##    access_predicates = models.CharField(max_length=4000, blank=True)
##    filter_predicates = models.CharField(max_length=4000, blank=True)
##    projection = models.CharField(max_length=4000, blank=True)
##    time = models.IntegerField(null=True, blank=True)
##    qblock_name = models.CharField(max_length=30, blank=True)
##    other_xml = models.TextField(blank=True)
##    class Meta:
##        db_table = u'plan_table'
#
#class Tapedata(models.Model):
#    vid = models.CharField(max_length=6, blank=True)
#    nbfiles = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    datasize = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    avgfilesize = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    stddevfilesize = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    minfilelastmodtime = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    avgfilelastmodtime = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    stddevfilelastmodtime = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    maxfilelastmodtime = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    class Meta:
#        db_table = u'tapedata'
#
#class CnsSegMetadata(models.Model):
#    s_fileid = models.DecimalField(unique=True, max_digits=127, decimal_places=0)
#    copyno = models.IntegerField(primary_key=True)
#    fsec = models.IntegerField(primary_key=True)
#    segsize = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    s_status = models.CharField(max_length=1, blank=True)
#    vid = models.CharField(unique=True, max_length=6, blank=True)
#    fseq = models.IntegerField(unique=True, null=True, blank=True)
#    blockid = models.TextField(blank=True) # This field type is a guess.
#    compression = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    side = models.IntegerField(unique=True, null=True, blank=True)
#    checksum_name = models.CharField(max_length=16, blank=True)
#    checksum = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    class Meta:
#        db_table = u'cns_seg_metadata'
#
#class CnsFilesExistTmp(models.Model):
#    tmpfileid = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    class Meta:
#        db_table = u'cns_files_exist_tmp'
#
#class Worstmigtemptable(models.Model):
#    fullname = models.CharField(max_length=2048, blank=True)
#    timetomigrate = models.CharField(max_length=2048, blank=True)
#    timespentintapemarks = models.CharField(max_length=2048, blank=True)
#    tapemarkspart = models.CharField(max_length=2048, blank=True)
#    avgfilesize = models.CharField(max_length=2048, blank=True)
#    nbfilecopiesontape = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    totalsize = models.CharField(max_length=2048, blank=True)
#    class Meta:
#        db_table = u'worstmigtemptable'
#
#class Worstrecalltemptable(models.Model):
#    fullname = models.CharField(max_length=2048, blank=True)
#    esttimetorecall = models.CharField(max_length=2048, blank=True)
#    losttime = models.CharField(max_length=2048, blank=True)
#    lostpart = models.CharField(max_length=2048, blank=True)
#    nbtapes = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    avgfilesize = models.CharField(max_length=2048, blank=True)
#    nbfilecopiesontape = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    totalsize = models.CharField(max_length=2048, blank=True)
#    class Meta:
#        db_table = u'worstrecalltemptable'
#
#class Biggestontapetemptable(models.Model):
#    fullname = models.CharField(max_length=2048, blank=True)
#    sizeontape = models.CharField(max_length=2048, blank=True)
#    nbfilecopiesontape = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    nbtapes = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    timetomigrate = models.CharField(max_length=2048, blank=True)
#    esttimetorecall = models.CharField(max_length=2048, blank=True)
#    class Meta:
#        db_table = u'biggestontapetemptable'
#
#class Biggestondisktemptable(models.Model):
#    fullname = models.CharField(max_length=2048, blank=True)
#    sizeofdiskonlydata = models.CharField(max_length=2048, blank=True)
#    nbdiskonlyfiles = models.DecimalField(null=True, max_digits=127, decimal_places=0, blank=True)
#    class Meta:
#        db_table = u'biggestondisktemptable'
