# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#     * Rearrange models' order
#     * Make sure each model has one field with primary_key=True
# Feel free to rename the models, but don't rename db_table values or field names.
#
# Also note: You'll have to insert the output of 'django-admin.py sqlcustom [appname]'
# into your database.

from django.db import connection, transaction, models
from django.db.models.query import QuerySet
from sites.treemap.objecttree.ObjectTree import ObjectTree
from sites.treemap.objecttree.TreeBuilder import TreeBuilder
from sites.treemap.viewtree.TreeCalculators import SquaredTreemapCalculator
from sites.treemap.objecttree.TreeRules import LevelRules
from sites.treemap.objecttree.Wrapper import Wrapper
from sites.treemap.objecttree.columntransformation import *
import profile
import datetime
from sites.treemap.drawing.TreemapDrawers import SquaredTreemapDrawer
from sites.treemap.defaultproperties.SquaredViewProperties import *
from sites.treemap.drawing.TreeDesigner import SquaredTreemapDesigner

class Dirs(models.Model):
    fileid = models.DecimalField(unique=True, max_digits=0, decimal_places=-127, primary_key=True)
    parent = models.ForeignKey('self', blank=True, null=True, related_name='dirs_set', db_column='parent')#models.DecimalField(max_digits=0, decimal_places=-127, )
    name = models.CharField(max_length=255, blank=True)
    depth = models.IntegerField(null=True, blank=True)
    fullname = models.CharField(max_length=2048, blank=True)
    totalsize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    sizeontape = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    dataontape = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    nbfiles = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    nbtapes = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    nbfilesontape = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    nbfilecopiesontape = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    nbsubdirs = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    oldestfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    avgfilelastmod = models.FloatField(null=True, blank=True)
    sigfilelastmod = models.FloatField(null=True, blank=True)
    newestfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    oldestfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    avgfileontapelastmod = models.FloatField(null=True, blank=True)
    sigfileontapelastmod = models.FloatField(null=True, blank=True)
    newestfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    timetomigrate = models.FloatField(null=True, blank=True)
    timetorecall = models.FloatField(null=True, blank=True)
    timelostintapemarks = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
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
    
#   not used
    def getDirsOf(self, id):
        prnt = Dirs.objects.get(pk=id)
        deeper = prnt.depth + 1
        children = Dirs.objects.filter(parent=id, depth = deeper,)
        return children
    
    def getDirs(self):
        if self.children_cached:
            return self.children
        else:
            deeper = self.depth + 1
            self.children = Dirs.objects.filter(parent=self.fileid, depth = deeper)
            self.children_cached = True
            
#            if self.children: self.hasChildren =  True 
#            if not self.children: self.hasChildren = False
#        children = self.dirs_set.all(depth = deeper)
        return self.children
    
    #EXISTS is faster but not supported by Django 1.1
    #Therefore executing raw command
    #That one is a lot slower but not Oracle specific:
    #    the_children = Dirs.objects.filter(parent=self.fileid).count() #.count(parent=self.fileid)[:1]
    
    #DUAL is Oracle's fake Table. It is needed to make the EXISTS Statement work, because EXISTS works only in WHERE
    def hasChildren(self):
        if self.children_cached:
            if self.children: return True 
            if not self.children: return False
        else:
            cursor = connection.cursor()
            cursor.execute('SELECT 1 FROM DUAL WHERE EXISTS (SELECT 1 FROM DIRS WHERE ROWNUM <= 1 AND DIRS.parent = %s)', [self.fileid])
            the_children = cursor.fetchone()
#            if the_children : 
#                self.test()
            if the_children : return True 
            else: return False
            
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
        children = self.getDirs()
       
    def getFiles(self):
        if self.files_cached:
            return self.files
        else:
            self.files = self.getFilesOf(self.fileid)#self.cnsfilemetadata_set.all()
            self.files_cached = True
        return self.files
    
    def getFilesAndFolders(self):

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
    
    def hasFiles(self):
        if self.files_cached:
            if self.files: 
                return True 
            if not self.files: 
                return False
        else:
            cursor = connection.cursor()
            cursor.execute('SELECT 1 FROM DUAL WHERE EXISTS (SELECT 1 FROM CNS_FILE_METADATA WHERE ROWNUM <= 1 AND CNS_FILE_METADATA.parent_fileid = %s AND bitand(filemode, 16384) = 0)', [self.fileid])
            the_files = cursor.fetchone()
            
            if the_files : 
                return True 
            else: return False
            
    def refreshFiles(self):
        self.files_cached = False
        files = self.getFiles()
        
    def hasAnyChildren(self):
        return self.hasChildren() or self.hasFiles()
    
    def getUserFriendlyName(self):
        return "Directory"
    
    #defines how to find an object, no matter in what process or physical address
    def getIdReplacement(self):
        return ''.join([bla for bla in [self.__class__.__name__, "_", self.pk.__str__()]])

Dirs.nonmetrics = ['fileid', 'parent', 'depth', 'fullname']      
    
#mark children methods for Dirs
Dirs.getDirs.__dict__['methodtype'] = 'children'
Dirs.getFiles.__dict__['methodtype'] = 'children'
Dirs.getFilesAndFolders.__dict__['methodtype'] = 'children'

#mark count Methods
Dirs.countDirs.__dict__['methodtype'] = 'childrencount'
Dirs.countDirs.__dict__['countsfor'] = 'getDirs'

Dirs.countFiles.__dict__['methodtype'] = 'childrencount'
Dirs.countFiles.__dict__['countsfor'] = 'getFiles'

Dirs.countFilesAndDirs.__dict__['methodtype'] = 'childrencount'
Dirs.countFilesAndDirs.__dict__['countsfor'] = 'getFilesAndFolders'

#mark parent Methods
Dirs.getDirParent.__dict__['methodtype'] = 'parent'
Dirs.getDirParent.__dict__['returntype'] = ['Dir']
        
class CnsFileMetadata(models.Model):
    fileid = models.DecimalField(max_digits=0, decimal_places=-127, primary_key=True)
    parent_fileid = models.ForeignKey('Dirs', blank=True, null=True, db_column='parent_fileid')#models.DecimalField(unique=True, null=True, max_digits=0, decimal_places=-127, blank=True)
    name = models.CharField(unique=True, max_length=255, blank=True)
    filemode = models.IntegerField(null=True, blank=True)
    nlink = models.IntegerField(null=True, blank=True)
    owner_uid = models.IntegerField(null=True, blank=True)
    gid = models.IntegerField(null=True, blank=True)
    filesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
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
    
    def hasAnyChildren(self):
        return False
    
    def hasFiles(self):
        return False
    
    def countChildren(self):
        print "countChildren for files"
        return 0
    
    def getChildren(self):
        return []
    
    def getDirParent(self):
        try:
            return self.parent_fileid
        except Dirs.DoesNotExist:
            return self
        
    def getUserFriendlyName(self):
        return "File"
    
    #defines how to find an object, no matter in what process or physical address
    def getIdReplacement(self):
        return ''.join([bla for bla in [self.__class__.__name__, "_", self.pk.__str__()]])

#mark children Methods   
CnsFileMetadata.getChildren.__dict__['methodtype'] = 'children'

#mark count Methods
CnsFileMetadata.countChildren.__dict__['methodtype'] = 'childrencount'
CnsFileMetadata.countChildren.__dict__['countsfor'] = 'getChildren'

#mark parent Methods
CnsFileMetadata.getDirParent.__dict__['methodtype'] = 'parent'
CnsFileMetadata.getDirParent.__dict__['returntype'] = ['Dir']

class Requests(models.Model):
    subreqid = models.CharField(unique=True, max_length=36)
    timestamp = models.DateField()
    reqid = models.CharField(max_length=36)
    nsfileid = models.DecimalField(max_digits=0, decimal_places=-127)
    type = models.CharField(max_length=255, blank=True)
    svcclass = models.CharField(max_length=255, blank=True)
    username = models.CharField(max_length=255, blank=True)
    state = models.CharField(max_length=255, blank=True)
    filename = models.CharField(max_length=2048, blank=True)
    filesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'requests'
        
    def getUserFriendlyName(self):
        return "Amount of Requests"
    
    #defines how to find an object, no matter in what process or physical address
    def getIdReplacement(self):
        return ''.join([bla for bla in [self.__class__.__name__, "_", self.pk.__str__()]])
    
#class Ydirs(models.Model):
#    fileid = models.DecimalField(unique=True, max_digits=0, decimal_places=-127)
#    parent = models.DecimalField(max_digits=0, decimal_places=-127)
#    name = models.CharField(max_length=255, blank=True)
#    depth = models.IntegerField(null=True, blank=True)
#    fullname = models.CharField(max_length=2048, blank=True)
#    totalsize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    sizeontape = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    dataontape = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    nbfiles = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    nbtapes = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    nbfilesontape = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    nbfilecopiesontape = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    oldestfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    avgfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    sigfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    newestfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    oldestfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    avgfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    sigfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    newestfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    timetomigrate = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    timetorecall = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    timelostintapemarks = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    opttimetorecall = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    class Meta:
#        db_table = u'ydirs'
#
#class Mdirs(models.Model):
#    fileid = models.DecimalField(unique=True, max_digits=0, decimal_places=-127)
#    parent = models.DecimalField(max_digits=0, decimal_places=-127)
#    name = models.CharField(max_length=255, blank=True)
#    depth = models.IntegerField(null=True, blank=True)
#    fullname = models.CharField(max_length=2048, blank=True)
#    totalsize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    sizeontape = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    dataontape = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    nbfiles = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    nbtapes = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    nbfilesontape = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    nbfilecopiesontape = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    oldestfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    avgfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    sigfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    newestfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    oldestfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    avgfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    sigfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    newestfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    timetomigrate = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    timetorecall = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    timelostintapemarks = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    opttimetorecall = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    class Meta:
#        db_table = u'mdirs'
#
#class Wdirs(models.Model):
#    fileid = models.DecimalField(unique=True, max_digits=0, decimal_places=-127)
#    parent = models.DecimalField(max_digits=0, decimal_places=-127)
#    name = models.CharField(max_length=255, blank=True)
#    depth = models.IntegerField(null=True, blank=True)
#    fullname = models.CharField(max_length=2048, blank=True)
#    totalsize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    sizeontape = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    dataontape = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    nbfiles = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    nbtapes = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    nbfilesontape = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    nbfilecopiesontape = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    oldestfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    avgfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    sigfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    newestfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    oldestfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    avgfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    sigfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    newestfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    timetomigrate = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    timetorecall = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    timelostintapemarks = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    opttimetorecall = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    class Meta:
#        db_table = u'wdirs'
#
#class Tapehelp(models.Model):
#    fileid = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    parent = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    vid = models.CharField(max_length=6, blank=True)
#    fsize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    lastmodtime = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    class Meta:
#        db_table = u'tapehelp'
#
#class CnsClassMetadata(models.Model):
#    classid = models.IntegerField(primary_key=True)
#    name = models.CharField(unique=True, max_length=15, blank=True)
#    owner_uid = models.IntegerField(null=True, blank=True)
#    gid = models.IntegerField(null=True, blank=True)
#    min_filesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    max_filesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    flags = models.IntegerField(null=True, blank=True)
#    maxdrives = models.IntegerField(null=True, blank=True)
#    max_segsize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    migr_time_interval = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    mintime_beforemigr = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    nbcopies = models.IntegerField(null=True, blank=True)
#    nbdirs_using_class = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    retenp_on_disk = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
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
#    u_fileid = models.DecimalField(unique=True, max_digits=0, decimal_places=-127)
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
#    status_number = models.DecimalField(unique=True, null=True, max_digits=0, decimal_places=-127, blank=True)
#    stat = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    class Meta:
#        db_table = u'status_values'
#
#class TapePool(models.Model):
#    vid = models.CharField(unique=True, max_length=10)
#    model = models.CharField(max_length=8)
#    density = models.CharField(max_length=8)
#    pool = models.CharField(max_length=20)
#    library = models.CharFitoo bigeld(max_length=20)
#    capacity = models.DecimalField(max_digits=0, decimal_places=-127)
#    status = models.IntegerField(null=True, blank=True)
#    used = models.DecimalField(null=True, max_digits=8, decimal_places=3, blank=True)
#    free_space = models.DecimalField(null=True, max_digits=8, decimal_places=3, blank=True)
#    effective_free_space = models.DecimalField(null=True, max_digits=8, decimal_places=3, blank=True)
#    nbfiles = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    noseg = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    class Meta:
#        db_table = u'tape_pool'
#
#class TapePools(models.Model):
#    vid = models.CharField(unique=True, max_length=10)
#    model = models.CharField(max_length=8)
#    density = models.CharField(max_length=8)
#    pool = models.CharField(max_length=20)
#    library = models.CharField(max_length=20)
#    capacity = models.DecimalField(max_digits=0, decimal_places=-127)
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
#    native_capacity = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
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
#    rcount = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    wcount = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
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
#    capacity = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    nb_free_slots = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
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
#    tot_free_space = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    capacity = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    class Meta:
#        db_table = u'vmgr_tape_pool'
#
#class VmgrTapePoolBack(models.Model):
#    name = models.CharField(max_length=15, blank=True)
#    owner_uid = models.IntegerField(null=True, blank=True)
#    gid = models.IntegerField(null=True, blank=True)
#    tot_free_space = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    capacity = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    class Meta:
#        db_table = u'vmgr_tape_pool_back'
#
#class VmgrTapeSide(models.Model):
#    vid = models.CharField(unique=True, max_length=6)
#    poolname = models.CharField(max_length=15, blank=True)
#    status = models.IntegerField(null=True, blank=True)
#    estimated_free_space = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    nbfiles = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
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
#    fileid = models.DecimalField(unique=True, max_digits=0, decimal_places=-127)
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
#    fileid = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
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
#    failurecount = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    type = models.CharField(max_length=20, blank=True)
#    state = models.CharField(max_length=20, blank=True)
#    schemaversion = models.CharField(max_length=20)
#    release = models.CharField(max_length=20)
#    class Meta:
#        db_table = u'upgradelog'
#
##class PlanTable(models.Model):
##    statement_id = models.CharField(max_length=30, blank=True)
##    plan_id = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
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
##    search_columns = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
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
#    nbfiles = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    datasize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    avgfilesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    stddevfilesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    minfilelastmodtime = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    avgfilelastmodtime = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    stddevfilelastmodtime = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    maxfilelastmodtime = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    class Meta:
#        db_table = u'tapedata'
#
#class CnsSegMetadata(models.Model):
#    s_fileid = models.DecimalField(unique=True, max_digits=0, decimal_places=-127)
#    copyno = models.IntegerField(primary_key=True)
#    fsec = models.IntegerField(primary_key=True)
#    segsize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    s_status = models.CharField(max_length=1, blank=True)
#    vid = models.CharField(unique=True, max_length=6, blank=True)
#    fseq = models.IntegerField(unique=True, null=True, blank=True)
#    blockid = models.TextField(blank=True) # This field type is a guess.
#    compression = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    side = models.IntegerField(unique=True, null=True, blank=True)
#    checksum_name = models.CharField(max_length=16, blank=True)
#    checksum = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    class Meta:
#        db_table = u'cns_seg_metadata'
#
#class CnsFilesExistTmp(models.Model):
#    tmpfileid = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    class Meta:
#        db_table = u'cns_files_exist_tmp'
#
#class Worstmigtemptable(models.Model):
#    fullname = models.CharField(max_length=2048, blank=True)
#    timetomigrate = models.CharField(max_length=2048, blank=True)
#    timespentintapemarks = models.CharField(max_length=2048, blank=True)
#    tapemarkspart = models.CharField(max_length=2048, blank=True)
#    avgfilesize = models.CharField(max_length=2048, blank=True)
#    nbfilecopiesontape = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    totalsize = models.CharField(max_length=2048, blank=True)
#    class Meta:
#        db_table = u'worstmigtemptable'
#
#class Worstrecalltemptable(models.Model):
#    fullname = models.CharField(max_length=2048, blank=True)
#    esttimetorecall = models.CharField(max_length=2048, blank=True)
#    losttime = models.CharField(max_length=2048, blank=True)
#    lostpart = models.CharField(max_length=2048, blank=True)
#    nbtapes = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    avgfilesize = models.CharField(max_length=2048, blank=True)
#    nbfilecopiesontape = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    totalsize = models.CharField(max_length=2048, blank=True)
#    class Meta:
#        db_table = u'worstrecalltemptable'
#
#class Biggestontapetemptable(models.Model):
#    fullname = models.CharField(max_length=2048, blank=True)
#    sizeontape = models.CharField(max_length=2048, blank=True)
#    nbfilecopiesontape = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    nbtapes = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    timetomigrate = models.CharField(max_length=2048, blank=True)
#    esttimetorecall = models.CharField(max_length=2048, blank=True)
#    class Meta:
#        db_table = u'biggestontapetemptable'
#
#class Biggestondisktemptable(models.Model):
#    fullname = models.CharField(max_length=2048, blank=True)
#    sizeofdiskonlydata = models.CharField(max_length=2048, blank=True)
#    nbdiskonlyfiles = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
#    class Meta:
#        db_table = u'biggestondisktemptable'

