# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#     * Rearrange models' order
#     * Make sure each model has one field with primary_key=True
# Feel free to rename the models, but don't rename db_table values or field names.
#
# Also note: You'll have to insert the output of 'django-admin.py sqlcustom [appname]'
# into your database.

from django.db import models

class Dirs(models.Model):
    fileid = models.DecimalField(unique=True, max_digits=0, decimal_places=-127)
    parent = models.DecimalField(max_digits=0, decimal_places=-127)
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
    avgfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    sigfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    newestfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    oldestfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    avgfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    sigfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    newestfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    timetomigrate = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    timetorecall = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    timelostintapemarks = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    opttimetorecall = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'dirs'

class Ydirs(models.Model):
    fileid = models.DecimalField(unique=True, max_digits=0, decimal_places=-127)
    parent = models.DecimalField(max_digits=0, decimal_places=-127)
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
    avgfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    sigfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    newestfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    oldestfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    avgfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    sigfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    newestfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    timetomigrate = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    timetorecall = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    timelostintapemarks = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    opttimetorecall = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'ydirs'

class Mdirs(models.Model):
    fileid = models.DecimalField(unique=True, max_digits=0, decimal_places=-127)
    parent = models.DecimalField(max_digits=0, decimal_places=-127)
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
    avgfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    sigfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    newestfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    oldestfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    avgfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    sigfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    newestfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    timetomigrate = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    timetorecall = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    timelostintapemarks = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    opttimetorecall = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'mdirs'

class Wdirs(models.Model):
    fileid = models.DecimalField(unique=True, max_digits=0, decimal_places=-127)
    parent = models.DecimalField(max_digits=0, decimal_places=-127)
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
    avgfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    sigfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    newestfilelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    oldestfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    avgfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    sigfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    newestfileontapelastmod = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    timetomigrate = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    timetorecall = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    timelostintapemarks = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    opttimetorecall = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'wdirs'

class Tapehelp(models.Model):
    fileid = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    parent = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    vid = models.CharField(max_length=6, blank=True)
    fsize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    lastmodtime = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'tapehelp'

class Tapedata(models.Model):
    vid = models.CharField(max_length=6, blank=True)
    nbfiles = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    datasize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    avgfilesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    stddevfilesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    minfilelastmodtime = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    avgfilelastmodtime = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    stddevfilelastmodtime = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    maxfilelastmodtime = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'tapedata'

class CnsClassMetadata(models.Model):
    classid = models.IntegerField(primary_key=True)
    name = models.CharField(unique=True, max_length=15, blank=True)
    owner_uid = models.IntegerField(null=True, blank=True)
    gid = models.IntegerField(null=True, blank=True)
    min_filesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    max_filesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    flags = models.IntegerField(null=True, blank=True)
    maxdrives = models.IntegerField(null=True, blank=True)
    max_segsize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    migr_time_interval = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    mintime_beforemigr = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    nbcopies = models.IntegerField(null=True, blank=True)
    retenp_on_disk = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'cns_class_metadata'

class CnsFileMetadata(models.Model):
    fileid = models.DecimalField(max_digits=0, decimal_places=-127, primary_key=True)
    parent_fileid = models.DecimalField(unique=True, null=True, max_digits=0, decimal_places=-127, blank=True)
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

class CnsTpPool(models.Model):
    classid = models.IntegerField(unique=True, null=True, blank=True)
    tape_pool = models.CharField(unique=True, max_length=15, blank=True)
    class Meta:
        db_table = u'cns_tp_pool'

class CnsUserMetadata(models.Model):
    u_fileid = models.DecimalField(unique=True, max_digits=0, decimal_places=-127)
    comments = models.CharField(max_length=255, blank=True)
    class Meta:
        db_table = u'cns_user_metadata'

class Experiments(models.Model):
    name = models.CharField(max_length=8, blank=True)
    gid = models.IntegerField(primary_key=True)
    class Meta:
        db_table = u'experiments'

class StatusValuesTobedroped(models.Model):
    status_string = models.CharField(max_length=100, blank=True)
    status_number = models.DecimalField(unique=True, null=True, max_digits=0, decimal_places=-127, blank=True)
    stat = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'status_values_tobedroped'

class TapePoolTobedroped(models.Model):
    vid = models.CharField(unique=True, max_length=10)
    model = models.CharField(max_length=8)
    density = models.CharField(max_length=8)
    pool = models.CharField(max_length=20)
    library = models.CharField(max_length=20)
    capacity = models.DecimalField(max_digits=0, decimal_places=-127)
    status = models.IntegerField(null=True, blank=True)
    used = models.DecimalField(null=True, max_digits=8, decimal_places=3, blank=True)
    free_space = models.DecimalField(null=True, max_digits=8, decimal_places=3, blank=True)
    effective_free_space = models.DecimalField(null=True, max_digits=8, decimal_places=3, blank=True)
    nbfiles = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    noseg = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'tape_pool_tobedroped'

class TapePoolsTobedroped(models.Model):
    vid = models.CharField(unique=True, max_length=10)
    model = models.CharField(max_length=8)
    density = models.CharField(max_length=8)
    pool = models.CharField(max_length=20)
    library = models.CharField(max_length=20)
    capacity = models.DecimalField(max_digits=0, decimal_places=-127)
    status = models.IntegerField(null=True, blank=True)
    used = models.DecimalField(null=True, max_digits=8, decimal_places=3, blank=True)
    free_space = models.DecimalField(null=True, max_digits=8, decimal_places=3, blank=True)
    effective_free_space = models.DecimalField(null=True, max_digits=8, decimal_places=3, blank=True)
    class Meta:
        db_table = u'tape_pools_tobedroped'

class CnsSymlinks(models.Model):
    fileid = models.DecimalField(unique=True, max_digits=0, decimal_places=-127)
    linkname = models.CharField(max_length=1023, blank=True)
    class Meta:
        db_table = u'cns_symlinks'

class Upgradelog(models.Model):
    username = models.CharField(max_length=64)
    machine = models.CharField(max_length=64)
    program = models.CharField(max_length=48)
    startdate = models.DateTimeField(null=True, blank=True)
    enddate = models.DateTimeField(null=True, blank=True)
    failurecount = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    type = models.CharField(max_length=20, blank=True)
    state = models.CharField(max_length=20, blank=True)
    schemaversion = models.CharField(max_length=20)
    release = models.CharField(max_length=20)
    class Meta:
        db_table = u'upgradelog'

class SteveSegMetadata180210(models.Model):
    s_fileid = models.DecimalField(max_digits=0, decimal_places=-127)
    copyno = models.IntegerField()
    fsec = models.IntegerField()
    segsize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    s_status = models.CharField(max_length=1, blank=True)
    vid = models.CharField(max_length=6, blank=True)
    fseq = models.IntegerField(null=True, blank=True)
    blockid = models.TextField(blank=True) # This field type is a guess.
    compression = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    side = models.IntegerField(null=True, blank=True)
    checksum_name = models.CharField(max_length=16, blank=True)
    checksum = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'steve_seg_metadata_18_02_10'

class SteveOpsSegrecovery180210(models.Model):
    fileid = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    filesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    segsize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'steve_ops_segrecovery_18_02_10'

class SteveFileMetadata180210(models.Model):
    fileid = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    parent_fileid = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    name = models.CharField(max_length=255, blank=True)
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
    guid = models.CharField(max_length=36, blank=True)
    csumtype = models.CharField(max_length=2, blank=True)
    csumvalue = models.CharField(max_length=32, blank=True)
    acl = models.CharField(max_length=3900, blank=True)
    class Meta:
        db_table = u'steve_file_metadata_18_02_10'

class SteveLostI04097180210(models.Model):
    fileid = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'steve_lost_i04097_18_02_10'

class SteveI04097FileMetadata(models.Model):
    fileid = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    parent_fileid = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    name = models.CharField(max_length=255, blank=True)
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
    guid = models.CharField(max_length=36, blank=True)
    csumtype = models.CharField(max_length=2, blank=True)
    csumvalue = models.CharField(max_length=32, blank=True)
    acl = models.CharField(max_length=3900, blank=True)
    class Meta:
        db_table = u'steve_i04097_file_metadata'

class SteveI04097SegMetadata(models.Model):
    s_fileid = models.DecimalField(max_digits=0, decimal_places=-127)
    copyno = models.IntegerField()
    fsec = models.IntegerField()
    segsize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    s_status = models.CharField(max_length=1, blank=True)
    vid = models.CharField(max_length=6, blank=True)
    fseq = models.IntegerField(null=True, blank=True)
    blockid = models.TextField(blank=True) # This field type is a guess.
    compression = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    side = models.IntegerField(null=True, blank=True)
    checksum_name = models.CharField(max_length=16, blank=True)
    checksum = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'steve_i04097_seg_metadata'

class SebDenorm(models.Model):
    fileid = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    parent_fileid = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    guid = models.CharField(max_length=36, blank=True)
    name = models.CharField(max_length=255, blank=True)
    filemode = models.IntegerField(null=True, blank=True)
    nlink = models.IntegerField(null=True, blank=True)
    owner_uid = models.IntegerField(null=True, blank=True)
    gid = models.IntegerField(null=True, blank=True)
    filesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    atime = models.IntegerField(null=True, blank=True)
    mtime = models.IntegerField(null=True, blank=True)
    ctime = models.IntegerField(null=True, blank=True)
    fileclass = models.IntegerField(null=True, blank=True)
    status = models.CharField(max_length=1, blank=True)
    copyno = models.IntegerField(null=True, blank=True)
    segsize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    vid = models.CharField(max_length=2048, blank=True)
    class Meta:
        db_table = u'seb_denorm'

class CnsSegMetadata(models.Model):
    s_fileid = models.DecimalField(unique=True, max_digits=0, decimal_places=-127)
    copyno = models.IntegerField(primary_key=True)
    fsec = models.IntegerField(primary_key=True)
    segsize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    s_status = models.CharField(max_length=1, blank=True)
    vid = models.CharField(unique=True, max_length=6, blank=True)
    fseq = models.IntegerField(unique=True, null=True, blank=True)
    blockid = models.TextField(blank=True) # This field type is a guess.
    compression = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    side = models.IntegerField(unique=True, null=True, blank=True)
    checksum_name = models.CharField(max_length=16, blank=True)
    checksum = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'cns_seg_metadata'

class CnsFilesExistTmp(models.Model):
    tmpfileid = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'cns_files_exist_tmp'

