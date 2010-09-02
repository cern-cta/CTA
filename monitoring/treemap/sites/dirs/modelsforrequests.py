# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#     * Rearrange models' order
#     * Make sure each model has one field with primary_key=True
# Feel free to rename the models, but don't rename db_table values or field names.
#
# Also note: You'll have to insert the output of 'django-admin.py sqlcustom [appname]'
# into your database.

from django.db import models

class Configschema(models.Model):
    expiry = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'configschema'

class ErrDiskcopy(models.Model):
    ora_err_number$ = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    ora_err_mesg$ = models.CharField(max_length=2000, blank=True)
    ora_err_rowid$ = models.TextField(blank=True) # This field type is a guess.
    ora_err_optyp$ = models.CharField(max_length=2, blank=True)
    ora_err_tag$ = models.CharField(max_length=2000, blank=True)
    nsfileid = models.CharField(max_length=4000, blank=True)
    timestamp = models.CharField(max_length=4000, blank=True)
    originalpool = models.CharField(max_length=4000, blank=True)
    targetpool = models.CharField(max_length=4000, blank=True)
    readlatency = models.CharField(max_length=4000, blank=True)
    copylatency = models.CharField(max_length=4000, blank=True)
    numcopiesinpools = models.CharField(max_length=4000, blank=True)
    srchost = models.CharField(max_length=4000, blank=True)
    desthost = models.CharField(max_length=4000, blank=True)
    class Meta:
        db_table = u'err_diskcopy'

class ReqdelMv(models.Model):
    timestamp = models.DateField()
    svcclass = models.CharField(max_length=255, blank=True)
    dif = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'reqdel_mv'

class GcmonitorMv(models.Model):
    total = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    avg_age = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    age_per = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    avg_size = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    size_per = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'gcmonitor_mv'

class MaintablecountersMv(models.Model):
    svcclass = models.CharField(max_length=255, blank=True)
    state = models.CharField(max_length=255, blank=True)
    num = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    per = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'maintablecounters_mv'

class MigmonitorMv(models.Model):
    svcclass = models.CharField(max_length=255, blank=True)
    migs = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'migmonitor_mv'

class SvcclassmapMv(models.Model):
    svcclass = models.CharField(max_length=255, blank=True)
    class Meta:
        db_table = u'svcclassmap_mv'

class Upgradelog(models.Model):
    username = models.CharField(max_length=64)
    schemaname = models.CharField(max_length=64)
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

class Internaldiskcopy(models.Model):
    timestamp = models.DateField()
    svcclass = models.CharField(max_length=255, blank=True)
    copies = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'internaldiskcopy'

class Diskcacheefficiencystats(models.Model):
    timestamp = models.DateField()
    interval = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    requesttype = models.CharField(max_length=255, blank=True)
    svcclass = models.CharField(max_length=255, blank=True)
    wait = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    d2d = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    recall = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    staged = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    waitperc = models.DecimalField(null=True, max_digits=38, decimal_places=2, blank=True)
    d2dperc = models.DecimalField(null=True, max_digits=38, decimal_places=2, blank=True)
    recallperc = models.DecimalField(null=True, max_digits=38, decimal_places=2, blank=True)
    stagedperc = models.DecimalField(null=True, max_digits=38, decimal_places=2, blank=True)
    total = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'diskcacheefficiencystats'

class Filesmigratedstats(models.Model):
    timestamp = models.DateField()
    interval = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    svcclass = models.CharField(max_length=255, blank=True)
    tapepool = models.CharField(max_length=255, blank=True)
    nbfiles = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    totalfilesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    numbyteswriteavg = models.DecimalField(null=True, max_digits=38, decimal_places=2, blank=True)
    class Meta:
        db_table = u'filesmigratedstats'

class Queuetimestats(models.Model):
    timestamp = models.DateField()
    interval = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    requesttype = models.CharField(max_length=255, blank=True)
    svcclass = models.CharField(max_length=255, blank=True)
    dispatched = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    minqueuetime = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    maxqueuetime = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    avgqueuetime = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    stddevqueuetime = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    medianqueuetime = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    class Meta:
        db_table = u'queuetimestats'

class Requeststats(models.Model):
    timestamp = models.DateField()
    interval = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    requesttype = models.CharField(max_length=255, blank=True)
    hostname = models.CharField(max_length=255, blank=True)
    euid = models.CharField(max_length=255, blank=True)
    requests = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    class Meta:
        db_table = u'requeststats'

class Top10Errors(models.Model):
    timestamp = models.DateField()
    interval = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    daemon = models.CharField(max_length=255, blank=True)
    nberrors = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    errormessage = models.CharField(max_length=512, blank=True)
    class Meta:
        db_table = u'top10errors'

class Latencystats(models.Model):
    timestamp = models.DateField()
    interval = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    requesttype = models.CharField(max_length=255, blank=True)
    svcclass = models.CharField(max_length=255, blank=True)
    protocol = models.CharField(max_length=255, blank=True)
    started = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    minlatencytime = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    maxlatencytime = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    avglatencytime = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    stddevlatencytime = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    medianlatencytime = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    class Meta:
        db_table = u'latencystats'

class Cacheefficiencyhelper(models.Model):
    reqid = models.CharField(max_length=36, blank=True)
    class Meta:
        db_table = u'cacheefficiencyhelper'

class Processingtimestats(models.Model):
    timestamp = models.DateField()
    interval = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    daemon = models.CharField(max_length=255, blank=True)
    requesttype = models.CharField(max_length=255, blank=True)
    svcclass = models.CharField(max_length=255, blank=True)
    requests = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    minprocessingtime = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    maxprocessingtime = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    avgprocessingtime = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    stddevprocessingtime = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    medianprocessingtime = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    class Meta:
        db_table = u'processingtimestats'

class Clientversionstats(models.Model):
    timestamp = models.DateField()
    interval = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    clientversion = models.CharField(max_length=255, blank=True)
    requests = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    class Meta:
        db_table = u'clientversionstats'

class Totallatency(models.Model):
    subreqid = models.CharField(unique=True, max_length=36)
    timestamp = models.DateField()
    nsfileid = models.DecimalField(max_digits=0, decimal_places=-127)
    totallatency = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'totallatency'

class Taperecall(models.Model):
    subreqid = models.CharField(unique=True, max_length=36)
    timestamp = models.DateField()
    tapeid = models.CharField(max_length=255, blank=True)
    tapemountstate = models.CharField(max_length=255, blank=True)
    readlatency = models.IntegerField(null=True, blank=True)
    copylatency = models.IntegerField(null=True, blank=True)
    class Meta:
        db_table = u'taperecall'

class Diskcopy(models.Model):
    nsfileid = models.DecimalField(max_digits=0, decimal_places=-127)
    timestamp = models.DateField()
    originalpool = models.CharField(max_length=255, blank=True)
    targetpool = models.CharField(max_length=255, blank=True)
    readlatency = models.IntegerField(null=True, blank=True)
    copylatency = models.IntegerField(null=True, blank=True)
    numcopiesinpools = models.IntegerField(null=True, blank=True)
    srchost = models.CharField(max_length=255, blank=True)
    desthost = models.CharField(max_length=255, blank=True)
    class Meta:
        db_table = u'diskcopy'

class Gcfiles(models.Model):
    timestamp = models.DateField()
    nsfileid = models.DecimalField(max_digits=0, decimal_places=-127)
    filesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    fileage = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    lastaccesstime = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    nbaccesses = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    gctype = models.CharField(max_length=255, blank=True)
    svcclass = models.CharField(max_length=255, blank=True)
    class Meta:
        db_table = u'gcfiles'

class Tapemountstats(models.Model):
    timestamp = models.DateField()
    interval = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    direction = models.CharField(max_length=20, blank=True)
    nbmounts = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    nbfiles = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    totalfilesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    avgruntime = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    nbfilespermount = models.IntegerField(null=True, blank=True)
    failures = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'tapemountstats'

class Tapemountshelper(models.Model):
    timestamp = models.DateField()
    facility = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    tapevid = models.CharField(max_length=20, blank=True)
    class Meta:
        db_table = u'tapemountshelper'

class Migration(models.Model):
    subreqid = models.CharField(unique=True, max_length=36)
    timestamp = models.DateField()
    reqid = models.CharField(max_length=36)
    nsfileid = models.DecimalField(max_digits=0, decimal_places=-127)
    type = models.CharField(max_length=255, blank=True)
    svcclass = models.CharField(max_length=255, blank=True)
    username = models.CharField(max_length=255, blank=True)
    state = models.CharField(max_length=255, blank=True)
    filename = models.CharField(max_length=2048, blank=True)
    totallatency = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    filesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'migration'

class Garbagecollectionstats(models.Model):
    timestamp = models.DateField()
    interval = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    diskserver = models.CharField(max_length=255, blank=True)
    requesttype = models.CharField(max_length=255, blank=True)
    deleted = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    totalfilesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    minfileage = models.IntegerField(null=True, blank=True)
    maxfileage = models.IntegerField(null=True, blank=True)
    avgfileage = models.IntegerField(null=True, blank=True)
    stddevfileage = models.IntegerField(null=True, blank=True)
    medianfileage = models.IntegerField(null=True, blank=True)
    minfilesize = models.IntegerField(null=True, blank=True)
    maxfilesize = models.IntegerField(null=True, blank=True)
    avgfilesize = models.IntegerField(null=True, blank=True)
    stddevfilesize = models.IntegerField(null=True, blank=True)
    medianfilesize = models.IntegerField(null=True, blank=True)
    class Meta:
        db_table = u'garbagecollectionstats'

class Replicationstats(models.Model):
    timestamp = models.DateField()
    interval = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    sourcesvcclass = models.CharField(max_length=255, blank=True)
    destsvcclass = models.CharField(max_length=255, blank=True)
    transferred = models.DecimalField(null=True, max_digits=38, decimal_places=3, blank=True)
    totalfilesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    minfilesize = models.IntegerField(null=True, blank=True)
    maxfilesize = models.IntegerField(null=True, blank=True)
    avgfilesize = models.IntegerField(null=True, blank=True)
    stddevfilesize = models.IntegerField(null=True, blank=True)
    medianfilesize = models.IntegerField(null=True, blank=True)
    class Meta:
        db_table = u'replicationstats'

class Filesrecalledstats(models.Model):
    timestamp = models.DateField()
    interval = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    nbfiles = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    totalfilesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    numbytesreadavg = models.DecimalField(null=True, max_digits=38, decimal_places=2, blank=True)
    class Meta:
        db_table = u'filesrecalledstats'

class Taperecalledstats(models.Model):
    timestamp = models.DateField()
    interval = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    requesttype = models.CharField(max_length=255, blank=True)
    username = models.CharField(max_length=255, blank=True)
    groupname = models.CharField(max_length=255, blank=True)
    tapevid = models.CharField(max_length=255, blank=True)
    tapestatus = models.CharField(max_length=255, blank=True)
    nbfiles = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    totalfilesize = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    mountsperday = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'taperecalledstats'

class ErrRequests(models.Model):
    ora_err_number$ = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    ora_err_mesg$ = models.CharField(max_length=2000, blank=True)
    ora_err_rowid$ = models.TextField(blank=True) # This field type is a guess.
    ora_err_optyp$ = models.CharField(max_length=2, blank=True)
    ora_err_tag$ = models.CharField(max_length=2000, blank=True)
    subreqid = models.CharField(max_length=4000, blank=True)
    timestamp = models.CharField(max_length=4000, blank=True)
    reqid = models.CharField(max_length=4000, blank=True)
    nsfileid = models.CharField(max_length=4000, blank=True)
    type = models.CharField(max_length=4000, blank=True)
    svcclass = models.CharField(max_length=4000, blank=True)
    username = models.CharField(max_length=4000, blank=True)
    state = models.CharField(max_length=4000, blank=True)
    filename = models.CharField(max_length=4000, blank=True)
    filesize = models.CharField(max_length=4000, blank=True)
    class Meta:
        db_table = u'err_requests'

class ErrLatency(models.Model):
    ora_err_number$ = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    ora_err_mesg$ = models.CharField(max_length=2000, blank=True)
    ora_err_rowid$ = models.TextField(blank=True) # This field type is a guess.
    ora_err_optyp$ = models.CharField(max_length=2, blank=True)
    ora_err_tag$ = models.CharField(max_length=2000, blank=True)
    subreqid = models.CharField(max_length=4000, blank=True)
    timestamp = models.CharField(max_length=4000, blank=True)
    nsfileid = models.CharField(max_length=4000, blank=True)
    totallatency = models.CharField(max_length=4000, blank=True)
    class Meta:
        db_table = u'err_latency'

class ErrMigration(models.Model):
    ora_err_number$ = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    ora_err_mesg$ = models.CharField(max_length=2000, blank=True)
    ora_err_rowid$ = models.TextField(blank=True) # This field type is a guess.
    ora_err_optyp$ = models.CharField(max_length=2, blank=True)
    ora_err_tag$ = models.CharField(max_length=2000, blank=True)
    subreqid = models.CharField(max_length=4000, blank=True)
    timestamp = models.CharField(max_length=4000, blank=True)
    reqid = models.CharField(max_length=4000, blank=True)
    nsfileid = models.CharField(max_length=4000, blank=True)
    type = models.CharField(max_length=4000, blank=True)
    svcclass = models.CharField(max_length=4000, blank=True)
    username = models.CharField(max_length=4000, blank=True)
    state = models.CharField(max_length=4000, blank=True)
    filename = models.CharField(max_length=4000, blank=True)
    totallatency = models.CharField(max_length=4000, blank=True)
    filesize = models.CharField(max_length=4000, blank=True)
    class Meta:
        db_table = u'err_migration'

class ErrTaperecall(models.Model):
    ora_err_number$ = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    ora_err_mesg$ = models.CharField(max_length=2000, blank=True)
    ora_err_rowid$ = models.TextField(blank=True) # This field type is a guess.
    ora_err_optyp$ = models.CharField(max_length=2, blank=True)
    ora_err_tag$ = models.CharField(max_length=2000, blank=True)
    subreqid = models.CharField(max_length=4000, blank=True)
    timestamp = models.CharField(max_length=4000, blank=True)
    tapeid = models.CharField(max_length=4000, blank=True)
    tapemountstate = models.CharField(max_length=4000, blank=True)
    readlatency = models.CharField(max_length=4000, blank=True)
    copylatency = models.CharField(max_length=4000, blank=True)
    class Meta:
        db_table = u'err_taperecall'

class ErrorsMv(models.Model):
    fac_name = models.CharField(max_length=20)
    msg_text = models.CharField(max_length=512)
    errorsum = models.DecimalField(null=True, max_digits=0, decimal_places=-127, blank=True)
    class Meta:
        db_table = u'errors_mv'

