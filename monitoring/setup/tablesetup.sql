grant select on svcclass to castor_read;
grant select on diskserver to castor_read;
grant select on filesystem to castor_read;
grant select on castorfile to castor_read;
grant select on diskcopy to castor_read;
grant select on tapecopy to castor_read;
grant select on diskpool2svcclass  to castor_read;
grant select on subrequest to castor_read;
grant select on stream to castor_read;
grant select on segment to castor_read;

CREATE TABLE monitoring_RecallStall (
  runtime 	DATE NOT NULL,
  svcclass      VARCHAR2(2048),
  onehour	NUMBER,
  sixhour	NUMBER,
  twelvehour	NUMBER,
  day		NUMBER,
  twoday	NUMBER,
  older		NUMBER
);

GRANT SELECT ON castor_stager.monitoring_RecallStall TO castor_read;
GRANT INSERT ON castor_stager.monitoring_RecallStall TO castor_read;

@ ../procedures/recallstall.sql

GRANT execute ON castor_stager.Proc_RecallStall TO castor_read;

CREATE TABLE monitoring_MigStall (
  runtime 	DATE NOT NULL,
  svcclass      VARCHAR2(2048),
  onehour	NUMBER,
  sixhour	NUMBER,
  twelvehour	NUMBER,
  day		NUMBER,
  twoday	NUMBER,
  older		NUMBER
);

GRANT SELECT ON castor_stager.monitoring_MigStall TO castor_read;
GRANT INSERT ON castor_stager.monitoring_MigStall TO castor_read;

@ ../procedures/migstall.sql

GRANT execute ON castor_stager.Proc_MigStall TO castor_read;

CREATE TABLE monitoring_TableSize (
  runtime 	DATE NOT NULL,
  subrequest	NUMBER,
  diskcopy      NUMBER,
  tapecopy      NUMBER,
  castorfile    NUMBER,
  id2type       NUMBER
);

GRANT SELECT ON castor_stager.monitoring_TableSize TO castor_read;
GRANT INSERT ON castor_stager.monitoring_TableSize TO castor_read;

@ ../procedures/tablesize.sql

GRANT execute ON castor_stager.Proc_TableSize TO castor_read;


CREATE TABLE monitoring_DiskServerStat (
  runtime 	DATE NOT NULL,
  diskserver    VARCHAR2(2048),
  filesystem	VARCHAR2(2048),
  status	NUMBER,
  total 	NUMBER
);

GRANT SELECT ON castor_stager.monitoring_DiskServerStat TO castor_read;
GRANT INSERT ON castor_stager.monitoring_DiskServerStat TO castor_read;

@ ../procedures/diskserver.sql

GRANT execute ON castor_stager.Proc_DiskServerStat TO castor_read;

CREATE TABLE monitoring_Segment (
  runtime 	DATE NOT NULL,
  svcclass      VARCHAR2(2048),
  unprocessed	NUMBER,
  filecopied	NUMBER,
  failed	NUMBER,
  selected	NUMBER,
  retried	NUMBER
);

GRANT SELECT ON castor_stager.monitoring_Segment TO castor_read;
GRANT INSERT ON castor_stager.monitoring_Segment TO castor_read;

@ ../procedures/segment.sql

GRANT execute ON castor_stager.Proc_Segment TO castor_read;

CREATE TABLE monitoring_STREAM (
  runtime 	DATE NOT NULL,
  svcclass     	VARCHAR2(2048),
  pending 	NUMBER,
  waitdrive	NUMBER,
  waitmount	NUMBER,
  running	NUMBER,
  waitspace 	NUMBER,
  created	NUMBER
);

GRANT SELECT ON castor_stager.monitoring_STREAM TO castor_read;
GRANT INSERT ON castor_stager.monitoring_STREAM TO castor_read;

@ ../procedures/stream.sql

GRANT execute ON castor_stager.Proc_STREAM TO castor_read;

CREATE TABLE monitoring_SubRequest (
  runtime 		DATE NOT NULL,
  svcclass     		VARCHAR2(2048),
  startval		NUMBER,
  restart		NUMBER,
  retry			NUMBER,
  waitshed 		NUMBER,
  waitttaperecall	NUMBER,
  waitsubrequest	NUMBER,
  ready			NUMBER,
  failed		NUMBER,
  finished		NUMBER,
  failedfinished	NUMBER,
  failedanswering	NUMBER
);

GRANT SELECT on castor_stager.monitoring_SubRequest TO castor_read;
GRANT INSERT ON castor_stager.monitoring_SubRequest TO castor_read;

@ ../procedures/subrequest.sql

GRANT execute ON castor_stager.Proc_SubRequest TO castor_read;

CREATE TABLE monitoring_Tape (
  runtime 	DATE NOT NULL,
  svcclass              VARCHAR2(2048),
  unused 	NUMBER,
  pending	NUMBER,
  waitdrive	NUMBER,
  waitmount 	NUMBER,
  mounted	NUMBER,
  finished	NUMBER,
  failed	NUMBER,
  unknown	NUMBER
);

GRANT SELECT ON castor_stager.monitoring_Tape TO castor_read;
GRANT INSERT ON castor_stager.monitoring_Tape TO castor_read;

@ ../procedures/tape.sql

GRANT execute ON castor_stager.Proc_Tape TO castor_read;

CREATE TABLE monitoring_TAPECOPY (
  runtime 	DATE NOT NULL,
  svcclass     	VARCHAR2(2048),
  created	NUMBER,
  tobemigrated 	NUMBER,
  waitinstreams	NUMBER,
  selected 	NUMBER,
  toberecalled	NUMBER,
  staged	NUMBER,
  failed	NUMBER
);
GRANT SELECT ON castor_stager.monitoring_TAPECOPY TO castor_read;
GRANT INSERT ON castor_stager.monitoring_TAPECOPY TO castor_read;

@ ../procedures/tapecopy.sql

GRANT execute ON castor_stager.Proc_TAPECOPY TO castor_read;

CREATE TABLE monitoring_DISKCOPY (
  runtime 		DATE NOT NULL,
  svcclass     		VARCHAR2(2048),
  staged 	 	NUMBER,
  waitdisk2diskcopy 	NUMBER,
  waittaperecall 	NUMBER,
  deleted		NUMBER,
  failed 		NUMBER,
  waitfs 		NUMBER,
  stageout		NUMBER,
  invalid 		NUMBER,
  gccandidate 		NUMBER,
  beingdeleted 	 	NUMBER,
  canbemigr 		NUMBER,
  waitfsscheduling	NUMBER
);
GRANT SELECT ON castor_stager.monitoring_DISKCOPY TO castor_read;
GRANT INSERT ON castor_stager.monitoring_DISKCOPY TO castor_read;

@ ../procedures/diskcopy.sql

GRANT execute ON castor_stager.Proc_DISKCOPY TO castor_read;

CREATE TABLE monitoring_STAGEOUT (
  runtime 		DATE NOT NULL,
  svcclass     		VARCHAR2(2048),
  creationtime 		NUMBER,
  mntpoint     		VARCHAR2(2048),
  lastknownfilename 	VARCHAR2(2048),
  name 			VARCHAR2(2048),
  path 			VARCHAR2(2048)
);

GRANT SELECT ON castor_stager.monitoring_STAGEOUT TO castor_read;
GRANT INSERT ON castor_stager.monitoring_STAGEOUT TO castor_read;

@ ../procedures/stageout.sql

GRANT execute ON castor_stager.Proc_STAGEOUT TO castor_read;

CREATE TABLE monitoring_WAITDISK2DISKCOPY (
  runtime 		DATE NOT NULL,
  svcclass     		VARCHAR2(2048),
  creationtime 		NUMBER,
  mntpoint     		VARCHAR2(2048),
  lastknownfilename 	VARCHAR2(2048),
  name 			VARCHAR2(2048),
  path 			VARCHAR2(2048)
);

GRANT SELECT ON castor_stager.monitoring_WAITDISK2DISKCOPY TO castor_read;
GRANT INSERT ON castor_stager.monitoring_WAITDISK2DISKCOPY TO castor_read;

@ ../procedures/oldwaitdisk2diskcopyfiles.sql

GRANT execute ON castor_stager.Proc_WAITDISK2DISKCOPY TO castor_read;

CREATE TABLE monitoring_waittaperecall (
  runtime 		DATE NOT NULL,
  svcclass     		VARCHAR2(2048),
  creationtime 		NUMBER,
  mntpoint     		VARCHAR2(2048),
  lastknownfilename 	VARCHAR2(2048),
  name 			VARCHAR2(2048),
  path 			VARCHAR2(2048)
);

GRANT SELECT ON castor_stager.monitoring_waittaperecall TO castor_read;
GRANT INSERT ON castor_stager.monitoring_waittaperecall TO castor_read;

@ ../procedures/waittaperecall.sql

GRANT execute ON castor_stager.Proc_waittaperecall TO castor_read;

CREATE TABLE monitoring_AccessCount (
  runtime 		DATE NOT NULL,
  svcclass 		VARCHAR2(2048),
  nbaccess1 		NUMBER,
  nbaccess2 		NUMBER,
  nbaccess3 		NUMBER,
  nbaccess4 		NUMBER,
  nbaccess5 		NUMBER,
  nbaccess6to10 	NUMBER,
  sixtotentotal 	NUMBER,
  nbaccess11to20 	NUMBER,
  eleventotwentytotal 	NUMBER,
  nbaccessgt20 		NUMBER,
  over20total 		NUMBER
);

GRANT SELECT ON castor_stager.monitoring_AccessCount TO castor_read;
GRANT INSERT ON castor_stager.monitoring_AccessCount TO castor_read;

CREATE TABLE monitoring_FileAge (
  runtime 	DATE NOT NULL,
  svcclass 	VARCHAR2(2048),
  onehour  	NUMBER,
  twelvehour 	NUMBER,
  day 		NUMBER,
  twoday 	NUMBER,
  fourday 	NUMBER,
  fourplus 	NUMBER
);

GRANT SELECT ON castor_stager.monitoring_FileAge TO castor_read;
GRANT INSERT ON castor_stager.monitoring_FileAge TO castor_read;

CREATE TABLE monitoring_FILESIZE (
   runtime 		DATE NOT NULL,
   svcclass    		VARCHAR2(2048),
   onemeg        	NUMBER,
   onemeg_total  	NUMBER,
   tenmeg        	NUMBER,
   tenmeg_total  	NUMBER,
   hundredmeg    	NUMBER,
   hundredmeg_total  	NUMBER,
   onegig            	NUMBER,
   onegig_total      	NUMBER,
   tengig            	NUMBER,
   tengig_total      	NUMBER,
   overtengig        	NUMBER,
   overtengig_total  	NUMBER
);

GRANT SELECT ON castor_stager.monitoring_FILESIZE TO castor_read;
GRANT INSERT ON castor_stager.monitoring_FILESIZE TO castor_read;

@ ../procedures/filemonitoring_v3.sql

GRANT execute ON castor_stager.Proc_FILEMONITORING TO castor_read;

CREATE TABLE monitoring_VeryOldFiles (
  runtime 		DATE NOT NULL,
  svcclass     		VARCHAR2(2048),
  creationtime 		NUMBER,
  mntpoint     		VARCHAR2(2048),
  lastknownfilename 	VARCHAR2(2048),
  name 			VARCHAR2(2048),
  path 			VARCHAR2(2048)
);

GRANT SELECT ON castor_stager.monitoring_VeryOldFiles TO castor_read;
GRANT INSERT ON castor_stager.monitoring_VeryOldFiles TO castor_read;

@ ../procedures/oldfiles_v2.sql

GRANT execute ON castor_stager.Proc_OLDFILES TO castor_read;

CREATE TABLE monitoring_MetaMigPending (
  runtime 	DATE NOT NULL,
  svcclass 	VARCHAR2(2048),
  min_age 	NUMBER,
  max_age  	NUMBER,
  avg_age 	NUMBER,
  count 	NUMBER
);

GRANT SELECT ON castor_stager.monitoring_MetaMigPending TO castor_read;
GRANT INSERT ON castor_stager.monitoring_MetaMigPending TO castor_read;

@ ../procedures/metamigpending_v2.sql

GRANT execute ON castor_stager.Proc_MetaMigPending TO castor_read;

CREATE TABLE monitoring_MetaMigSelected (
  runtime 	DATE NOT NULL,
  svcclass 	VARCHAR2(2048),
  min_age 	NUMBER,
  max_age  	NUMBER,
  avg_age 	NUMBER,
  count 	NUMBER
);

GRANT SELECT ON castor_stager.monitoring_MetaMigSelected TO castor_read;
GRANT INSERT ON castor_stager.monitoring_MetaMigSelected TO castor_read;

@ ../procedures/metamigselected_v2.sql

GRANT execute ON castor_stager.Proc_MetaMigSelected TO castor_read;

CREATE TABLE monitoring_MetaRecallRunning (
    runtime 	DATE NOT NULL,
    svcclass 	VARCHAR2(2048),
    minage 	NUMBER,
    maxage 	NUMBER,
    avg 	NUMBER,
    total 	NUMBER
);

GRANT SELECT ON castor_stager.monitoring_MetaRecallRunning TO castor_read;
GRANT INSERT ON castor_stager.monitoring_MetaRecallRunning TO castor_read;

@ ../procedures/metarecallrunning.sql

GRANT EXECUTE ON castor_stager.Proc_MetaRecallRunning TO castor_read;

CREATE TABLE monitoring_OldStageInFiles (
  runtime 		DATE NOT NULL,
  svcclass     		VARCHAR2(2048),
  creationtime 		NUMBER,
  mntpoint     		VARCHAR2(2048),
  lastknownfilename 	VARCHAR2(2048),
  name 			VARCHAR2(2048),
  path 			VARCHAR2(2048)
);

GRANT SELECT ON castor_stager.monitoring_OldStageInFiles TO castor_read;
GRANT INSERT ON castor_stager.monitoring_OldStageInFiles TO castor_read;

@ ../procedures/oldstageinfiles.sql

GRANT EXECUTE ON castor_stager.Proc_OldStageInFiles TO castor_read;