CREATE OR REPLACE PROCEDURE Proc_FILEMONITORING AS

  TYPE first IS TABLE OF varchar2(2048);
  TYPE secnd IS TABLE OF NUMBER;

  service_name first;

  nbaccess1 secnd;
  nbaccess2 secnd;
  nbaccess3 secnd;
  nbaccess4 secnd;
  nbaccess5 secnd;
  nbaccess6to10 secnd;
  sixto10total secnd;
  nbaccess11to20 secnd;
  elevento20total secnd;
  nbaccessgt20 secnd;
  over20total secnd;

  onehour secnd;
  twelvehour secnd;
  day secnd;
  twoday secnd;
  fourday secnd;
  fourplus secnd;

  onemeg secnd;
  tenmeg secnd;
  hundredmeg secnd;
  onegig secnd;
  tengig secnd;
  overtengig secnd;
  onemeg_total secnd;
  tenmeg_total secnd;
  hundredmeg_total secnd;
  onegig_total secnd;
  tengig_total secnd;
  overtengig_total secnd;

  mytime DATE := SYSDATE;

BEGIN
  execute immediate 'truncate table castor_stager.monitoring_AccessCount';
  execute immediate 'truncate table castor_stager.monitoring_FileSize';
  execute immediate 'truncate table castor_stager.monitoring_FileAge';

  SELECT name,
         sum(decode(DiskCopy.nbcopyaccesses,1,1,0)) ,
         sum(decode(DiskCopy.nbcopyaccesses,2,1,0)) ,
         sum(decode(DiskCopy.nbcopyaccesses,3,1,0)) ,
         sum(decode(DiskCopy.nbcopyaccesses,4,1,0)) ,
         sum(decode(DiskCopy.nbcopyaccesses,5,1,0)) ,
         sum(case when DiskCopy.nbcopyaccesses between 6 and 10 then 1 else 0 end) ,
         sum(case when DiskCopy.nbcopyaccesses between 6 and 10 then DiskCopy.nbcopyaccesses else 0 end) ,
         sum(case when DiskCopy.nbcopyaccesses between 11 and 20 then 1 else 0 end) ,
         sum(case when DiskCopy.nbcopyaccesses between 11 and 20 then DiskCopy.nbcopyaccesses else 0 end) ,
         sum(case when DiskCopy.nbcopyaccesses > 20 then 1 else 0 end),
         sum(case when DiskCopy.nbcopyaccesses > 20 then DiskCopy.nbcopyaccesses else 0 end),
         sum(case when DiskCopy.CREATIONTIME >= ((to_number(SYSDATE - TO_DATE(19700101,'YYYYMMDD')) * 86400)-1*3600) then 1 else 0 end) ,
         sum(case when DiskCopy.CREATIONTIME between ((to_number(SYSDATE - TO_DATE(19700101,'YYYYMMDD')) * 86400)-1*3600)
             and ((to_number(SYSDATE - TO_DATE(19700101,'YYYYMMDD')) * 86400)-12*3600) then 1 else 0 end),
         sum(case when DiskCopy.CREATIONTIME between ((to_number(SYSDATE - TO_DATE(19700101,'YYYYMMDD')) * 86400)-12*3600)
             and ((to_number(SYSDATE - TO_DATE(19700101,'YYYYMMDD')) * 86400)-24*3600) then 1 else 0 end),
         sum(case when DiskCopy.CREATIONTIME between ((to_number(SYSDATE - TO_DATE(19700101,'YYYYMMDD')) * 86400)-24*3600)
             and ((to_number(SYSDATE - TO_DATE(19700101,'YYYYMMDD')) * 86400)-48*3600) then 1 else 0 end),
         sum(case when DiskCopy.CREATIONTIME between ((to_number(SYSDATE - TO_DATE(19700101,'YYYYMMDD')) * 86400)-48*3600)
             and ((to_number(SYSDATE - TO_DATE(19700101,'YYYYMMDD')) * 86400)-96*3600) then 1 else 0 end),
         sum(case when DiskCopy.CREATIONTIME < ((to_number(SYSDATE - TO_DATE(19700101,'YYYYMMDD')) * 86400)-96*3600) then 1 else 0 end),
         sum(case when DiskCopy.diskcopysize between 0 and 1024*1024 then 1 else 0 end) ,
         sum(case when DiskCopy.diskcopysize between 0 and 1024*1024 then DiskCopy.diskcopysize else 0 end) ,
         sum(case when DiskCopy.diskcopysize between 1024*1024 and 10*1024*1024 then 1 else 0 end),
         sum(case when DiskCopy.diskcopysize between 1024*1024 and 10*1024*1024 then DiskCopy.diskcopysize else 0 end),
         sum(case when DiskCopy.diskcopysize between 10*1024*1024 and 100*1024*1024 then 1 else 0 end),
         sum(case when DiskCopy.diskcopysize between 10*1024*1024 and 100*1024*1024 then DiskCopy.diskcopysize else 0 end),
         sum(case when DiskCopy.diskcopysize between 100*1024*1024 and 1000*1024*1024 then 1 else 0 end),
         sum(case when DiskCopy.diskcopysize between 100*1024*1024 and 1000*1024*1024 then DiskCopy.diskcopysize else 0 end),
         sum(case when DiskCopy.diskcopysize between 1000*1024*1024 and 10000*10*1024*1024 then 1 else 0 end),
         sum(case when DiskCopy.diskcopysize between 1000*1024*1024 and 10000*10*1024*1024 then DiskCopy.diskcopysize else 0 end),
         sum(case when DiskCopy.diskcopysize > 10000*1024*1024 then 1 else 0 end),
         sum(case when DiskCopy.diskcopysize > 10000*1024*1024 then DiskCopy.diskcopysize else 0 end)
  BULK COLLECT INTO service_name,
                    nbaccess1 , nbaccess2 , nbaccess3 , nbaccess4, nbaccess5 ,
                    nbaccess6to10, sixto10total, nbaccess11to20, elevento20total, nbaccessgt20, over20total,
                    onehour, twelvehour, day, twoday, fourday, fourplus,
                    onemeg, onemeg_total, tenmeg, tenmeg_total, hundredmeg, hundredmeg_total,
                    onegig, onegig_total, tengig, tengig_total, overtengig, overtengig_total
    FROM DiskCopy, FileSystem, DiskPool2SvcClass, SvcClass
   WHERE DiskCopy.filesystem = FileSystem.id
     AND FileSystem.diskpool = DiskPool2SvcClass.parent
     AND DiskPool2SvcClass.child = SvcClass.id
   GROUP BY SvcClass.name;

  FORALL d in service_name.first..service_name.last
    INSERT INTO castor_stager.monitoring_AccessCount
    VALUES (mytime, service_name(d), nbaccess1(d), nbaccess2(d), nbaccess3(d),
            nbaccess4(d), nbaccess5(d), nbaccess6to10(d), sixto10total(d),
            nbaccess11to20(d), elevento20total(d), nbaccessgt20(d), over20total(d));

  FORALL d in service_name.first..service_name.last
    INSERT INTO castor_stager.monitoring_FILEAGE
    VALUES (mytime, service_name(d), onehour(d), twelvehour(d), day(d), twoday(d), fourday(d), fourplus(d));

  FORALL d in service_name.first..service_name.last
    INSERT INTO castor_stager.monitoring_FILESIZE
    VALUES (mytime, service_name(d), onemeg(d), onemeg_total(d), tenmeg(d), tenmeg_total(d), hundredmeg(d), hundredmeg_total(d), onegig(d), onegig_total(d), tengig(d), tengig_total(d), overtengig(d), overtengig_total(d));

END Proc_FILEMONITORING;
