CREATE OR REPLACE PROCEDURE Proc_ExperimentSummary AS

  TYPE first IS TABLE OF varchar2(2048);
  TYPE secnd IS TABLE OF NUMBER;

  name first;
  usage secnd;
  nbfiles secnd;
  tpusge secnd;
  tpusge_rwc secnd;

  mytime DATE := SYSDATE;

BEGIN
  select e.name,
       nvl(es.ns_usage,0), nvl(es.nbfiles,0),
       nvl(nst.tpusage,0), nvl(nst.tpusage_rwc,0)
  BULK COLLECT INTO name, usage, nbfiles, tpusge, tpusge_rwc
  from experiments e,
     exp_stats_v es,
     ns_tpstats_all_v nst
  where e.gid = es.gid
        and e.gid = nst.gid(+);


  FORALL d in name.first..name.last
        INSERT INTO monitoring_ExpSummary
        VALUES (mytime, name(d), usage(d), nbfiles(d), tpusge(d), tpusge_rwc(d));

END Proc_ExperimentSummary;


CREATE OR REPLACE PROCEDURE Proc_ExperimentSummary AS

  TYPE first IS TABLE OF varchar2(2048);
  TYPE secnd IS TABLE OF NUMBER;

  name first;
  usage secnd;
  nbfiles secnd;
  tpusge secnd;
  tpusge_rwc secnd;

  mytime DATE := SYSDATE;

BEGIN
  select e.name,
       nvl(es.ns_usage,0), nvl(es.nbfiles,0),
       nvl(nst.tpusage,0), nvl(nst.tpusage_rwc,0)
  BULK COLLECT INTO name, usage, nbfiles, tpusge, tpusge_rwc
  from experiments e,
     exp_stats_v es,
     ns_tpstats_all_v nst
  where e.gid = es.gid
        and e.gid = nst.gid(+);


  FORALL d in name.first..name.last
        INSERT INTO monitoring_ExpSummary
        VALUES (mytime, name(d), usage(d), nbfiles(d), tpusge(d), tpusge_rwc(d));

END Proc_ExperimentSummary;




CREATE OR REPLACE PROCEDURE Proc_TapeSummary AS

  TYPE first IS TABLE OF varchar2(2048);
  TYPE secnd IS TABLE OF NUMBER;

  name first;
  usage secnd;
  nbfiles secnd;

  mytime DATE := SYSDATE;

BEGIN
  select e.name,
       es.ns_usage, es.nbfiles
  BULK COLLECT INTO name, usage, nbfiles
  from experiments e,
     usr_stats_v es
  where e.gid = es.gid;


  FORALL d in name.first..name.last
        INSERT INTO monitoring_tapesummary
        VALUES (mytime, name(d), usage(d), nbfiles(d));

END Proc_TapeSummary;

