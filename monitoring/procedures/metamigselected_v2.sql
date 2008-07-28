CREATE OR REPLACE PROCEDURE Proc_MetaMigSelected AS

    TYPE first IS TABLE OF CASTOR_STAGER.svcclass.name%TYPE;
    TYPE secnd IS TABLE OF NUMBER;

    a first;
    b secnd;
    c secnd;
    d secnd;
    e secnd;

    mytime DATE := SYSDATE;

    BEGIN

      execute immediate 'truncate table castor_stager.monitoring_MetaMigSelected';

      SELECT * BULK COLLECT INTO a, b, c, d, e
      FROM (
	select b.name,
               round(nvl(a.minage,0),0),
               round(nvl(a.maxage,0),0),
               round(nvl(a.aveage,0),0),
               nvl(a.total,0)
        from (
	  select svc.name,
                 min(CASTOR_STAGER.gettime()-dc.creationtime) minage,
                 max(CASTOR_STAGER.gettime()-dc.creationtime) maxage,
		 avg(CASTOR_STAGER.gettime()-dc.creationtime) aveage,
                 count(*) TOTAL
          from CASTOR_STAGER.tapecopy tc,
               CASTOR_STAGER.diskcopy dc,
               CASTOR_STAGER.svcclass svc,
               CASTOR_STAGER.filesystem fs,
               CASTOR_STAGER.diskpool2svcclass dp2svc
	  where dc.castorfile = tc.castorfile
            and dc.status = 10 and tc.status in (2,3)
	    and dc.filesystem=fs.id and dp2svc.parent=fs.diskpool and dp2svc.child=svc.id
	  group by svc.name
	  order by svc.name
        ) a,
        CASTOR_STAGER.svcclass b where a.name(+)=b.name
      );

      forall f in a.first..a.last
        insert into CASTOR_STAGER.monitoring_MetaMigSelected values(mytime, a(f), b(f), c(f), d(f), e(f));

END Proc_MetaMigSelected;

