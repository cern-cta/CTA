-- This script garbage collects diskcopies that are replicas:
-- only the diskcopy in the emptiest filesystem is kept.
-- Note that for files with more than 2 replicas you have to run it more times
begin
  for cf in (
        select id from
        (select count(*) as c, castorfile.id
          from Diskcopy, Castorfile
         where Diskcopy.castorfile = castorfile.id
           and Diskcopy.status = 0    -- STAGED
         group by castorfile.id)
        where c > 1) loop
    update diskcopy set status = 8 where id = (   -- GCCANDIDATE
    select id from (
        select dc.id
          from diskcopy dc, filesystem fs
         where dc.status = 0
           and dc.castorfile = cf.id
           and dc.filesystem = fs.id
         order by fs.free asc
         )
     where ROWNUM < 2);
  end loop;
end;

-- To look at the current distribution of replicas in diskcopies:
select count(*) as n, c from (
    select count(*) as c
      from Diskcopy, Castorfile
     where Diskcopy.castorfile = castorfile.id
       and Diskcopy.status = 0    -- STAGED
     group by castorfile.id)
  group by c order by c;
