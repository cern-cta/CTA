-- This script garbage collects diskcopies that are replicas:
-- only the diskcopy in the filesystem with least copies is kept.
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

    update diskcopy set status = 88 where id = (
        select dc1.id
          from diskcopy dc1
         where dc1.castorfile = cf.id
	   and dc1.status = 0      -- STAGED
           and dc1.filesystem = (
		   select fsid from (
		        -- here we select the filesystems and their diskcopy count
			select dccount.filesystem as fsid
			  from diskcopy dc, filesystem fs, diskcopy dccount
			 where dc.castorfile = cf.id
			   and dc.status = 0     -- STAGED
			   and dc.filesystem = fs.id
			   and dccount.filesystem = fs.id
			 group by dccount.filesystem
			 order by count(dccount.id) desc
			)
		   where rownum < 2
		   )
	   and rownum < 2
	);
  end loop;
end;

update diskcopy set status = 8 where status = 88;
select count(*) from diskcopy where status = 88;

-- To look at the current distribution of diskcopies' replicas:
select count(*) as n, c from (
    select count(*) as c
      from Diskcopy, Castorfile
     where Diskcopy.status = 0
       and Diskcopy.castorfile = castorfile.id
     group by castorfile.id)
  group by c order by c;

-- To look at the current distribution of diskcopies in filesystems
select count(*) as c, dc.filesystem, diskserver.name
  from diskcopy dc, filesystem, diskserver
 where (dc.status = 0)
   and dc.filesystem = filesystem.id
   and filesystem.diskserver = diskserver.id
 group by dc.filesystem, diskserver.name order by c desc;
