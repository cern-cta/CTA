<?php 
 include ("../lib/_oci_cache.php");
 //User account 
 include ("../../../conf/castor-mon-web/user.php");
 //Get posted data
 $service = $_GET['service'];
 //connection -- DB login 
 $conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
 if(!$conn) {
	$e = ocierror();
	print htmlentities($e['message']);
	exit;
 }
 $data_read_4h = "select nvl(sum(gigabytes),0) dataRead
                  from ".$db_instances[$service]['schema'].".tapestatnew
                  where timestamp >= sysdate - 3 - 5/1440 - 4/24
		    and facility = 2";
 $files_transferred_4h = "select nvl(sum(files),0) fileTransf
                          from ".$db_instances[$service]['schema'].".tapestatnew
			  where timestamp >= sysdate - 3 - 5/1440 - 4/24
			    and facility = 2";
 $avg_fileSize_1h = 'select nvl(round(avg("avgFileSize(MB)"),2),0) filesize
                          from '.$db_instances[$service]['schema'].'.tapestatnew
			  where timestamp >= sysdate - 3 - 5/1440 - 1/24
			    and facility = 2';
 $data_written_4h = "select nvl(sum(gigabytes),0) dataRead
                     from ".$db_instances[$service]['schema'].".tapestatnew
                     where timestamp >= sysdate - 3 - 5/1440 - 4/24
		       and facility = 1";
 $files_written_4h = "select nvl(sum(files),0) fileTransf
                      from ".$db_instances[$service]['schema'].".tapestatnew
		      where timestamp >= sysdate - 3 - 5/1440 - 4/24
			and facility = 1";
 $avg_filesize_w_1h = 'select nvl(round(avg("avgFileSize(MB)"),2),0) filesize
                       from '.$db_instances[$service]['schema'].'.tapestat
		       where timestamp >= sysdate - 3 - 5/1440 - 1/24
			 and facility = 1';
 $read_rate_1h = 'select nvl(round(avg("rate(MB/sec)"),2),0) avg_filesize
                  from '.$db_instances[$service]['schema'].'.tapestatnew
		  where facility = 2
		    and timestamp >= sysdate - 3 - 5/1440 - 1/24';
 $write_rate_1h = 'select nvl(round(avg("rate(MB/sec)"),2),0) avg_filesize
                   from '.$db_instances[$service]['schema'].'.tapestat
		   where facility = 1
		     and timestamp >= sysdate - 3 - 5/1440 - 1/24';
 $total_mounts_4h = "select nvl(sum(nbmounts),0) 
                     from ".$db_instances[$service]['schema'].".tapemountstats
		     where timestamp >= sysdate - 3 - 5/1440 - 4/24";
 $read_mounts_4h  = "select nvl(sum(nbmounts),0) 
                     from ".$db_instances[$service]['schema'].".tapemountstats
		     where timestamp >= sysdate - 3 - 5/1440 - 4/24
		       and direction = 'READ'";
 $write_mounts_4h = "select nvl(sum(nbmounts),0) 
                     from ".$db_instances[$service]['schema'].".tapemountstats
		     where timestamp >= sysdate - 3 - 5/1440 - 4/24
		       and direction = 'WRITE'";
 $files_per_mount_read_4h = "select nvl(round(avg(filespermount),2),0)
 			     from ".$db_instances[$service]['schema'].".tapestat
		             where timestamp >= sysdate - 3 - 5/1440 - 4/24
		               and facility = 2";
 $files_per_mount_write_4h = "select nvl(round(avg(filespermount),2),0)
 			     from ".$db_instances[$service]['schema'].".tapestat
		             where timestamp >= sysdate - 3 - 5/1440 - 4/24
		               and facility = 1";
 $repeat_read_mounts_24h = "select nvl(round(avg(repeat),2),0) from (
                              select tapevid, count(*) repeat
			      from ".$db_instances[$service]['schema'].".tapemountshelper
			      where tapevid is not null
			        and timestamp >= sysdate - 3 - 1
				and facility = 2
			      group by tapevid)";
 $repeat_write_mounts_24h = "select nvl(round(avg(repeat),2),0) from (
                              select tapevid, count(*) repeat
			      from ".$db_instances[$service]['schema'].".tapemountshelper
			      where tapevid is not null
			        and timestamp >= sysdate - 3 - 1
				and facility = 1
			      group by tapevid)";
 //Parse Queries
 if (!($parsed_data_read_4h = OCIParse($conn, $data_read_4h))) 
	{ echo "Error Parsing Query"; exit();}
 if (!($parsed_files_transferred_4h = OCIParse($conn, $files_transferred_4h))) 
	{ echo "Error Parsing Query"; exit();}
 if (!($parsed_avg_fileSize_1h = OCIParse($conn, $avg_fileSize_1h))) 
	{ echo "Error Parsing Query"; exit();}
 if (!($parsed_data_written_4h = OCIParse($conn, $data_written_4h))) 
	{ echo "Error Parsing Query"; exit();}
 if (!($parsed_files_written_4h = OCIParse($conn, $files_written_4h))) 
	{ echo "Error Parsing Query"; exit();}
 if (!($parsed_avg_filesize_w_1h = OCIParse($conn, $avg_filesize_w_1h))) 
	{ echo "Error Parsing Query"; exit();}	
 if (!($parsed_read_rate_1h = OCIParse($conn, $read_rate_1h))) 
	{ echo "Error Parsing Query"; exit();}
 if (!($parsed_write_rate_1h = OCIParse($conn, $write_rate_1h))) 
	{ echo "Error Parsing Query"; exit();}
 if (!($parsed_total_mounts_4h = OCIParse($conn, $total_mounts_4h))) 
	{ echo "Error Parsing Query"; exit();}
 if (!($parsed_read_mounts_4h = OCIParse($conn, $read_mounts_4h))) 
	{ echo "Error Parsing Query"; exit();}
 if (!($parsed_write_mounts_4h = OCIParse($conn, $write_mounts_4h))) 
	{ echo "Error Parsing Query"; exit();}
 if (!($parsed_files_per_mount_read_4h = OCIParse($conn, $files_per_mount_read_4h))) 
	{ echo "Error Parsing Query"; exit();}
 if (!($parsed_files_per_mount_write_4h = OCIParse($conn, $files_per_mount_write_4h))) 
	{ echo "Error Parsing Query"; exit();}
 if (!($parsed_repeat_read_mounts_24h = OCIParse($conn, $repeat_read_mounts_24h))) 
	{ echo "Error Parsing Query"; exit();}
 if (!($parsed_repeat_write_mounts_24h = OCIParse($conn, $repeat_write_mounts_24h))) 
	{ echo "Error Parsing Query"; exit();}
 //Execute Queries
 if (!OCIExecute($parsed_data_read_4h))
	{ echo "Error Executing Query";exit();}
 if (!OCIExecute($parsed_files_transferred_4h))
	{ echo "Error Executing Query";exit();}
 if (!OCIExecute($parsed_avg_fileSize_1h))
	{ echo "Error Executing Query";exit();}
 if (!OCIExecute($parsed_data_written_4h))
	{ echo "Error Executing Query";exit();}
 if (!OCIExecute($parsed_files_written_4h))
	{ echo "Error Executing Query";exit();}
 if (!OCIExecute($parsed_avg_filesize_w_1h))
	{ echo "Error Executing Query";exit();}
 if (!OCIExecute($parsed_read_rate_1h))
	{ echo "Error Executing Query";exit();}
 if (!OCIExecute($parsed_write_rate_1h))
	{ echo "Error Executing Query";exit();}
 if (!OCIExecute($parsed_total_mounts_4h))
	{ echo "Error Executing Query";exit();}
 if (!OCIExecute($parsed_read_mounts_4h))
	{ echo "Error Executing Query";exit();}
 if (!OCIExecute($parsed_write_mounts_4h))
	{ echo "Error Executing Query";exit();}
 if (!OCIExecute($parsed_files_per_mount_read_4h))
	{ echo "Error Executing Query";exit();}
 if (!OCIExecute($parsed_files_per_mount_write_4h))
	{ echo "Error Executing Query";exit();}
 if (!OCIExecute($parsed_repeat_read_mounts_24h))
	{ echo "Error Executing Query";exit();}
 if (!OCIExecute($parsed_repeat_write_mounts_24h))
	{ echo "Error Executing Query";exit();}
//fetch data
 while (OCIFetch($parsed_data_read_4h)) 
	$data_read_4h_res = OCIResult($parsed_data_read_4h,1);	
 while (OCIFetch($parsed_files_transferred_4h)) 
	$files_transferred_4h_res = OCIResult($parsed_files_transferred_4h,1);
 while (OCIFetch($parsed_avg_fileSize_1h)) 
	$avg_fileSize_1h_res = OCIResult($parsed_avg_fileSize_1h,1);
 while (OCIFetch($parsed_data_written_4h)) 
	$data_written_4h_res = OCIResult($parsed_data_written_4h,1);
 while (OCIFetch($parsed_files_written_4h)) 
	$files_written_4h_res = OCIResult($parsed_files_written_4h,1);
 while (OCIFetch($parsed_avg_filesize_w_1h)) 
	$avg_filesize_w_1h_res = OCIResult($parsed_avg_filesize_w_1h,1);
 while (OCIFetch($parsed_read_rate_1h)) 
	$read_rate_1h_res = OCIResult($parsed_read_rate_1h,1);
 while (OCIFetch($parsed_write_rate_1h)) 
	$write_rate_1h_res = OCIResult($parsed_write_rate_1h,1);
 while (OCIFetch($parsed_total_mounts_4h)) 
	$total_mounts_4h_res = OCIResult($parsed_total_mounts_4h,1);
 while (OCIFetch($parsed_read_mounts_4h)) 
	$read_mounts_4h_res = OCIResult($parsed_read_mounts_4h,1);
 while (OCIFetch($parsed_write_mounts_4h)) 
	$write_mounts_4h_res = OCIResult($parsed_write_mounts_4h,1);
 while (OCIFetch($parsed_files_per_mount_read_4h)) 
	$files_per_mount_read_4h_res = OCIResult($parsed_files_per_mount_read_4h,1);
 while (OCIFetch($parsed_files_per_mount_write_4h)) 
	$files_per_mount_write_4h_res = OCIResult($parsed_files_per_mount_write_4h,1);
 while (OCIFetch($parsed_repeat_read_mounts_24h)) 
	$repeat_read_mounts_24h_res = OCIResult($parsed_repeat_read_mounts_24h,1);
 while (OCIFetch($parsed_repeat_write_mounts_24h)) 
	$repeat_write_mounts_24h_res = OCIResult($parsed_repeat_write_mounts_24h,1);
echo "
 <html>
  <body>
   <h3> Tape Statistics </h3>
   <table>
     <tr><td>DATA READ: data volume read in GB in last 4 hours: </td><td>$data_read_4h_res</td></tr>
     <tr><td>DATA READ: number of files transferred in last 4 hours: </td><td>$files_transferred_4h_res</td></tr>
     <tr><td>DATA READ: average file size in MB: </td><td>$avg_fileSize_1h_res</td></tr>
     <tr><td>DATA WRITE: data volume written in GB in last 4 hours: </td><td>$data_written_4h_res</td></tr>
     <tr><td>DATA WRITE: number of files transferred in last 4 hours: </td><td>$files_written_4h_res</td></tr>
     <tr><td>DATA WRITE: average file size in MB: </td><td>$avg_filesize_w_1h_res</td></tr>
     <tr><td>RATE READ: read transfer rate inc drive overhead MB/sec (est.): </td><td>$read_rate_1h_res</td></tr>
     <tr><td>RATE READ: drive read transfer rate MB/sec (est.): </td><td><b>N/A</b></td></tr>
     <tr><td>RATE WRITE: write transfer rate inc drive overhead MB/sec (est.): </td><td>$write_rate_1h_res</td></tr>
     <tr><td>RATE WRITE: drive write transfer rate MB/sec (est.): </td><td><b>N/A</b></td></tr>
     <tr><td>TAPE MOUNT: successful mounts in last 4 hours: </td><td>$total_mounts_4h_res</td></tr>
     <tr><td>TAPE MOUNT: failed mounts in last 4 hours: </td><td><b>N/A</b></td></tr>
     <tr><td>TAPE MOUNT: read mounts in last 4 hours: </td><td>$read_mounts_4h_res</td></tr>
     <tr><td>TAPE MOUNT: write mounts in last 4 hours: </td><td>$write_mounts_4h_res</td></tr>
     <tr><td>TAPE QUEUES: average wait for read in secs: </td><td><b>N/A</b></td></tr>
     <tr><td>TAPE QUEUES: average wait for write in secs: </td><td><b>N/A</b></td></tr>
     <tr><td>FILES PER MOUNT: read average in last 4 hours: </td><td>$files_per_mount_read_4h_res</td></tr>
     <tr><td>FILES PER MOUNT: write average in last 4 hours: </td><td>$files_per_mount_write_4h_res</td></tr>
     <tr><td>TAPE REPEAT MOUNT: read average in last 24 hours: </td><td>$repeat_read_mounts_24h_res</td></tr>
     <tr><td>TAPE REPEAT MOUNT: write average in last 24 hours: </td><td>$repeat_write_mounts_24h_res</td></tr>
   </table>
  </body>
 </html>"; 
 
 
?>
