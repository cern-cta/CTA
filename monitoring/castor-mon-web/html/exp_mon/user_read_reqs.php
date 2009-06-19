<?php
//Number of Migrated Files Distribution per Service Class
//Include necessary libraries
include ("../lib/_oci_cache.php");
//User account
include ("../../../conf/castor-mon-web/user.php");
//Get posted data
$service = $_GET['service'];
//Create new graph, enable image cache by setting countdown period(in minutes) 
//depending on selected $period. If the cached image is valid the script immediately 
//returns the cached image and exits without logining in the DB

//connection -- DB login 
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = ocierror();
	print htmlentities($e['message']);
	exit;
}
$user_incoming_reqs = "select * from (
		       select euid, sum(requests) reqs, 
                              sum(case when type = 'StageGetRequest' then requests else 0 end) get, 
                              sum(case when type = 'StagePrepareToGetRequest' then requests else 0 end) prepare_get  
		       from ".$db_instances[$service]['schema'].".requeststats
                       where euid <> '-'
                         and type in ('StagePrepareToGetRequest','StageGetRequest')
                         and timestamp >= sysdate - 1/24
                         and timestamp <= sysdate
                       group by euid
                       order by reqs desc )
                       where rownum < 11";
		      
$user_processed_reqs = "select * from (
		        select username, 
			       count(*) total, 
			       sum(case when state = 'DiskHit' then 1 else 0 end) DH, 
			       sum(case when state = 'DiskCopy' then 1 else 0 end) DC, 
			       sum(case when state = 'TapeRecall' then 1 else 0 end) TR
                        from ".$db_instances[$service]['schema'].".requests
                        where timestamp >= sysdate - 1/24
                          and timestamp <= sysdate
                        group by username
                        order by total desc) where rownum < 11";
if (!($parsed_user_incoming_reqs = OCIParse($conn, $user_incoming_reqs))) 
	{ echo "Error Parsing Query"; exit();}
if (!($parsed_user_processed_reqs = OCIParse($conn, $user_processed_reqs))) 
	{ echo "Error Parsing Query"; exit();}
if (!OCIExecute($parsed_user_incoming_reqs))
	{ echo "Error Executing Query";exit();}
if (!OCIExecute($parsed_user_processed_reqs))
	{ echo "Error Executing Query";exit();}
//fetch data into local tables
echo "
<html>
  <head>
    <title> Experiments Monitor
    </title>
      <style>
		#fonts {
			font: 12px/15px arial, helvetica, sans-serif;
		}
  </style>
  </head>
  <body>
    <table cellspacing = 3 id='fonts' bgcolor = 'lightyellow' frame = box>
      <tr>
        <td colspan = 5 align = left><b>Incoming Read Requests breakdown by User (Last Hour)</b></td>
      </tr>
      <tr>
        <td align = left><b>Username</b></td>
	<td align = left><b>Total Req.</b></td>
	<td align = left><b>StageGetRequest</b></td>
	<td colspan = 2 align = left><b>StagePrepareToGet</b></td>
      </tr>";
//$i = 0;
while (OCIFetch($parsed_user_incoming_reqs)) {
	$userid = OCIResult($parsed_user_incoming_reqs,1);
	$username = posix_getpwuid((int)$userid);
	if(!empty($username))
	  	$uname = $username['name'];
	else $uname = $userid;
	$total_reqs = OCIResult($parsed_user_incoming_reqs,2);
	$dir_get = OCIResult($parsed_user_incoming_reqs,3);
	$prep_get = OCIResult($parsed_user_incoming_reqs,4);
	echo " 
	 <tr>
	  <td align = left> $uname </td>
          <td align = left> $total_reqs</td>
	  <td align = left> $dir_get</td>
	  <td colspan = 2 align = left> $prep_get</td>
         </tr>";
// 	$users_incoming[$i] = $uname;
// 	$inc_reqs[$uname]['total'] = $total;
// 	$inc_reqs[$uname]['get'] = $dir_get;
// 	$inc_reqs[$uname]['prep_get'] = $prep_get;
// 	$i++;
}
echo "<tr>
        <td colspan = 7 align = left><b>Processed Read Requests breakdown by User (Last Hour)</b></td>
      </tr>
      <tr>
        <td align = left><b>Username</b></td>
	<td align = left><b>Total Req.</b></td>
	<td align = left><b>Disk Cache Hits</b></td>
	<td align = left><b>D2D Copies</b></td>
	<td align = left><b>Tape Recalls</b></td>
      </tr>";
//$i = 0;
while (OCIFetch($parsed_user_processed_reqs)) {
        $user_name = OCIResult($parsed_user_processed_reqs,1);
	$processed = OCIResult($parsed_user_processed_reqs,2);
	$dh = OCIResult($parsed_user_processed_reqs,3);
	$dc = OCIResult($parsed_user_processed_reqs,4);
	$tr = OCIResult($parsed_user_processed_reqs,5);
	echo " 
	 <tr>
	  <td align = left> $user_name </td>
          <td align = left> $processed</td>
	  <td align = left> $dh</td>
	  <td align = left> $dc</td>
	  <td align = left> $tr</td>
         </tr>";
// 	$users_processed[$i] = $user_name;
// 	$proc_reqs[$user_name]['total'] = $processed;
// 	$proc_reqs[$user_name]['prc_get'] = $proc_get;
// 	$proc_reqs[$user_name]['proc_pre_get'] = $proc_prep_get;
// 	$proc_reqs[$user_name]['diskhit'] = $dh;
// 	$proc_reqs[$user_name]['diskcopy'] = $dc;
// 	$proc_reqs[$user_name]['taperecall'] = $tr;
// 	$i++;
}
echo "</table>
  </body>
</html>";
?>










