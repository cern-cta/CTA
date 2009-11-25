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
			sum(case when requesttype = 'StagePutRequest' then requests else 0 end) put, 
			sum(case when requesttype = 'StagePrepareToPutRequest' then requests else 0 end) prepare_put,
			sum(case when requesttype = 'StagePutDoneRequest' then requests else 0 end) put_done
		       from ".$db_instances[$service]['schema'].".requeststats
		       where euid <> '-'
			and requesttype in ('StagePrepareToPutRequest','StagePutRequest','StagePutDoneRequest')
			and timestamp >= sysdate - 1/24
			and timestamp <= sysdate
		       group by euid
		       order by reqs desc )
		       where rownum < 11";
		      
$user_processed_reqs = "select * from (
			select username, count(*) total,
			   sum(case when type = 'StagePutRequest' then 1 else 0 end) put,
			   sum(case when type = 'StagePrepareToPutRequest' then 1 else 0 end) prep_put,
			   sum(case when type = 'StagePutDoneRequest' then 1 else 0 end) put_done
			from ".$db_instances[$service]['schema'].".migration
			where timestamp >= sysdate - 1/24
			  and timestamp <= sysdate
			  and type in ('StagePutRequest','StagePrepareToPutRequest','StagePutDoneRequest')
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
	<td align = left><b>Write Request</b></td>
	<td align = left><b>Prepare to Write</b></td>
	<td align = left><b>Write Done</b></td>
      </tr>";
while (OCIFetch($parsed_user_incoming_reqs)) {
	$userid = OCIResult($parsed_user_incoming_reqs,1);
	$username = posix_getpwuid((int)$userid);
	if(!empty($username))
	  	$uname = $username['name'];
	else $uname = $userid;
	$total_reqs = OCIResult($parsed_user_incoming_reqs,2);
	$put = OCIResult($parsed_user_incoming_reqs,3);
	$prep_put = OCIResult($parsed_user_incoming_reqs,4);
	$put_done = OCIResult($parsed_user_incoming_reqs,5);
	echo " 
	 <tr>
	  <td align = left> $uname </td>
          <td align = left> $total_reqs</td>
	  <td align = left> $put</td>
	  <td align = left> $prep_put</td>
	  <td align = left> $put_done</td>
         </tr>";
}
echo "<tr>
        <td colspan = 5 align = left><b>Processed Write Requests breakdown by User (Last Hour)</b></td>
      </tr>
      <tr>
        <td align = left><b>Username</b></td>
	<td align = left><b>Total Req.</b></td>
	<td align = left><b>Write Req.</b></td>
	<td align = left><b>Prepare to Write Req.</b></td>
	<td align = left><b>Write Done Req.</b></td>
      </tr>";
while (OCIFetch($parsed_user_processed_reqs)) {
        $user_name = OCIResult($parsed_user_processed_reqs,1);
	$processed = OCIResult($parsed_user_processed_reqs,2);
	$put_req = OCIResult($parsed_user_processed_reqs,3);
	$p_put = OCIResult($parsed_user_processed_reqs,4);
	$done = OCIResult($parsed_user_processed_reqs,5);
	echo " 
	 <tr>
	  <td align = left> $user_name </td>
          <td align = left> $processed</td>
	  <td align = left> $put_req</td>
	  <td align = left> $p_put</td>
	  <td align = left> $done</td>
         </tr>";
}
echo "</table>
  </body>
</html>";
?>










