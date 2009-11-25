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
$new_inc_put_reqs = "select nvl(sum(requests),0) reqs
                     from ".$db_instances[$service]['schema'].".requeststats
                     where timestamp >= sysdate - 1/24
		       and timestamp < sysdate
		       and euid = '-'
		       and requesttype  = 'StagePutRequest'";
$new_space_reserve_reqs = "select nvl(sum(requests),0) reqs
			   from ".$db_instances[$service]['schema'].".requeststats
			   where timestamp >= sysdate - 1/24
			     and timestamp < sysdate
			     and euid = '-'
			     and requesttype  = 'StagePrepareToPutRequest'";
$put_done_reqs = "select nvl(sum(requests),0) reqs
		  from ".$db_instances[$service]['schema'].".requeststats
		  where timestamp >= sysdate - 1/24
		    and timestamp < sysdate
		    and euid = '-'
		    and requesttype  = 'StagePutDoneRequest'";
$dispatched_reqs = "select nvl(sum(dispatched),0) dispatched, round(nvl(avg(avgqueuetime),0),3) avg_queue_time
                    from ".$db_instances[$service]['schema'].".queuetimestats
		    where timestamp >= sysdate - 1/24
		      and timestamp < sysdate
		      and requesttype = 'StagePutRequest'";
$latency_reqs = "select nvl(sum(started),0) reqs, round(nvl(avg(avglatencytime),0),3) avg_latency_time
                    from ".$db_instances[$service]['schema'].".latencystats
		    where timestamp >= sysdate - 1/24
		      and timestamp < sysdate
		      and requesttype = 'StagePutRequest'";

if (!($parsed_inc_put_reqs = OCIParse($conn, $new_inc_put_reqs))) 
	{ echo "Error Parsing Query"; exit();}
if (!($parsed_space_reserve_reqs = OCIParse($conn, $new_space_reserve_reqs))) 
	{ echo "Error Parsing Query"; exit();}
if (!($parsed_done_reqs = OCIParse($conn, $put_done_reqs))) 
	{ echo "Error Parsing Query"; exit();}
if (!($parsed_dispatched_reqs = OCIParse($conn, $dispatched_reqs))) 
	{ echo "Error Parsing Query"; exit();}
if (!($parsed_latency_reqs = OCIParse($conn, $latency_reqs))) 
	{ echo "Error Parsing Query"; exit();}
if (!OCIExecute($parsed_inc_put_reqs))
	{ echo "Error Executing Query";exit();}
if (!OCIExecute($parsed_space_reserve_reqs))
	{ echo "Error Executing Query";exit();}
if (!OCIExecute($parsed_done_reqs))
	{ echo "Error Executing Query";exit();}
if (!OCIExecute($parsed_dispatched_reqs))
	{ echo "Error Executing Query";exit();}
if (!OCIExecute($parsed_latency_reqs))
	{ echo "Error Executing Query";exit();}
//fetch data into local tables
while (OCIFetch($parsed_inc_put_reqs)) {
	$new_incoming_put = OCIResult($parsed_inc_put_reqs,1);		
}
while (OCIFetch($parsed_space_reserve_reqs)) {
	$new_space_reserve = OCIResult($parsed_space_reserve_reqs,1);		
}
while (OCIFetch($parsed_done_reqs)) {
	$new_done_put = OCIResult($parsed_done_reqs,1);		
}
while (OCIFetch($parsed_dispatched_reqs)) {
	$dispatched = OCIResult($parsed_dispatched_reqs,1);
	$dispatched_avg_time = (float)OCIResult($parsed_dispatched_reqs,2);
}
while (OCIFetch($parsed_latency_reqs)) {
	$processed = OCIResult($parsed_latency_reqs,1);	
	$latency_avg = (float)OCIResult($parsed_latency_reqs,2);
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
echo "
  <body>
    <table bgcolor = 'lightyellow'>
      <tr>
       <td valign = top><table id ='fonts' bgcolor = 'lightyellow'>
		<tr>
			<td colspan = 4 align = left><b>Write Requests (Last Hour)</b></td>
		</tr>
		<tr>
			<td align = left><b>Write Requests</b></td>
			<td align = left><b>(#)</b></td>
			<td align = center><b>Avg. Queue Time (sec)</b></td>
			<td align = center><b>Avg. Latency (sec)</b></td>
		</tr>
		<tr>
			<td align = left><b>Incoming Write Req.:</b></td>
			<td align = left>$new_incoming_put</td>
			<td align = center>$dispatched_avg_time</td>
			<td align = center>$latency_avg</td>
		</tr>
		<tr>
			<td align = left><b>Reserve Space Req.:</b></td>
			<td align = left>$new_space_reserve</td>
			<td></td>
			<td></td>
		</tr>
		<tr>
			<td align = left><b>Write Done Req.:</b></td>
			<td align = left>$new_done_put</td>
			<td></td>
			<td></td>
		</tr>
		<tr>
			<td align = left><b>Dispatched:</b></td>
			<td align = left>$dispatched</td>
			<td></td>
			<td></td>
		</tr>
		<tr>
			<td align = left><b>Processed:</b></td>
			<td align = left>$processed</td>
			<td></td>
			<td></td>
		</tr>
		<tr>
			<td colspan = 4 align = left><b>Write Requests Timeseries (Last 4 Hours)</b></td>
		</tr>
		<tr>
		<tr>
			<td colspan = 4>
			<img src = 'exp_write_timeseries.php?service=$service'>
			</td>
		</tr>
	</table></td>
	<td valign = top>
	   <table cellspacing = 3 id='fonts' bgcolor = 'lightyellow'>
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
$i = 0;
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
	echo "<tr><td align = left onmouseover='doTooltip(event,\"user_write_timeseries.php?service=$service&euid=$userid\")' onmouseout='hideTip()'> $uname </td>";
        echo "<td align = left> $total_reqs</td>
	  <td align = left> $put</td>
	  <td align = left> $prep_put</td>
	  <td align = left> $put_done</td>
         </tr>";
	 $i++;
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
$i = 0;
while (OCIFetch($parsed_user_processed_reqs)) {
        $user_name = OCIResult($parsed_user_processed_reqs,1);
	$processed = OCIResult($parsed_user_processed_reqs,2);
	$put_req = OCIResult($parsed_user_processed_reqs,3);
	$p_put = OCIResult($parsed_user_processed_reqs,4);
	$done = OCIResult($parsed_user_processed_reqs,5);
	$imgstr = "\"user_proc_write_timeseries.php?service=$service&username=$user_name\""; 
	echo "<tr><td align = left onmouseover='doTooltip(event,$imgstr)' onmouseout='hideTip()'> $user_name </td>";
        echo "<td align = left> $processed</td>
	  <td align = left> $put_req</td>
	  <td align = left> $p_put</td>
	  <td align = left> $done</td>
         </tr>";
	 $i++;
}
echo "</table>
  </body>
</html>";
?>










