<?php
//Include necessary libraries
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
$new_incoming_reqs = "select nvl(sum(requests),0) reqs
                     from ".$db_instances[$service]['schema'].".requeststats
                     where timestamp >= sysdate - 1/24
		       and timestamp < sysdate
		       and euid = '-'
		       and requesttype in ('StagePrepareToGetRequest','StageGetRequest')";
$dispatched_reqs = "select nvl(sum(dispatched),0) dispatched, round(nvl(avg(avgqueuetime),0),3) avg_queue_time
                    from ".$db_instances[$service]['schema'].".queuetimestats
		    where timestamp >= sysdate - 1/24
		      and timestamp < sysdate
		      and requesttype = 'StageGetRequest'";
$latency_reqs = "select nvl(sum(started),0) reqs, round(nvl(avg(avglatencytime),0),3) avg_latency_time
                    from ".$db_instances[$service]['schema'].".latencystats
		    where timestamp >= sysdate - 1/24
		      and timestamp < sysdate
		      and requesttype = 'StageGetRequest'";

if (!($parsed_incoming_reqs = OCIParse($conn, $new_incoming_reqs))) 
	{ echo "Error Parsing Query"; exit();}
if (!($parsed_dispatched_reqs = OCIParse($conn, $dispatched_reqs))) 
	{ echo "Error Parsing Query"; exit();}
if (!($parsed_latency_reqs = OCIParse($conn, $latency_reqs))) 
	{ echo "Error Parsing Query"; exit();}
if (!OCIExecute($parsed_incoming_reqs))
	{ echo "Error Executing Query";exit();}
if (!OCIExecute($parsed_dispatched_reqs))
	{ echo "Error Executing Query";exit();}
if (!OCIExecute($parsed_latency_reqs))
	{ echo "Error Executing Query";exit();}
//fetch data into local tables
while (OCIFetch($parsed_incoming_reqs)) {
	$new_incoming = OCIResult($parsed_incoming_reqs,1);		
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
                              sum(case when requesttype = 'StageGetRequest' then requests else 0 end) get, 
                              sum(case when requesttype = 'StagePrepareToGetRequest' then requests else 0 end) prepare_get  
		       from ".$db_instances[$service]['schema'].".requeststats
                       where euid <> '-'
                         and requesttype in ('StagePrepareToGetRequest','StageGetRequest')
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
echo "
  <body>
    <table bgcolor = 'lightyellow'>
      <tr>
       <td valign = top><table id='fonts' bgcolor = 'lightyellow'>
		<tr>
		<td colspan = 4 align = left><b>Read Requests (Last Hour)</b></td>
		</tr>
		<tr>
		<td align = left><b>Read Requests</b></td>
		<td align = left><b>(#)</b></td>
		<td align = center><b>Avg. Queue Time (sec)</b></td>
		<td align = center><b>Avg. Latency (sec)</b></td>
		</tr>
		<tr>
		<td align = left><b>Incoming (Not Scheduled):</b></td>
		<td align = left>$new_incoming</td>
		<td align = center>$dispatched_avg_time</td>
		<td align = center>$latency_avg</td>
		</tr>
		<tr>
		<td align = left><b>Dispatched (Scheduled):</b></td>
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
		<td colspan = 4 align = left><b>Read Requests Timeseries (Last 4 Hours)</b></td>
		</tr>
		<tr>
		<td colspan = 4>
		<img src = 'exp_read_timeseries.php?service=$service'>
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
	          <td align = left><b>StageGetRequest</b></td>
	          <td colspan = 2 align = left><b>StagePrepareToGet</b></td>
                 </tr>";
$i = 0;
while (OCIFetch($parsed_user_incoming_reqs)) {
	$userid = OCIResult($parsed_user_incoming_reqs,1);
	$username = posix_getpwuid((int)$userid);
	if(!empty($username))
	  	$uname = $username['name'];
	else $uname = $userid;
	$total_reqs = OCIResult($parsed_user_incoming_reqs,2);
	$dir_get = OCIResult($parsed_user_incoming_reqs,3);
	$prep_get = OCIResult($parsed_user_incoming_reqs,4);
	echo "<tr><td align = left onmouseover='doTooltip(event,\"user_read_timeseries.php?service=$service\&euid=$userid\")' onmouseout='hideTip()'> $uname </td>";
        echo "<td align = left> $total_reqs</td>
	  <td align = left> $dir_get</td>
	  <td colspan = 2 align = left> $prep_get</td>
         </tr>";
	 $i++;
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
$i = 0;
while (OCIFetch($parsed_user_processed_reqs)) {
        $user_name = OCIResult($parsed_user_processed_reqs,1);
	$processed = OCIResult($parsed_user_processed_reqs,2);
	$dh = OCIResult($parsed_user_processed_reqs,3);
	$dc = OCIResult($parsed_user_processed_reqs,4);
	$tr = OCIResult($parsed_user_processed_reqs,5);
	$imgstr = "\"user_proc_read_timeseries.php?service=$service&username=$user_name\"";
	echo "<tr><td align = left onmouseover='doTooltip(event,$imgstr)' onmouseout='hideTip()'> $user_name </td>";
        echo "<td align = left> $processed</td>
	  <td align = left> $dh</td>
	  <td align = left> $dc</td>
	  <td align = left> $tr</td>
         </tr>";
	 $i++;
}
echo "</table>
       </td>
      </tr>
    </table>
  </body>
</html>";
?>










