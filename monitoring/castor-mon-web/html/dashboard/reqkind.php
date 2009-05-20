<?php
 //DashBoard web page
 //input: Request kind
 //output: Historical Data(last 10 minutes) 
?> 
<head>
	<style>
		#fonts {
			font: bold 12px/15px arial, helvetica, sans-serif;
		}
		
		td a {
			color: #000;
			text-decoration: none;
			background: #C0C0C0;
		}
		
		td a:hover {
			color: #a00;
			background: #C0C0C0;
		}
	</style>
</head>
<body>
<?php
//checks if included dynamic library needed for the Oracle db
if (!function_exists('ociplogon')) {
	if (strtoupper(substr(PHP_OS, 0, 3) == 'WIN')) {
		dl('php_oci8.dll');
	}
}
//get posted values
include("../../../conf/castor-mon-web/user.php");
$reqkind = $_GET['reqkind'];
$pattern_1 = '/[a-zA-Z0-9]{1,15}/';
preg_match($pattern_1,$reqkind,$match);
$reqkind = $match[0];
$service = $_GET['service'];
//connection
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
echo "<div style='background-color: orangered' align='center'><b> $reqkind - Last 10 Minutes History</b> </div>";
//If selected kind is TapeRecalls (extra data about prestagind retrieved)
if ($reqkind == 'TapeRecall') {
$query1 = "select svcclass,username, count(*) r, count(case when type='StageGetRequest' then 1 else null end) non,
		   count(case when type='StagePrepareToGetRequest' then 1 else null end) pre
           from ".$db_instances[$service]['schema'].".requests
           where timestamp >= sysdate - 15/1440
           and timestamp < sysdate - 5/1440  
           and state = 'TapeRecall'
           group by svcclass,username
           order by svcclass, r desc";
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$pre_pool = 'foo';
$j = 0;
$count = 0;
$i = 0;
//fetch data into local tables
while (OCIFetch($parsed1)) {
	$pool = OCIResult($parsed1,1);
	$u = OCIResult($parsed1,2);
	$uc = OCIResult($parsed1,3);
	$no_prestage = OCIResult($parsed1,4);
	$prestage = OCIResult($parsed1,5);
	if($pool != $pre_pool) {
		$pool_names[$j] = $pool;
		$reqs[$pre_pool] = $count;
		$count = 0;
		$pre_pool = $pool;
		$j++;
	}
	$count += $uc;
	$user[$i][0] = $u;
	$user[$i][1] = $pool;
	$user[$i][2] = $uc;
	$user[$i][3] = $no_prestage;
	$user[$i][4] = $prestage;
	$i++;
}
// create display table
$reqs[$pre_pool] = $count;
if(empty($pool_names))
	echo "<b>No_Data_Available</b>";
else {
echo "<div><img src ='timeseriesreqkind.php?service=$service&reqkind=$reqkind'></div>";
echo "<div style='background-color: orange' align = 'center'><b> File Size of recalled files within the last 10 Minutes</b> </div>";
echo "<div><img src ='trsdistr.php?service=$service'></div>";
$span = count($pool_names) + 1;
echo "<div><table border = 1><tbody>
		<tr>
			<td colspan = $span align = 'center' style='background-color: orange'> Number of Tape Recalled Files per SvcClass</td>
		</tr>
		<tr>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> SvcClass </td>";
foreach ($pool_names as $pn) {
	echo "<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'><a href='reqpool.php?service=$service&svcclass=$pn&reqkind=$reqkind'> $pn </a></td>";
}
echo  "</tr><tr><td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> $reqkind </td>";
foreach ($pool_names as $pn) {
	echo "<td id ='fonts' align ='center'>$reqs[$pn]</td>";
}

echo "</tr></tbody></table></div>";
$span = count($user) + 1;
//users
echo "<div><table border = 1><tbody>
		<tr>
			<td colspan = $span align = 'center' style='background-color: orange'> $reqkind counters per User/SvcClass</td>
		</tr>
		<tr>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> Username(SvcClass) </td>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> Total TapeRecalls </td>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> Immediate TRs </td>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> Prestaged TRs </td></tr>";
foreach ($user as $u) {
	echo "<tr><td id = 'fonts' align = 'center' style='background-color: #C0C0C0'><a href='userhist.php?service=$service&user=$u[0]'> $u[0]($u[1]) </a></td>";
	echo "<td id ='fonts' align ='center'>$u[2]</td>";
	echo "<td id ='fonts' align ='center'>$u[3]</td>";
	echo "<td id ='fonts' align ='center'>$u[4]</td></tr>";
}

echo "</tbody></table></div>";
}
}
else {
$query1 = "select svcclass,username, count(*) r
		   from ".$db_instances[$service]['schema'].".requests
		   where timestamp >= sysdate - 15/1440
		   and timestamp < sysdate - 5/1440 
		   and state = '$reqkind'
		   group by svcclass,username
		   order by svcclass, r desc";
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$pre_pool = 'foo';
$j = 0;
$count = 0;
$i = 0;
while (OCIFetch($parsed1)) {
	$pool = OCIResult($parsed1,1);
	$u = OCIResult($parsed1,2);
	$uc = OCIResult($parsed1,3);
	if($pool != $pre_pool) {
		$pool_names[$j] = $pool;
		$reqs[$pre_pool] = $count;
		$count = 0;
		$pre_pool = $pool;
		$j++;
	}
	$count += $uc;
	$user[$i][0] = $u;
	$user[$i][1] = $pool;
	$user[$i][2] = $uc;
	$i++;
}
$reqs[$pre_pool] = $count;
if(empty($pool_names))
	echo "<b>No_Data_Available</b>";
//If selected kind not TapeRecalls
else {
echo "<div><img src ='timeseriesreqkind.php?service=$service&reqkind=$reqkind'></div>";
$span = count($pool_names) + 1;
echo "<div><table border = 1><tbody>
		<tr>
			<td colspan = $span align = 'center' style='background-color: orange'> $reqkind-counters per SvcClass</td>
		</tr>
		<tr>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> SvcClass </td>";
foreach ($pool_names as $pn) {
	echo "<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'><a href='reqpool.php?service=$service&svcclass=$pn&reqkind=$reqkind'> $pn </a></td>";
}
echo  "</tr><tr><td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> $reqkind </td>";
foreach ($pool_names as $pn) {
	echo "<td id ='fonts' align ='center'>$reqs[$pn]</td>";
}

echo "</tr></tbody></table></div>";

//users
$span = count($user) + 1;
echo "<div><table border = 1><tbody>
		<tr>
			<td colspan = $span align = 'center' style='background-color: orange'> $reqkind counters per User/SvcClass</td>
		</tr>
		<tr>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> Username(SvcClass) </td>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> $reqkind </td></tr>";
foreach ($user as $u) {
	echo "<tr><td id = 'fonts' align = 'center' style='background-color: #C0C0C0'><a href='userhist.php?service=$service&user=$u[0]'> $u[0]($u[1]) </a></td>";
	echo "<td id ='fonts' align ='center'>$u[2]</td></tr>";
}

echo "</tbody></table></div>";
}
}
?>
