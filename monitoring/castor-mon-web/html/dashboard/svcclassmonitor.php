<?php
//dashboard service class monitor
//transactions between different service classes 
?>
<head>
	<style>
		#fonts {
			font: bold 12px/15px arial, helvetica, sans-serif;
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
//checks if included dynamic library needed for the Oracle db
include("../../../conf/castor-mon-web/user.php");
//connection
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
$pool[-1] = 'foo';
$query1 = "select * from ".$db_instances[$service]['schema']." MV_MainTableCounters";
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$pre_pool = 'foo';
$i = 0;
while (OCIFetch($parsed1)) {
	$pool = OCIResult($parsed1,1);
	if($pool != $pre_pool) {
		$pool_names[$i] = $pool;
		$pre_pool = $pool;
		$i++;
	}
	$state = OCIResult($parsed1,2);
	if ($state == 'DiskHit') {
		$diskhits[$pool] = OCIResult($parsed1,3);
		$diskper[$pool] = (float)OCIResult($parsed1,4);
	}
	else if ($state == 'DiskCopy') {
		$diskcopies[$pool] = OCIResult($parsed1,3);
		$copyper[$pool] = (float)OCIResult($parsed1,4);
	}	
	else if ($state == 'TapeRecall'){
		$taperecalls[$pool] = OCIResult($parsed1,3);
		$tapeper[$pool] = (float)OCIResult($parsed1,4);
	}
}
if(empty($pool_names)) {
	echo "<b> No Data Available</b>";
}
else {
echo "<table border = 1>
				<tbody>
					<tr style='background-color: #C0C0C0'>
					 <td id = 'fonts' align = 'center'> SvcClass </td> 
					 <td id = 'fonts' align = 'center'><a class='outer' href='reqkind.php?service=$service&reqkind=DiskHit'> DiskHits (%) </a></td>
					 <td id = 'fonts' align = 'center'><a class='outer' href='reqkind.php?service=$service&reqkind=DiskCopy'> DiskCopies (%) </a></td>
					 <td id = 'fonts' align = 'center'><a class='outer' href='reqkind.php?service=$service&reqkind=TapeRecall'> TapeRecalls (%) </a></td>
					</tr>";
foreach ($pool_names as $pn) {
	echo "<tr><td id ='fonts' align ='center' style='background-color: #C0C0C0'><a class='outer' href='pool.php?service=$service&svcclass=$pn'> $pn </a></td>";
	if ( $diskhits[$pn] != NULL)
		$dh = $diskhits[$pn];
	else $dh = 0;
	if ( $diskcopies[$pn] != NULL)
		$dc = $diskcopies[$pn];
	else $dc = 0;
	if ($taperecalls[$pn] != NULL)
		$tr = $taperecalls[$pn];
	else $tr = 0;
	$dhper = round($dh/($dh+$dc+$tr),2);
	$dcper = round($dc/($dh+$dc+$tr),2);
	$trper = round($tr/($dh+$dc+$tr),2);
	if($dhper < 0.5)
		echo "<td id ='fonts' align ='center' style='background-color: red'><a class='inner' style='background-color: red' href='reqpool.php?service=$service&svcclass=$pn&reqkind=DiskHit'> $dh ($dhper) </a></td>";
	else if (($dhper >= 0.5)&&($dhper <0.7))
		echo "<td id ='fonts' align ='center' style='background-color: orange'><a class='inner' style='background-color: orange' href='reqpool.php?service=$service&svcclass=$pn&reqkind=DiskHit'> $dh ($dhper) </a></td>";
	else
		echo "<td id ='fonts' align ='center' style='background-color: green'><a class='inner' style='background-color: green' href='reqpool.php?service=$service&svcclass=$pn&reqkind=DiskHit'> $dh ($dhper) </a></td>";
	if($dcper > 0.2)
		echo "<td id ='fonts' align ='center' style='background-color: red'><a class='inner' style='background-color: red' href='reqpool.php?service=$service&svcclass=$pn&reqkind=DiskCopy'> $dc ($dcper)</a></td>";
	else if (($dcper >= 0.15)&&($dcper <=0.2))
		echo "<td id ='fonts' align ='center' style='background-color: orange'><a class='inner' style='background-color: orange' href='reqpool.php?service=$service&svcclass=$pn&reqkind=DiskCopy'> $dc ($dcper)</a></td>";
	else
		echo "<td id ='fonts' align ='center' style='background-color: green'><a class='inner' style='background-color: green' href='reqpool.php?service=$service&svcclass=$pn&reqkind=DiskCopy'> $dc ($dcper) </a></td>";
	if($trper > 0.1)
		echo "<td id ='fonts' align ='center' style='background-color: red'><a class='inner' style='background-color: red' href='reqpool.php?service=$service&svcclass=$pn&reqkind=TapeRecall'> $tr ($trper)</a></td>";
	else if (($trper >= 0.08)&&($trper <=0.1))
		echo "<td id ='fonts' align ='center' style='background-color: orange'><a class='inner' style='background-color: orange' href='reqpool.php?service=$service&svcclass=$pn&reqkind=TapeRecall'> $tr ($trper)</a></td>";
	else
		echo "<td id ='fonts' align ='center' style='background-color: green'><a class='inner' style='background-color: green' href='reqpool.php?service=$service&svcclass=$pn&reqkind=TapeRecall'> $tr ($trper)</a></td>";
	echo "</tr>";
	$i++;
}
echo "</tbody></table>";
}
?>
