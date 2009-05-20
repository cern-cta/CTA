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
//checks if included dynamic library needed for the Oracle db
include("../../../conf/castor-mon-web/user.php");
$username = $_GET['user'];
$pattern_1 = '/[a-zA-Z0-9]{1,15}/';
preg_match($pattern_1,$username,$match);
$username = $match[0];
$service = $_GET['service'];
//connection
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
$query1 = " select svcclass, count(1) total,count(case when state='DiskHit' then 1 else null end) dh, 
			count(case when state='DiskCopy' then 1 else null end) dc,
			count(case when state='TapeRecall' then 1 else null end) tr,
			count(case when (state='TapeRecall' and type='StagePrepareToGetRequest') then 1 else null end) pretr,
			count(case when (state='TapeRecall' and type='StageGetRequest') then 1 else null end) immtr
			from ".$db_instances[$service]['schema'].".requests
			where timestamp >= sysdate - 15/1440
				and timestamp < sysdate - 5/1440 
				and username = :username
			group by svcclass
			order by total desc";
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
ocibindbyname($parsed1,":username",$username);
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$i = 0;
while (OCIFetch($parsed1)) {
	$hist[$i][0] = OCIResult($parsed1,1);
	$hist[$i][1] = OCIResult($parsed1,2);
	$hist[$i][2] = OCIResult($parsed1,3);
	$hist[$i][3] = OCIResult($parsed1,4);
	$hist[$i][4] = OCIResult($parsed1,5);
	$hist[$i][5] = OCIResult($parsed1,6);
	$hist[$i][6] = OCIResult($parsed1,7);
	$i++;
}
if(empty($hist)) {
	echo "<b> No Data Available for $username</b>";
	exit();
}
echo "<table><tr><td colspan =2 align = 'center' style='background-color: orangered'> $username last 10 minutes read request history </td></tr>";
echo "<tr><td align = 'center'><img src ='pie_user.php?service=$service&user=$username'></td>";
echo "<td align = 'center'><img src ='tsuser.php?service=$service&user=$username'></td></tr>";
echo "<tr><td colspan = 2 align = 'center' style='background-color: orange'> File Size Distribution - Files Requested By $username</td></tr>";
echo "<tr><td colspan =2 align = 'center'><img src ='sizedistr_user.php?service=$service&user=$username'></td></tr>";
echo "<tr><td colspan = 2 align = 'center' style='background-color: orange'> Files Requested(Count) By $username</td></tr>";
echo "<tr><td align ='center'><table border = 1><tbody>
		<tr>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> SvcClass </td>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> Total Reqs </td>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> DiskHits </td>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> DiskCopies </td>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> TapeRecalls </td>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> PreStaged TRs </td>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> Immediate TRs </td></tr>";
foreach ($hist as $h) {
	echo "</tr><td id = 'fonts' align = 'center'style='background-color: #C0C0C0'> $h[0]</td>";
	echo "<td id = 'fonts' align = 'center'>$h[1]</td>";
	echo "<td id = 'fonts' align = 'center'>$h[2]</td>";
	echo "<td id = 'fonts' align = 'center'>$h[3]</td>";
	echo "<td id = 'fonts' align = 'center'>$h[4]</td>";
	echo "<td id = 'fonts' align = 'center'>$h[5]</td>";
	echo "<td id = 'fonts' align = 'center'>$h[6]</td></tr>";
}

echo "</tbody></table></td></tr></table>";
?>
