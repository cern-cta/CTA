<?php 
//Pool DashBoard Web Page
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
//includes user account
include("../../../conf/castor-mon-web/user.php");
//get posted values
if($period == NULL) $period = 0;
$svcclass = $_GET['svcclass'];
$pattern_1 = '/[a-zA-Z0-9]{1,15}/';
preg_match($pattern_1,$svcclass,$match);
$svcclass = $match[0];
if($svcclass == NULL) $svcclass = 'default';
$service = $_GET['service'];
//DB -Login
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
$query1 = "select * from (
			select username , count(*) total,count(case when state='DiskHit' then 1 else null end) dh, 
			count(case when state='DiskCopy' then 1 else null end) dc,
			count(case when state='TapeRecall' then 1 else null end) tr
			from ".$db_instances[$service]['schema'].".requests
			where timestamp >= sysdate - 15/1440
				and timestamp < sysdate - 5/1440 
				and svcclass = :svcclass
			group by username
			order by total desc,username)
			where rownum < 11";
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
ocibindbyname($parsed1,":svcclass",$svcclass);
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$i = 0;
//fetch data into local tables
while (OCIFetch($parsed1)) {
	$user[$i][0] = OCIResult($parsed1,1);
	$user[$i][1] = OCIResult($parsed1,2);
	$user[$i][2] = OCIResult($parsed1,3);
	$user[$i][3] = OCIResult($parsed1,4);
	$user[$i][4] = OCIResult($parsed1,5);
	$i++;
}
//Display Tables-Results
echo "<table><tr><td colspan =2 align = 'center' style='background-color: orangered'><b> Read File Requests History of last 10 min in $svcclass</b></td></tr>";
echo "<tr><td align = 'center'><img src ='pie_pool.php?service=$service&svcclass=$svcclass'></td>";
echo "<td align = 'center'><img src ='tspool.php?service=$service&svcclass=$svcclass'></td></tr>";
echo "<tr><td colspan =2 align = 'center' style='background-color: orange'> File Size Distribution of tape recalled files(last 10 minutes)</td></tr>"; 
echo "<tr><td colspan =2 align = 'center'><img src ='sizedistr_svc.php?service=$service&svcclass=$svcclass'></td></tr>";
echo "<tr><td colspan =2 align = 'center' style='background-color: orange'>Read File Requests Counter per User</td></tr>"; 
echo "<tr><td colspan =2 align ='center'><table border = 1><tbody>
		<tr>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> Username </td>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> Total Reqs </td>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> DiskHits </td>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> DiskCopies </td>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> TapeRecalls </td></tr>";
foreach ($user as $u) {
	echo "</tr><td id = 'fonts' align = 'center'style='background-color: #C0C0C0'><a href='userhist.php?service=$service&user=$u[0]'> $u[0] </a></td>";
	echo "<td id = 'fonts' align = 'center'>$u[1]</td>";
	echo "<td id = 'fonts' align = 'center'>$u[2]</td>";
	echo "<td id = 'fonts' align = 'center'>$u[3]</td>";
	echo "<td id = 'fonts' align = 'center'>$u[4]</td></tr>";
}

echo "</tbody></table></td></tr></table>";
?>
