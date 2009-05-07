<?php 
//this script displays the number of Migrated Files per serviceclass
error_reporting(E_ALL ^ E_NOTICE);
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
//include user account
include("../../../conf/castor-mon-web/user.php");
//get posted values
$svcclass = $_GET['svcclass'];
$pattern_1 = '/[a-zA-Z0-9]{1,15}/';
preg_match($pattern_1,$svcclass,$match);
$svcclass = $match[0];
if($svcclass == NULL) $svcclass = 'default';
$service = $_GET['service'];
//connection - DB login 
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}

$query1 = "select * from (
			select username , count(*) total
			from ".$db_instances[$service]['schema'].".migration
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
//fetch data in local tables 
$i = 0;
while (OCIFetch($parsed1)) {
	$user[$i][0] = OCIResult($parsed1,1);
	$user[$i][1] = OCIResult($parsed1,2);
	$i++;
}
//display table at the same time
echo "<table><tr><td align ='center' style='background-color: orangered'><b>Last Ten Minutes History (Migrated Files - $svcclass)</b></td></tr>";
echo "<tr><td align = 'center'><img src ='tsmpool.php?service=$service&svcclass=$svcclass'></td></tr>";
if (empty($user)) {
	 echo "<tr><td align = 'center'><b> No Data Available</b></td></tr>";
}
else {
echo "<tr><td align = 'center'><table border = 1><tbody>
		<tr>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> Username </td>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> Total Migrated Files </td></tr>";
foreach ($user as $u) {
	echo "</tr><td id = 'fonts' align = 'center'style='background-color: #C0C0C0'><a href='userhistm.php?service=$service&user=$u[0]'> $u[0] </a></td>";
	echo "<td id = 'fonts' align = 'center'>$u[1]</td></tr>";
}
}

echo "</tbody></table></td></tr></table>";
?>
