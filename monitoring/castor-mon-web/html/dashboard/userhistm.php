<?php 
// user historical review page (DashBoard)?>
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
//user account
include("../../../conf/castor-mon-web/user.php");
$username = $_GET['user'];
$pattern_1 = '/[a-zA-Z0-9]{1,15}/';
preg_match($pattern_1,$username,$match);
$username = $match[0];
$service = $_GET['service'];
//connection - db login
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
$query1 = " select svcclass, count(1) total
			from ".$db_instances[$service]['schema'].".migration
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
//fetch data into local tables
while (OCIFetch($parsed1)) {
	$hist[$i][0] = OCIResult($parsed1,1);
	$hist[$i][1] = OCIResult($parsed1,2);
	$i++;
}
echo "<table><tr><td align = 'center' style='background-color:orangered'><b> Last Ten minutes Migrated Files(User: $username)</b></td></tr>";
echo "<tr><td align = 'center'><img src ='tsmuser.php?service=$service&user=$username'></td></tr>";
echo "<tr><td align = 'center'><table border = 1><tbody>
		<tr>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> SvcClass </td>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> Total Migrated Files </td></tr>";
foreach ($hist as $h) {
	echo "</tr><td id = 'fonts' align = 'center'style='background-color: #C0C0C0'> $h[0] </a></td>";
	echo "<td id = 'fonts' align = 'center'>$h[1]</td></tr>";
}
echo "</tbody></table></td></tr></table>";
?>
