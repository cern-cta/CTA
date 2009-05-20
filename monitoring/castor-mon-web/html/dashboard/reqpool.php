<?php
//Historical data for all kinds of ".$db_instances[$service]['schema']." requests per pool
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
//get posted variables
$svcclass = $_GET['svcclass'];
$pattern_1 = '/[a-zA-Z0-9]{1,15}/';
preg_match($pattern_1,$svcclass,$match);
$svcclass = $match[0];
if($svcclass == NULL) $svcclass = 'default';
$reqkind = $_GET['reqkind'];
preg_match($pattern_1,$reqkind,$match_1);
$reqkind = $match_1[0];
$service = $_GET['service'];

//connection
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}

$query1 = "select * from (
		   select username, count(*) r
		   from ".$db_instances[$service]['schema'].".requests
		   where timestamp >= sysdate - 15/1440
		   and timestamp < sysdate - 5/1440 
		   and state = :reqkind
		   and svcclass = :svcclass
		   group by username
		   order by r desc)
		   where rownum < 11";
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
ocibindbyname($parsed1,":reqkind",$reqkind);
ocibindbyname($parsed1,":svcclass",$svcclass);
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$i = 0;
//fetch data into local tables
while (OCIFetch($parsed1)) {
	$user[$i][0] = OCIResult($parsed1,1);
	$user[$i][1] = OCIResult($parsed1,2);
	$i++;
}
//Display table with counters
echo "<table><tr><td align ='center' style='background-color: orangered'><b> Last Ten minutes History ($reqkind - $svcclass)</b></td></tr>";
if (empty($user)) {
  echo "<tr><td>No Available Data</td></tr></table>";
  exit();
}
echo "<tr><td align = 'center'><img src ='timeseries_reqkind_svc.php?service=$service&reqkind=$reqkind&svcclass=$svcclass'></td></tr>";
echo "<tr><td align = 'center'><table border = 1><tbody>
		<tr>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> UserName($svcclass) </td>
			<td id = 'fonts' align = 'center' style='background-color: #C0C0C0'> $reqkind </td></tr>";
foreach ($user as $u) {
	echo "<tr><td id = 'fonts' align = 'center' style='background-color: #C0C0C0'><a href='userhist.php?service=$service&user=$u[0]'> $u[0]</a></td>";
	echo "<td id ='fonts' align ='center'>$u[1]</td></tr>";
}

echo "</tbody></table></td></tr></table>";

?>
