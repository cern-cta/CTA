<?php 
//Migration Monitor(CASTOR DashBoard)?>
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
//include user account
include("../../../conf/castor-mon-web/user.php");
//connection - DB login
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}

$query1 = "select * from ".$db_instances[$service]['schema']." MV_MigMonitor";
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$i = 0;
$pool = array();
//fetch data in local tables
while (OCIFetch($parsed1)) {
	$pool[$i] = OCIResult($parsed1,1);
	$migs[$i] = OCIResult($parsed1,2);
	$i++;
}
//display table 
echo "<table border = 1>
				<tbody>
					<tr style='background-color: #C0C0C0'>
					 <td id = 'fonts' align = 'center'> SvcClass </td> 
					 <td id = 'fonts' align = 'center'> Files Migrated </td>
					</tr>";
for($j = 0; $j< $i; $j++) {
	echo "<tr><td id ='fonts' align ='center' style='background-color: #C0C0C0'><a class='outer' href='migpool.php?service=$service&svcclass=$pool[$j]'> $pool[$j] </a></td>";
	echo "<td id ='fonts' align ='center' style='background-color: white'> $migs[$j] </td>";
}
echo "</tbody></table>";
?>
