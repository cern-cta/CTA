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

//connection
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
$query1 = "select fac_name, msg_text, errorsum
	   from ".$db_instances[$service]['schema'].".Errors_MV
	   order by errorsum desc";

if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$i =0;
while(OCIFetch($parsed1)) {
	$fac_name[$i] = OCIResult($parsed1,1);
	$err_text[$i] = OCIResult($parsed1,2);
	$error_number[$i]    = OCIResult($parsed1,3);
	$i++;
}
$size = $i;
?>
<table>
	<tbody>
	<tr><td aling = left>
		<table border = '1'>
		<thead>
		<tr style="background-color: #C0C0C0">
		<th id="fonts" align = center>Facility</th>
		<th id="fonts" align = center>Error</th>
		<th id="fonts" align = center>Total Errors</th>
		</tr>
		</thead>
		<tbody>
		   <?php for($j = 0; $j < $size;$j++) {
			     $facility = $fac_name[$j];
			     $error = $err_text[$j];
			     $value = $error_number[$j];
			     echo "<tr><td id ='fonts' align='left' >$facility</td>";
			     echo "<td id ='fonts' align='left' >$error</td>";
			     echo "<td id ='fonts' align='left' >$value</td></tr>";?>
		   <?php } ?>
		</tbody>
		</table>
	</td>
	</tr>
	</tbody>
</table>
</body>
</html>

