<?php 
//This script creates the GC monitor for the DashBoard Page
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
//Include user account
include("../../../conf/castor-mon-web/user.php");
//connection
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
$pool = array(-1=>"foo");
$query1 = "select * from ".$db_instances[$service]['schema'].".GcMonitor_MV";
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
//Fetch Data and simultaneously create GC monitor table
echo "<table border = '1'>
				<tbody>
					<tr>
					 <td style='background-color: #C0C0C0' id = 'fonts' align = 'center' ><a class='outer' href='gcts.php?service=$service'> # GC Files</a></td>
					 <td style='background-color: #C0C0C0' id = 'fonts' align = 'center' colspan=2><a class='outer' href='gcagets.php?service=$service'> Average Age (sec) </a></td>
					 <td style='background-color: #C0C0C0' id = 'fonts' align = 'center' colspan=2><a class='outer' href='gcsizets.php?service=$service'>  Average Size (MB) </a></td>
					</tr><tr>";
while (OCIFetch($parsed1)) {
	$gcfiles = OCIResult($parsed1,1);
	$age = (float)OCIResult($parsed1,2);
	$age_per = (float)OCIResult($parsed1,3);
	$size = (float)OCIResult($parsed1,4);
	$size_per = (float)OCIResult($parsed1,5);
	if ($gcfiles != NULL) {
			echo "<td id ='fonts' align ='center'> $gcfiles</td>";
	}
	else "<td id ='fonts' align ='center'> 0 </td>";
	if ($age != NULL) {
		if($age_per > 0)
			echo "<td><img src='../images/greenarrow.jpg' ></td><td id ='fonts' align ='center'> $age</td>";
		else if ($age_per < 0)
		  echo "<td><img src='../images/redarrow.jpg' ></td><td id ='fonts' align ='center'> $age</td>";
		else
			echo "<td><b>-</b></td><td id ='fonts' align ='center'> $age</td>";
	}
	else echo "<td id ='fonts' align ='center' colspan=2> 0 </td>";
	if ($size != NULL) {
		if($size_per > 0)
			echo "<td><img src='../images/greenarrow.jpg' ></td><td id ='fonts' align ='center'> $size</td>";
		else if ($size_per < 0)
		  echo "<td><img src='../images/redarrow.jpg' ></td><td id ='fonts' align ='center'> $size</td>";
		else
			echo "<td><b>-</b></td><td id ='fonts' align ='center'> $size</td>";
	}
	else echo "<td id ='fonts' align ='center'colspan=2> 0 </td>";
}
echo "</tr></tbody></table>";
?>
