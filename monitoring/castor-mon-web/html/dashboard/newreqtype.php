<?php 
//This script displays the number of New Incoming Requests per Request Type
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
//get posted values
$type = $_GET['type'];
$pattern_1 = '/[a-zA-Z0-9]{1,30}/';
preg_match($pattern_1,$type,$match);
$type = $match[0];
if($type == NULL) $type = 'StageGetRequest';
$service = $_GET['service'];

echo "<table><tr><td align ='center' style='background-color: orangered'><b>Incoming Request: $type (Last Ten Minutes)</b></td></tr> <tr><td align = 'center'><img src ='tsnewreq.php?service=$service&period=$period&type=$type'></td></tr>
</table>";
?>