<?php 
/*Plots a detailed errors report 
 * for every Service Class
 * Plots the result by calling errormes.php
 */
/*Deactivate Notices*/
error_reporting(E_ALL ^ E_NOTICE);
/*include user account*/
include("../../../conf/castor-mon-web/user.php");
//checks if included dynamic library needed for the Oracle db
if (!function_exists('ociplogon')) {
	if (strtoupper(substr(PHP_OS, 0, 3) == 'WIN')) {
		dl('php_oci8.dll');
	}
}
//Get posted value of $period
//connect to the DB
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
//Select facility number , facility name
$query1 = "select fac_no,fac_name from castor_dlf.dlf_facilities";

if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$i = 0;
//Fetch Results in Local tables
while (OCIFetch($parsed1)) {
	$fac[$i] = OCIResult($parsed1,1);
	$fac_name[$i] = OCIResult($parsed1,2);
	$i++;
}
//No results return No Data Available Image
if(empty($fac)) {
	No_Data_Image();
	exit();
};
//For each one of the different facilities 
//Plot error distribution for the period given
echo "<table><tr> ";
for($j=0;$j<$i;$j++) {
   $f = $fac[$j];
   $n = $fac_name[$j];
   if ($period != NULL)
	echo "<tr><td align = 'center'><img src='errormes.php?service=$service&f=$f&period=$period&n=$n'></td></tr>";
   else
	echo "<tr><td align = 'center'><img src='errormes.php?service=$service&f=$f&from=$date1 $date1hour&to=$date2 $date2hour&n=$n'></td></tr>";
}
echo "</tr></table>";
?>