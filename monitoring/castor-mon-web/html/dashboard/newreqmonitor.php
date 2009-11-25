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
include ("../../../conf/castor-mon-web/user.php");
//connection - DB login
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}

$query1 = "select requesttype request, sum(requests) total
	     from ".$db_instances[$service]['schema'].".requeststats
	    where timestamp >= sysdate - 15/1440
	    and timestamp < sysdate - 5/1440
	    and euid = '-'
            group by requesttype
	    order by total desc";

if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$i = 0;
$pool = array();
//fetch data in local tables
while (OCIFetch($parsed1)) {
	$request[$i] = OCIResult($parsed1,1);
	$total[$i] = OCIResult($parsed1,2);
	$i++;
}
//display table 
echo "<table border = 1>
				<tbody>
					<tr style='background-color: #C0C0C0'>
					 <td id = 'fonts' align = 'center'> Request Type </td> 
					 <td id = 'fonts' align = 'center'> Total Requests </td>
					</tr>";
$top = 5;
$overalltotal = 0;
for ($j = 0; $j< $i; $j++) {
   $overalltotal += $total[$j];
}
$remainingtotal = $overalltotal;
for($j = 0; $j< $top; $j++) {
	$remainingtotal -= $total[$j];
	echo "<tr><td id ='fonts' align ='center' style='background-color: #C0C0C0'><a class='outer' href='newreqtype.php?service=$service&type=$request[$j]'> $request[$j] </a></td>";
	echo "<td id ='fonts' align ='center' style='background-color: white'> $total[$j] </td>";
}
echo "<tr style='background-color: #C0C0C0'>
      <td id = 'fonts' align = 'center'> Overall Total  </td>
      <td id = 'fonts' align = 'center'> Remaining Total </td>
      </tr> 
      <tr style='background-color: #C0C0C0'>
      <td id ='fonts' align ='center' style='background-color: white'> $overalltotal </td>
      <td id ='fonts' align ='center' style='background-color: white'> $remainingtotal </td>
      </tr>
      </tbody></table>";
?>
