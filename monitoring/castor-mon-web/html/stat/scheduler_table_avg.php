<head>
	<style>
		#fonts {
			font: bold 12px/15px arial, helvetica, sans-serif;
		}
	</style>
</head>
<body>
<?php
if (!function_exists(decode)) {
function decode($total,$max,$min) {
	$bin = ($max - $min )/ 6;
	if ($total < $min + $bin)
		$colour = "azure";
	else if(($total >= $min + $bin)&&($total < $min + 2*$bin)) 
		$colour = "lemonchiffon";
	else if (($total >=$min + 2*$bin)&&($total < $min + 3*$bin)) 
		$colour = "yellow";
	else if (($total >=$min + 3*$bin)&&($total <$min + 4*$bin)) 
		$colour = "gold";
	else if (($total >=$min + 4*$bin)&&($total <$min + 5*$bin)) 
		$colour = "orangered";
	else if ($total >$min + 5*$bin)
		$colour = "red";
	return $colour;
}}

//checks if included dynamic library needed for the Oracle db
if (!function_exists('ociplogon')) {
	if (strtoupper(substr(PHP_OS, 0, 3) == 'WIN')) {
		dl('php_oci8.dll');
	}
}
//include user account
include("../../../conf/castor-mon-web/user.php");
//connection - db login
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
if ($timewindow != NULL) { 
	$qn = 1;
}	
else { 
	$qn = 2;
}
$pool =array(-1 => 'foo');
if ($timewindow == "10/1440")
	$period = 10/1440;
else if ($timewindow == "1/24")
	$period = 1/24;
else if ($timewindow == "1")
	$period = 1;
else if ($timewindow == "7")
	$period = 7;
else if ($timewindow == "30")
	$period = 30;
if ($qn == 1) 
	$query1 = "select svcclass, requesttype, avg(avgqueuetime)
		   from ".$db_instances[$service]['schema'].".queuetimestats
		   where timestamp > sysdate - :period
		   group by svcclass, requesttype";
else if ($qn ==2)
	$query1 = "select svcclass, requesttype, avg(avgqueuetime)
		   from ".$db_instances[$service]['schema'].".queuetimestats
		   where timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
			and timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
		   group by svcclass, requesttype";

if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if ($qn == 1) {
	ocibindbyname($parsed1,":period",$period);
}
else if ($qn == 2) {
	ocibindbyname($parsed1,":from_date",$from);
	ocibindbyname($parsed1,":to_date",$to);
}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$i =0;
$max = -1;
$min = NULL;
//fetch data into local tables
while(OCIFetch($parsed1)) {
	$svcclass = OCIResult($parsed1,1);
	$type = OCIResult($parsed1,2);
	$avgtime = round((float)OCIResult($parsed1,3),3); 
	if ($min == NULL) $min = $avgtime;
	if ($avgtime >= $max) $max = $avgtime;
	if ($avgtime <= $min) $min = $avgtime;
	$svcc[$svcclass] = $svcclass;if ($avgtime >= $max) $max = $avgtime;
	$t[$type] = $type;
	$result[$svcclass][$type]['avg'] = $avgtime;
}
		 
//display table
?>
<table>
	<tbody>
	<tr><td aling = left>
		<table border = '1'>
		<thead>
		<tr style="background-color: #C0C0C0">
		<th id="fonts" align = center>TYPE - SVCCLASS</a></th>
		<?php
		  foreach($svcc as $svcclass) {
		?>
		<th id="fonts"  align = center><?php echo $svcclass;?></th>
		<?php }?>
		</tr>
		</thead>
		<tbody>
		<?php
		foreach($t as $type) { ?>
		  <tr>
		    <th id="fonts"  style="background-color: #C0C0C0"><?php echo $type?></th>
		  <?php foreach($svcc as $svcclass          ) {
		        $temp = $result[$svcclass][$type]['avg'];
		       	if ($temp == NULL) $temp = 0;
			$colour = decode($temp,$max,$min);
			echo "<td id ='fonts' align='center' style='background-color: $colour'>$temp";
		       ?></td>
		   <?php } ?>
		   </tr><?php }?>
		</tbody>
		</table>
	</td>
	</tr>
	</tbody>
</table>
</body>
</html>

