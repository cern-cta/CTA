<?php
/******************************************************************************
 *              pool_transaction.php
 *
 * This file is part of the Castor Monitoring project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2008  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * Author Theodoros Rekatsinas, 
 *****************************************************************************/
 //Service Class to Service Class file copies (Internal + External Copies)?>
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
//This function matches a number with a colour. The bigger the number the more intense the colour 
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
}

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
	$query1 = "select originalpool,targetpool,count(*)
		   from ".$db_instances[$service]['schema'].".diskcopy
		   where timestamp > sysdate - :period
		   group by originalpool,targetpool
		   union
		   select svcclass,svcclass,copies from ".$db_instances[$service]['schema'].".internaldiskcopy
		   where timestamp > sysdate - :period
		   order by originalpool";
else if ($qn ==2)
	$query1 = "select originalpool,targetpool,count(*)
		   from ".$db_instances[$service]['schema'].".diskcopy
		   where timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
			and timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
		   group by originalpool,targetpool
		   union
		   select svcclass,svcclass,copies from ".$db_instances[$service]['schema'].".internaldiskcopy
		   where timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
			and timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
		   order by originalpool";

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
	$opool = OCIResult($parsed1,1);
	$tpool = OCIResult($parsed1,2);
	if (!(in_array($opool,$pool))) {
		$pool[$i] = $opool;
		$i++;
	};
	if (!(in_array($tpool,$pool))) {
		$pool[$i] = $tpool;
		$i++;
	};
	$total = OCIResult($parsed1,3);
	if ($min == NULL) $min = $total;
	if ($total > $max) $max = $total;
	if ($total < $min) $min = $total;
	$trans[$opool][$tpool] = OCIResult($parsed1,3);
}
//process retrieved data
$size = $i;
for($i = 0; $i < $size;$i++) {
	for($j = 0; $j < $size;$j++) {
		if ($trans[$pool[$i]][$pool[$j]] == NULL) 
			$final[$i][$j] = 0;
		else $final[$i][$j] = $trans[$pool[$i]][$pool[$j]];
	
	}
}
		 
//display table
?>
<table>
	<tbody>
	<tr><td aling = left>
		<table border = '1'>
		<thead>
		<tr style="background-color: #C0C0C0">
		<th id="fonts" align = center>FROM - TO</a></th>
		<?php
		  for($k=0;$k < $size; $k++) {
		  $p = $pool[$k];
		?>
		<th id="fonts"  align = center><?php echo $p;?></th>
		<?php }?>
		</tr>
		</thead>
		<tbody>
		<?php
		for($i = 0; $i < $size;$i++) {?>
		   <tr>
		    <th id="fonts"  style="background-color: #C0C0C0"><?php echo $pool[$i]?></th>
		   <?php for($j = 0; $j < $size;$j++) {
		             $colour = decode($final[$i][$j],$max,$min);
			     $value = $final[$i][$j];
			     echo "<td id ='fonts' align='center' style='background-color: $colour' >$value</td>";?>
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

