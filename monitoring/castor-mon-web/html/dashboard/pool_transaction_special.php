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
 //same as pool_transcation.php. This one checks the last ten minutes?>
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
function decode($transactions) {
	$i = 0;
	if ($transactions == 0)
		$colour = "white";
	else if($transactions < 10) 
		$colour = "lemonchiffon";
	else if (($transactions >= 10)&&($transactions < 100))
		$colour = "DarkKhaki";
	else if (($transactions >= 100)&&($transactions < 1000))
		$colour = "yellow";
	else if (($transactions >= 1000)&&($transactions < 10000))
		$colour = "orangered";
	else if ($transactions >= 10000)
		$colour = "red";
	return $colour;
}

//connection
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
$pool =array(-1 => 'foo');
$query1 = "select originalpool,targetpool,count(*)
	   from ".$db_instances[$service]['schema']."diskcopy
	   where timestamp >= sysdate - 15/1440
	   and timestamp < sysdate - 5/1440 
	   group by originalpool,targetpool
	   union
	   select svcclass,svcclass,copies from ".$db_instances[$service]['schema']."internaldiskcopy
	   where timestamp >= sysdate - 15/1440
	   and timestamp < sysdate - 5/1440 
	   order by originalpool";

if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$i =0;
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
	$trans[$opool][$tpool] = OCIResult($parsed1,3);
}
$size = $i;
for($i = 0; $i < $size;$i++) {
	for($j = 0; $j < $size;$j++) {
		if ($trans[$pool[$i]][$pool[$j]] == NULL) 
			$final[$i][$j] = 0;
		else $final[$i][$j] = $trans[$pool[$i]][$pool[$j]];
	
	}
}
		 

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
		<?php echo "<th id=\"fonts\"  align = center><a class ='outer' href='destsvcclass.php?svcclass=$p'> $p </a></th>"; ?>
		<?php }?>
		</tr>
		</thead>
		<tbody>
		<?php
		for($i = 0; $i < $size;$i++) {?>
		   <tr>
		   <?php echo "<th id=\"fonts\"  style=\"background-color: #C0C0C0\"><a class ='outer' href='srcsvcclass.php?svcclass=$pool[$i]'>$pool[$i]</a></th>";?>
		   <?php for($j = 0; $j < $size;$j++) {
		             $colour = decode($final[$i][$j]);
			     $value = $final[$i][$j];
			     echo "<td id ='fonts' align='center' style='background-color: $colour' ><a class='inner' style='background-color: $colour' href= 'pptran.php?src=$pool[$i]&dest=$pool[$j]'>$value</a></td>";?>
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

