/******************************************************************************
 *              toptapesdet.php
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
<?php 
include ("user.php");
//checks if included dynamic library needed for the Oracle db
if (!function_exists('ociplogon')) {
	if (strtoupper(substr(PHP_OS, 0, 3) == 'WIN')) {
		dl('php_oci8.dll');
	}
}
//connection
$conn = ocilogon(username,pass,serv);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
// $period = $_GET['period'];
// if($period == NULL) $period = 10000;
$query1 = "select distinct svcclass
	   from requests
	   where state='TapeRecall'
	   and timestamp > sysdate - $period";

if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$i = 0;
while (OCIFetch($parsed1)) {
	$pool[$i] = OCIResult($parsed1,1);
	$i++;		
}
if(empty($pool)) {
	echo "No_Data";
}
else {?>
<table>
   <tr> 
   <?php foreach($pool as $p) {?>
   <tr><td align = "center"><?php echo "<img src=tapes.php?p=$p&period=$period>";?></td></tr>
   <?php } ?>
   </tr>
</table> <?php }?>

	
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	
