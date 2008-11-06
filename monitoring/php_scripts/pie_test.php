/******************************************************************************
 *              pie_test.php
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
include ("lib.php");
include ("../jpgraph-2.3.3/src/jpgraph.php");
include ("../jpgraph-2.3.3/src/jpgraph_pie.php");
include ("no_data.php");
include ("user.php");
$pid = $_GET['pid'];
if($pid == NULL) 
	$pid = 1;
//connection
$conn = ocilogon(username,pass,serv);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
$query1 = "SELECT filepath,count(*)
           FROM filesystem
           where level = 1
           and parent_id = $pid
           START WITH parent_id = $pid 
           CONNECT BY  prior  node_id = parent_id
	   group by filepath";

if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
while (OCIFetch($parsed1)) {
	$parent = OCIResult($parsed1,1);
	$number_files = OCIResult($parsed1,2);
}
	
$query2 = "SELECT name filename, num_accesses
           FROM filesystem
           where level = 1
           and parent_id = $pid
	   and rownum < 51
           START WITH parent_id = $pid 
           CONNECT BY  prior  node_id = parent_id
           order siblings by num_accesses desc";

if (!($parsed2 = OCIParse($conn, $query2))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed2))
	{ echo "Error Executing Query";exit();}
$i = 0;
while (OCIFetch($parsed2)) {
	$siblings[$i] = OCIResult($parsed2,1);
	$num_accesses[$i] = OCIResult($parsed2,2);
	$i++;
}
if ($number_files > 50) {
$query3 = "SELECT sum(num_accesses)
           FROM filesystem
           where level = 1
           and parent_id = $pid
	   and rownum < $number_files - 49
           START WITH parent_id = $pid 
           CONNECT BY  prior  node_id = parent_id
           order siblings by num_accesses";

if (!($parsed3 = OCIParse($conn, $query3))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed3))
	{ echo "Error Executing Query";exit();}
while (OCIFetch($parsed3)) {
	$num = $number_files - 30;
	$siblings[$i] = "Rest ".$num." files";
	$num_accesses[$i] = OCIResult($parsed3,1);
}
}
if(empty($siblings)) {
	No_Data_Image();
	exit();
};
$max = 0;
foreach ($siblings as $str) {
	$len = strlen($str);
	if($len > $max) $max = $len;
}
if ($max > 50)
	$graph  = new PieGraph (1500,900);
else 
	$graph  = new PieGraph (1000,900);
//$graph->SetShadow();  
$graph->title-> Set( $parent."\n"."Number of Accesses Percentages");
$p1 = new PiePlot($num_accesses);
$p1->SetLegends($siblings);  
$p1->SetTheme("sand");
//$p1->SetAngle(90);
$p1->SetCenter(0.4,0.5);
$p1 ->SetGuideLines (false);
$graph ->legend->Pos( 0.05,0.5,"right" ,"center");
$graph->Add( $p1,0,0); 
$graph->Stroke(); 


