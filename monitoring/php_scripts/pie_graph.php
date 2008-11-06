/******************************************************************************
 *              pie_graph.php
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
include ("../jpgraph-2.3.3/src/jpgraph_pie3d.php"); 
include ("no_data.php");
include ("user.php");
//connection
$conn = ocilogon(username,pass,serv);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
$period = $_GET['period'];
if($period == NULL) $period = 10000;
$query1 = "select distinct state, count(state) over (Partition by state) 
	   from requests 
	   where timestamp > sysdate - $period
	   order by state";

if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$i = 0;
while (OCIFetch($parsed1)) {
	$num_files[$i] = OCIResult($parsed1,2);
	$i++;
}

if(($num_files[0] + $num_files[1] + $num_files[2]) == 0) {
	No_Data_Image();
	exit();
};
$data = array($num_files[1],$num_files[0],$num_files[2]);
$leg = array("Resident Files","Pool Copies","Tape Recalls"); 
 if ($period == 10/1440) 
	$graph = new PieGraph(420,200,"auto",1);
 else if ($period == 1/24)
	$graph = new PieGraph(420,200,"auto",5);
 else if ($period == 1)
	$graph = new PieGraph(420,200,"auto",30);
 else if ($period == 7)
	$graph = new PieGraph(420,200,"auto",60);
 else if ($period == 30)
	$graph = new PieGraph(420,200,"auto",360);
 else  
	$graph = new PieGraph(420,200,"auto",360); 
$graph->SetShadow();
$graph->title-> Set( "File Requests Percentages");
$p1 = new PiePlot3D($data);
$p1->SetLegends($leg);  
$p1->SetTheme("sand");
$p1->SetAngle(50);
$graph->Add( $p1); 
$graph->Stroke(); 
?>
