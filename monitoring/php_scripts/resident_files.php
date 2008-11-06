/******************************************************************************
 *              resident_files.php
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
include ("../jpgraph-2.3.3/src/jpgraph_bar.php");
include("no_data.php");
include ("user.php");
$period = $_GET['period'];
if($period == NULL) $period = 10000;
//connection
$conn = ocilogon(username,pass,serv);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
$query1 = "select svcclass,count(state) res_files  
	   from requests 
	   where state = 'DiskHit'
	   and timestamp > sysdate - $period
	   group by svcclass
	   order by res_files desc";

if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$i = 0;
while (OCIFetch($parsed1)) {
	$pool_names[$i] = OCIResult($parsed1,1);
	$num_res_files[$i] = OCIResult($parsed1,2);
	$pool = $pool_names[$i];
	$query2 = "select count(*)  
	   	   from requests 
	  	   where svcclass = '$pool'
		   and timestamp > sysdate - $period";
	$parsed2 = OCIParse($conn, $query2);
	OCIExecute($parsed2);
	while(OCIFetch($parsed2)){
		$total_files[$i] = OCIResult($parsed2,1);
	}
	$ratio[$i] = $num_res_files[$i]/$total_files[$i];
	$i++;		
}
if(empty($pool_names)) {
	No_Data_Image();
	exit();
};
$graph = new Graph(800,600,"auto");
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Resident Files / Total Files Requested per POOL");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
$graph->yaxis->title->Set("Ratio" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);

$graph->xaxis->SetTickLabels($pool_names);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("POOLS");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($ratio);
$b1->SetFillColor("maroon");
$b1->value->Show();
$b1->value->SetFormat('%01.02f');
$graph->Add($b1);
$graph->Stroke();
?> 



	
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	
