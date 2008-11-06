/******************************************************************************
 *              prefetch.php
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
$query1 = "select distinct svcclass,type, count(type) over (Partition by svcclass,type) number_of_req 
	   from requests
	   where timestamp > sysdate - $period
	   and state = 'TapeRecall'
	   order by svcclass,type";

if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}


$pools[-1] = 'foo';
$i = 0;
$stag_found = 0;
$pre_stag_found = 0;
while (OCIFetch($parsed1)) {
	$name = OCIResult($parsed1,1);
	$type = OCIResult($parsed1,2);
	$reqs = OCIResult($parsed1,3);
	if (!in_array($name,$pools)) {
		if(($stag_found == 1)&&($pre_stag_found == 0))
			$prestage[$i-1] = 0;
		if(($stag_found == 0)&&($pre_stag_found == 1))
			$stag[$i-1] = 0;
		$pool_names[$i] = $name;
		$pools[$i] = $name;
		$stag_found = 0;
		$pre_stag_found = 0;
		$i++;
	}
	if ($type == 'StageGetRequest') {
		$stag[$i-1] = $reqs;
		$stag_found = 1;
	}
	else if ($type == 'StagePrepareToGetRequest') {
		$prestage[$i-1] = $reqs;
		$pre_stag_found = 1;
		
	}
}
if(empty($pool_names)) {
	No_Data_Image();
	exit();
};
if ($period == 10/1440) 
	$graph = new Graph(700,350,"auto",1);
else if ($period == 1/24)
	$graph = new Graph(700,350,"auto",5);
else if ($period == 1)
	$graph = new Graph(700,350,"auto",30);
else if ($period == 7)
	$graph = new Graph(700,350,"auto",60);
else if ($period == 30)
	$graph = new Graph(700,350,"auto",360);
else 
	$graph = new Graph(700,350,"auto",360);
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Prestaged / Non - Prestaged TapeRecalled Files");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(80,80,80,120);
$graph->yaxis->title->Set("Requests per POOL" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);
$graph->legend->Pos(0.5,0.95,"center","bottom");
$graph->xaxis->SetTickLabels($pool_names);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("POOL");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($prestage);
$b1->SetWidth(0.5);
$b1->SetFillColor("orangered");
$b1->value->SetFont(FF_FONT1,FS_BOLD,10);
$b1->value->SetAngle(90);
$b1->value->SetColor("orangered");
$b1->value->Show();
$b1->value->SetFormat('%0.0f');
$b1 -> SetLegend("Prestaged Requests");
$b2 = new BarPlot($stag);
$b2->SetWidth(0.5);
$b2->SetFillColor("darkgoldenrod1");
$b2->value->SetFont(FF_FONT1,FS_BOLD,10);
$b2->value->SetColor("darkgoldenrod1");
$b2->value->SetAngle(90);
$b2->value->Show();
$b2->value->SetFormat('%0.0f');
$b2 -> SetLegend("Non Prestaged Requests");
$gbplot = new GroupBarPlot(array($b2,$b1));
$graph->Add($gbplot);
$graph->Stroke();

?> 



	
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	
