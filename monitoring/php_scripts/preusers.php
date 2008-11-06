/******************************************************************************
 *              preusers.php
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
$query1 = "select * from (
select a.username , count(*) stage, temp.prestage
	   from requests a 
           left outer join (
                 select username, count(*) prestage 
      		 from requests
      		 where type = 'StagePrepareToGetRequest'
		 and timestamp > sysdate - $period
      		 group by username) temp
	   on a.username = temp.username
	   where a.type = 'StageGetRequest'
	   	and a.timestamp > sysdate - $period
	   group by a.username , temp.prestage
     	order by stage desc) 
	where (stage - prestage < 0) and rownum < 11
	order by prestage desc";

if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$i = 0;
while (OCIFetch($parsed1)) {
	$names[$i] = OCIResult($parsed1,1);
	$stag[$i] = OCIResult($parsed1,2);
	$pref = OCIResult($parsed1,3);
	if($pref == NULL) $prestage[$i] = 0;
	else $prestage[$i] = $pref;
	$i++;		
}
if(empty($names)) {
	No_Data_Image();
	exit();
};
$i = 0;

$graph = new Graph(800,600,"auto");
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Requests per USER(Top Ten)");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
$graph->yaxis->title->Set("Prestaged / Non - Prestaged Requets" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);

$graph->xaxis->SetTickLabels($names);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("USER");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($prestage);
$b1->SetWidth(0.5);
$b1->SetFillColor("orangered");
$b1->value->SetFont(FF_ARIAL,FS_BOLD,10);
$b1->value->SetAngle(90);
$b1->value->SetColor("orangered");
$b1->value->Show();
$b1->value->SetFormat('%0.0f');
$b1 -> SetLegend("Prestaged Requests");
$b2 = new BarPlot($stag);
$b2->SetWidth(0.5);
$b2->SetFillColor("darkgoldenrod1");
$b2->value->SetFont(FF_ARIAL,FS_BOLD,10);
$b2->value->SetColor("darkgoldenrod1");
$b2->value->SetAngle(90);
$b2->value->Show();
$b2->value->SetFormat('%0.0f');
$b2 -> SetLegend("Non Prestaged Requests");
$gbplot = new GroupBarPlot(array($b2,$b1));
$graph->Add($gbplot);
$graph->Stroke();

?> 



	
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	
