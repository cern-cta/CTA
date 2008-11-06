/******************************************************************************
 *              usertapecontribution.php
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
  select username, count(username) con_mount
  from requests a, taperecall b
  where  a.subreqid = b.subreqid
  and a.timestamp > sysdate - $period
  and b.timestamp > sysdate - $period
  and b.tapemountstate in ('TAPE_PENDING','TAPE_WAITDRIVE','TAPE_WAITPOLICY','TAPE_WAITMOUNT') 
  group by username
  order by con_mount desc )
  where rownum < 11";

	   
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}

$i=0;
while (OCIFetch($parsed1)) {
	$username[$i] = OCIResult($parsed1,1);
	$mounts[$i] = OCIResult($parsed1,2);
	$i++;		
}
if(empty($username)) {
	No_Data_Image();
	exit();
};

if ($period == 10/1440) 
	$graph = new Graph(700,300,"auto",1);
else if ($period == 1/24)
	$graph = new Graph(700,300,"auto",5);
else if ($period == 1)
	$graph = new Graph(700,300,"auto",30);
else if ($period == 7)
	$graph = new Graph(700,300,"auto",60);
else if ($period == 30)
	$graph = new Graph(700,300,"auto",360);
else 
	$graph = new Graph(700,300,"auto",360);
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Top Ten Mounters");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
$graph->yaxis->title->Set("Number of Requests" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);

$graph->xaxis->SetTickLabels($username);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("Username");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($mounts);
$b1->SetFillColor("yellow");
$b1->value->Show(); 
$b1->value->SetFormat('%0.0f');
$graph->Add($b1);
$graph->Stroke();
?> 



	
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	
