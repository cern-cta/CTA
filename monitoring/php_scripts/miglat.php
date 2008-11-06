/******************************************************************************
 *              miglat.php
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
//initialization 
$bins = array( 0 =>"<1sec",1 =>"[1-2)sec",2 =>"[2-4)sec",3 =>"[4-6)sec",4 =>"[6-8)sec",5 =>"[8-10)sec",6 =>"[10-120)sec",7 =>"[2-5)min",8 =>"[5-10)min",9 =>"[10-30)min",10 =>"[30 -60)min", 11=> ">=1 hour");
//connection
$conn = ocilogon(username,pass,serv);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
for($i = 0;$i < 12; $i++)
	$lat[$i] = 0;
$query1 = "select distinct bin, count(bin) over (Partition by bin) migs 
	from (
	select round(totallatency), case when totallatency < 1 then 1
	  when totallatency >= 1 and totallatency < 2 then 2
	  when totallatency >= 2 and totallatency < 4 then 3
	  when totallatency >= 4 and totallatency < 6 then 4
	  when totallatency >= 6 and totallatency < 8 then 5
	  when totallatency >= 8 and totallatency < 10 then 6
	  when totallatency >= 10 and totallatency < 120 then 7
	  when totallatency >= 120 and totallatency < 300 then 8
	  when totallatency >= 300 and totallatency < 600 then 9
	  when totallatency >= 600 and totallatency < 1800 then 10
	  when totallatency >= 1800 and totallatency < 3600 then 11
	  else 12 end bin
	from migration
	where totallatency is not null
	and timestamp > sysdate - $period)
	order by bin ";

if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
while (OCIFetch($parsed1)) {
	$bin_num = OCIResult($parsed1,1);
	$lat[$bin_num - 1] = OCIResult($parsed1,2);
}
if(empty($lat)) {
	No_Data_Image();;
	exit();
};

//data processing

//plot
if ($period == 10/1440) 
	$graph = new Graph(800,300,"auto",1);
else if ($period == 1/24)
	$graph = new Graph(800,300,"auto",5);
else if ($period == 1)
	$graph = new Graph(800,300,"auto",30);
else if ($period == 7)
	$graph = new Graph(800,300,"auto",60);
else if ($period == 30)
	$graph = new Graph(800,300,"auto",360);
else 
	$graph = new Graph(800,300,"auto",360);
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Migration Latency Distribution");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
$graph->yaxis->title->Set("Number of Files Migrated" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);

$graph->xaxis->SetTickLabels($bins);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("time");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($lat);
$b1->SetWidth(0.33);
$b1->SetFillColor("orangered");
$b1->value->SetFont(FF_FONT1,FS_BOLD,10);
$b1->value->Show();
$b1->value->SetFormat('%0.0f');
$graph->Add($b1);
$graph->Stroke();
?> 