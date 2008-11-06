/******************************************************************************
 *              latencies.php
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
$bins = array( 0 =>"<1sec",1 =>"[1-10)sec",2 =>"[10-120)sec",3 =>"[2-5)min",4 =>"[5-10)min",5 =>"[10-30)min",6 =>"[30 -60)min", 7=> "[1-5)hours", 8=> "[5-12)hours", 9=> "[12-24)hours", 10=> "[1-2]days", 11=> ">2days");
//connection
$conn = ocilogon(username,pass,serv);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
for($i = 0;$i < 12; $i++)
	$diskhitlat[$i] = 0;
$query1 = "select distinct bin, count(bin) over (Partition by bin) reqs 
	from (
	select round(totallatency), case when totallatency < 1 then 1
	  when totallatency >= 1 and totallatency < 10 then 2
	  when totallatency >= 10 and totallatency < 120 then 3
	  when totallatency >= 120 and totallatency < 300 then 4
	  when totallatency >= 300 and totallatency < 600 then 5
	  when totallatency >= 600 and totallatency < 1800 then 6
	  when totallatency >= 1800 and totallatency < 3600 then 7
	  when totallatency >= 3600 and totallatency < 18000 then 8
	  when totallatency >= 18000 and totallatency < 43200 then 9
	  when totallatency >= 43200 and totallatency < 86400 then 10
	  when totallatency >= 86400 and totallatency < 172800 then 11
	  else 12 end bin
	from requests
	where totallatency is not null
	and timestamp > sysdate - $period
	and state='DiskHit')
	order by bin ";

if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
while (OCIFetch($parsed1)) {
	$bin_num = OCIResult($parsed1,1);
	$diskhitlat[$bin_num - 1] = OCIResult($parsed1,2);
}
for($i = 0;$i < 12; $i++)
	$diskcopylat[$i] = 0;
$query2 = "select distinct bin, count(bin) over (Partition by bin) reqs 
	from (
	select round(totallatency), case when totallatency < 1 then 1
	  when totallatency >= 1 and totallatency < 10 then 2
	  when totallatency >= 10 and totallatency < 120 then 3
	  when totallatency >= 120 and totallatency < 300 then 4
	  when totallatency >= 300 and totallatency < 600 then 5
	  when totallatency >= 600 and totallatency < 1800 then 6
	  when totallatency >= 1800 and totallatency < 3600 then 7
	  when totallatency >= 3600 and totallatency < 18000 then 8
	  when totallatency >= 18000 and totallatency < 43200 then 9
	  when totallatency >= 43200 and totallatency < 86400 then 10
	  when totallatency >= 86400 and totallatency < 172800 then 11
	  else 12 end bin
	from requests
	where totallatency is not null
	and timestamp > sysdate - $period
	and state='DiskCopy')
	order by bin ";

if (!($parsed2 = OCIParse($conn, $query2))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed2))
	{ echo "Error Executing Query";exit();}
while (OCIFetch($parsed2)) {
	$bin_num = OCIResult($parsed1,1);
	$diskcopylat[$bin_num - 1] = OCIResult($parsed2,2);
}
for($i = 0;$i < 12; $i++)
	$tapelat[$i] = 0;
$query3 = "select distinct bin, count(bin) over (Partition by bin) reqs 
	from (
	select round(totallatency), case when totallatency < 1 then 1
	  when totallatency >= 1 and totallatency < 10 then 2
	  when totallatency >= 10 and totallatency < 120 then 3
	  when totallatency >= 120 and totallatency < 300 then 4
	  when totallatency >= 300 and totallatency < 600 then 5
	  when totallatency >= 600 and totallatency < 1800 then 6
	  when totallatency >= 1800 and totallatency < 3600 then 7
	  when totallatency >= 3600 and totallatency < 18000 then 8
	  when totallatency >= 18000 and totallatency < 43200 then 9
	  when totallatency >= 43200 and totallatency < 86400 then 10
	  when totallatency >= 86400 and totallatency < 172800 then 11
	  else 12 end bin
	from requests
	where totallatency is not null
	and timestamp > sysdate - $period
	and state='TapeRecall')
	order by bin ";

if (!($parsed3 = OCIParse($conn, $query3))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed3))
	{ echo "Error Executing Query";exit();}
while (OCIFetch($parsed3)) {
	$bin_num = OCIResult($parsed3,1);
	$tapelat[$bin_num - 1] = OCIResult($parsed3,2);	
}

if(empty($diskhitlat) && empty($diskcopylat)&&empty($tapelat)) {
	No_Data_Image();;
	exit();
};

//data processing

//plot
if ($period == 10/1440) 
	$graph = new Graph(730,300,"auto",1);
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
$graph->title->Set("Request Latency Distribution");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
$graph->yaxis->title->Set("Number of Requests" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);

$graph->xaxis->SetTickLabels($bins);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("time");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($diskhitlat);
$b1->SetWidth(0.33);
$b1->SetFillColor("orangered");
$b1->value->SetFont(FF_FONT1,FS_BOLD,10);
$b1->value->SetAngle(90);
$b1->value->SetColor("orangered");
$b1->value->Show();
$b1->value->SetFormat('%0.0f');
$b1 -> SetLegend("Resident Files");
$b2 = new BarPlot($diskcopylat);
$b2->SetWidth(0.33);
$b2->SetFillColor("darkgoldenrod1");
$b2->value->SetFont(FF_FONT1,FS_BOLD,10);
$b2->value->SetColor("darkgoldenrod1");
$b2->value->SetAngle(90);
$b2->value->Show();
$b2->value->SetFormat('%0.0f');
$b2 -> SetLegend("Pool Copies");
$b3 = new BarPlot($tapelat);
$b3->SetWidth(0.34);
$b3->SetFillColor("maroon");
$b3->value->SetFont(FF_FONT1,FS_BOLD,10);
$b3->value->SetAngle(90);
$b3->value->SetColor("maroon");
$b3->value->Show();
$b3->value->SetFormat('%0.0f');
$b3 -> SetLegend("Tape Recalls");
$gbplot  = new GroupBarPlot (array($b1,$b2,$b3)); 
$graph->Add($gbplot);
$graph->Stroke();
?> 