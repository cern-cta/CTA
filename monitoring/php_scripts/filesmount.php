/******************************************************************************
 *              filesmount.php
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
include ("no_data.php");
include ("user.php");
$period = $_GET['period'];
if($period == NULL) $period = 10000;
//initialization
$bins = array( 0 =>"=1",1 =>"[2-5)",2 =>"[5-10)Kb",3 =>"[10-40)",4 =>"[40-80)",5 =>"[80-120)",6 =>"[120-160)", 7=> "[160-200)",8=> "[200-250)",9=> "[250-300)",10=> "[300-400)",11=> "[400-500)",12=> "[500-1000)",13=> ">=1000");
//connection
$conn = ocilogon(username,pass,serv);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
$query1 = "select distinct bin, count(bin) over (Partition by bin) reqs 
	from (
	select value, case when value = 1  then 1
	  when value > 1 and value < 5 then 2
	  when value >= 5 and value < 10 then 3
	  when value >= 10 and value < 40 then 4
	  when value >= 40 and value < 80 then 5
	  when value >= 80 and value < 120 then 6
	  when value >= 120 and value < 160 then 7
	  when value >= 160 and value < 200 then 8
	  when value >= 200 and value < 250 then 9
	  when value >= 250 and value < 300 then 10
	  when value >= 300 and value < 400 then 11
	  when value >= 400 and value < 500 then 12
	  when value >= 500 and value < 1000 then 13
	  else 14 end bin
		from (select b.value
	from castor_dlf.dlf_messages a, castor_dlf.dlf_num_param_values b
	where a.id = b.id
	  and b.name = 'FILESCP'
	  and facility = 2 and msg_no = 20
	  and a.timestamp > sysdate - $period
	  ) )
	order by bin";

if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
for($i = 0;$i < 13; $i++)
	$files[$i] = 0;
while (OCIFetch($parsed1)) {
	$bin_num = OCIResult($parsed1,1);
	$files[$bin_num - 1] = OCIResult($parsed1,2);
}

if(empty($bin_num)) {
	No_Data_Image();
	exit();
};
//data processing

//plot
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
$graph->title->Set("Files Recalled per Mount - Distribution");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
$graph->yaxis->title->Set("Times" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);

$graph->xaxis->SetTickLabels($bins);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("Number of Files");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($files);
$b1->SetFillColor("orangered");
$b1->value->Show();
$b1->value->SetFormat('%0.0f');
$graph->Add($b1);
$graph->Stroke();
?> 



	
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	
