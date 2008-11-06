/******************************************************************************
 *              gcreq.php
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
$bins = array( 0 =>"<0.1 h",1 =>"[0.1-0.5) h",2 =>"[0.5-1) h",3 =>"[1-2) h",4 =>"[2-4) h",5 =>"[4-6) h",6 =>"[6-8] h", 7=> "(8-24) h");
//connection
$conn = ocilogon(username,pass,serv);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
$query1 = "select distinct bin, count(bin) over (Partition by bin) reqs
		from (
    			select dif , case when dif < 0.1 then 1
            when dif >= 0.1 and dif < 0.5 then 2
            when dif >= 0.5 and dif < 1 then 3
            when dif >= 1 and dif < 2 then 4
            when dif >= 2 and dif < 4 then 5
            when dif >= 4 and dif < 6 then 6
            when dif >= 6 and dif <= 8 then 7
                else 8 end bin 
          from req_del
          where timestamp > sysdate - $period)
  		order by bin";

if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
for($i = 0;$i < 8; $i++)
	$interval[$i] = 0;
while (OCIFetch($parsed1)) {
	$bin_num = OCIResult($parsed1,1);
	$interval[$bin_num - 1] = OCIResult($parsed1,2);
}

if(empty($bin_num)) {
	No_Data_Image();
	exit();
};
//data processing

//plot
if ($period == 10/1440) 
	$graph = new Graph(730,300,"auto",1);
else if ($period == 1/24)
	$graph = new Graph(730,300,"auto",5);
else if ($period == 1)
	$graph = new Graph(730,300,"auto",30);
else if ($period == 7)
	$graph = new Graph(730,300,"auto",60);
else if ($period == 30)
	$graph = new Graph(730,300,"auto",360);
else 
	$graph = new Graph(730,300,"auto",360);
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Files Requested After Garbage Collection");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
$graph->yaxis->title->Set("Number of Files" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);

$graph->xaxis->SetTickLabels($bins);
$graph->xaxis->SetLabelAngle(0);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("time");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($interval);
$b1->SetFillColor("red");
$b1->value->Show();
$b1->value->SetFormat('%0.0f');
$graph->Add($b1);
$graph->Stroke();
?> 



	
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	
