/******************************************************************************
 *              gcfileage.php
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
$conn = $conn = ocilogon(username,pass,serv);
  if (!$conn) {
   $e = oci_error();
   print htmlentities($e['message']);
   exit;
  }
 
$bins = array( 0 =>"<10sec",1 =>"[10-60)sec",2 =>"[1-15)min",3 =>"[15-30)min",4 =>"[30-60)min",5 =>"[1-6)hours",6 =>"[6 -12)hours", 7=> "[12-24)hours", 8=> "[1-2)days", 9=> "[2-4)days", 10=> "[4-8)days",11=> "[8-16)days", 12=> ">=16days");
$period = $_GET['period'];
if($period == NULL) $period = 10000;

$query1 = "select distinct bin, count(bin) over (Partition by bin) reqs 
	from (
	select round(fileage), case when fileage < 10 then 'bin1'
	  when fileage >= 10 and fileage < 60 then 'bin2'
	  when fileage >= 60 and fileage < 900 then 'bin3'
	  when fileage >= 900 and fileage < 1800 then 'bin4'
	  when fileage >= 1800 and fileage < 3600 then 'bin5'
	  when fileage >= 3600 and fileage < 21600 then 'bin6'
	  when fileage >= 21600 and fileage < 43200 then 'bin7'
	  when fileage >= 43200 and fileage < 86400 then 'bin8'
	  when fileage >= 86400 and fileage < 172800 then 'bin9'
	  when fileage >= 172800 and fileage < 345600 then 'binn10'
	  when fileage >= 345600 and fileage < 691200 then 'binn11'
    when fileage >= 691200 and fileage < 1382400 then 'binn12'
	  else 'binn13' end bin
	from gcfiles
	where fileage is not null
	and timestamp > sysdate - $period)
	order by bin ";

$parsed1 = OCIParse($conn, $query1);

OCIExecute($parsed1);


$j=0;

while ( OCIFetch($parsed1) ) {
  $fileage[$j] = OCIResult($parsed1, 2);
 
  $j++;   
}
if(empty($fileage)) {
	No_Data_Image();
	exit();
};

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
$graph->title->Set("FileAge Distribution of Garbage Collected Files");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
$graph->yaxis->title->Set("Number of Files" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);

$graph->xaxis->SetTickLabels($bins);
$graph->xaxis->SetLabelAngle(0);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("Age");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($fileage);
$b1->SetFillColor("red");
$b1->value->Show();
$b1->value->SetFormat('%0.00f');
$graph->Add($b1);
$graph->Stroke();
?>
