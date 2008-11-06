/******************************************************************************
 *              oldrequestsperpool.php
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
$query1 = "select svcclass, count(*) reqs
	   from requests
	   where timestamp > sysdate - $period
	   group by svcclass
	   order by reqs desc";

if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$i = 0;
while (OCIFetch($parsed1)) {
	$pool_names[$i] = OCIResult($parsed1,1);
	$num_reqs[$i] = OCIResult($parsed1,2);
	$i++;		
}
if(empty($pool_names)) {
	No_Data_Image();
	exit();
};
$query2 = "select svcclass,count(*) diskhits
      	        from requests
      		where timestamp > sysdate - $period
		      and state='DiskHit'
      		group by svcclass ";
	   
if (!($parsed2 = OCIParse($conn, $query2))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed2))
	{ echo "Error Executing Query";exit();}
	
while (OCIFetch($parsed2)) {
	$pool = OCIResult($parsed2,1);
	$diskhits[$pool] = OCIResult($parsed2,2);
}
$query3 = "select svcclass,count(*) diskcopies
      	        from requests
      		where timestamp > sysdate - $period
		      and state='DiskCopy'
      		group by svcclass ";
	   
if (!($parsed3 = OCIParse($conn, $query3))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed3))
	{ echo "Error Executing Query";exit();}
	
while (OCIFetch($parsed3)) {
	$pool = OCIResult($parsed3,1);
	$diskcopies[$pool] = OCIResult($parsed3,2);
}
$query4 = "select svcclass,count(*) taperecalls
      	        from requests
      		where timestamp > sysdate - $period
		      and state='TapeRecall'
      		group by svcclass ";
	   
if (!($parsed4 = OCIParse($conn, $query4))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed4))
	{ echo "Error Executing Query";exit();}
	
while (OCIFetch($parsed4)) {
	$pool = OCIResult($parsed4,1);
	$taperecalls[$pool] = OCIResult($parsed4,2);
}
$i = 0;
foreach ($pool_names as $pn) {
	if( $diskhits[$pn] != NULL )
		$dh[$i] = $diskhits[$pn];
	else $dh[$i] = 0;
	if( $diskcopies[$pn] != NULL )
		$dc[$i] = $diskcopies[$pn];
	else $dc[$i] = 0;
	if( $taperecalls[$pn] != NULL )
		$tr[$i] = $taperecalls[$pn];
	else $tr[$i] = 0;
	$i++;
}

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
$graph->title->Set("Requests per POOL");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
// $graph->yaxis->title->Set("Number of Requests" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);
$graph->yaxis->SetFont(FF_FONT1,FS_BOLD,8);
$graph->xaxis->SetTickLabels($pool_names);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD,8);
// $graph->xaxis->title->Set("POOL");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($dh);
$b1->SetFillColor("orangered");
$b1 -> SetLegend("Resident Files");
$b2 = new BarPlot($dc);
$b2->SetFillColor("maroon");
$b2 -> SetLegend("Pool Copies");
$b3 = new BarPlot($tr);
$b3->SetFillColor("darkgoldenrod1");
$b3 -> SetLegend("Tape Recalls");
$gbplot = new AccBarPlot(array($b1,$b2,$b3));
$gbplot->value->Show(); 
$graph->Add($gbplot);
$graph->Stroke();

?> 



	
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	
