/******************************************************************************
 *              oldtopusers.php
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
$query1 = "select username from (
      		select username,count(username) reqs
      		from requests
      		where timestamp > sysdate - $period
      		group by username 
      		order by reqs desc )
	   where rownum < 11";

	   
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$i = 0;
while (OCIFetch($parsed1)) {
	$username[$i] = OCIResult($parsed1,1);
	$i++;		
}
if(empty($username)) {
	No_Data_Image();
	exit();
};
$unames = "(";
foreach ($username as $un) {
	$unames .= "'$un'".",";
}
$unames = substr($unames, 0, -1);
$unames .= ")";

$query2 = "select username,count(username) diskhits
      	        from requests
      		where timestamp > sysdate - $period
		      and username in $unames
		      and state='DiskHit'
      		group by username ";
	   
if (!($parsed2 = OCIParse($conn, $query2))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed2))
	{ echo "Error Executing Query";exit();}
	
while (OCIFetch($parsed2)) {
	$uname = OCIResult($parsed2,1);
	$diskhits[$uname] = OCIResult($parsed2,2);
}
$query3 = "select username,count(username) diskcopies
      	        from requests
      		where timestamp > sysdate - $period
		      and username in $unames
		      and state='DiskCopy'
      		group by username ";
	   
if (!($parsed3 = OCIParse($conn, $query3))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed3))
	{ echo "Error Executing Query";exit();}
	
while (OCIFetch($parsed3)) {
	$uname = OCIResult($parsed3,1);
	$diskcopies[$uname] = OCIResult($parsed3,2);
}
$query4 = "select username,count(username) taperecalls
      	        from requests
      		where timestamp > sysdate - $period
		      and username in $unames
		      and state='TapeRecall'
      		group by username ";
	   
if (!($parsed4 = OCIParse($conn, $query4))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed4))
	{ echo "Error Executing Query";exit();}
	
while (OCIFetch($parsed4)) {
	$uname = OCIResult($parsed4,1);
	$taperecalls[$uname] = OCIResult($parsed4,2);
}
$i = 0;
foreach ($username as $un) {
	if( $diskhits[$un] != NULL )
		$dh[$i] = $diskhits[$un];
	else $dh[$i] = 0;
	if( $diskcopies[$un] != NULL )
		$dc[$i] = $diskcopies[$un];
	else $dc[$i] = 0;
	if( $taperecalls[$un] != NULL )
		$tr[$i] = $taperecalls[$un];
	else $tr[$i] = 0;
	$i++;
}

$graph = new Graph(700,350,"auto");
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Top Ten Users(Requests - User)");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
// $graph->yaxis->title->Set("Number of Requests" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);

$graph->xaxis->SetTickLabels($username);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
// $graph->xaxis->title->Set("Username");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($dh);
$b1->SetFillColor("green");
$b1 -> SetLegend("Resident Files");
$b2 = new BarPlot($dc);
$b2->SetFillColor("yellow");
$b2 -> SetLegend("Pool Copies");
$b3 = new BarPlot($tr);
$b3->SetFillColor("red");
$b3 -> SetLegend("Tape Recalls");
$gbplot = new AccBarPlot(array($b1,$b2,$b3));
$gbplot->value->Show(); 
$b1->value->SetFormat('%0.0f');
$graph->Add($gbplot);
$graph->Stroke();
?> 



	
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	
