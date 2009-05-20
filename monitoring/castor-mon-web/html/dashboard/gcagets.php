<?php
//This script plots timeseries about the avg age of garbage collected files
//Used for GC Monitor (DASHBOARD)
//include necessary libraries 
include ("../lib/_oci_no_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_line.php");
include("../lib/no_data.php");
//include user account
include ("../../../conf/castor-mon-web/user.php");
//get posted values

$service = $_GET['service'];
//connection -DB login
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
//retrieve timeseries info
$query1 = "select to_char(bin,'HH24:MI') , number_of_req from (
			select trunc(timestamp,'Mi') bin, round(avg(fileage),4) number_of_req  
			from ".$db_instances[$service]['schema'].".gcfiles
			where timestamp >= trunc(sysdate - 15/1440,'Mi')
			and timestamp < trunc(sysdate - 5/1440 ,'Mi')
			group by trunc(timestamp,'Mi'))
		   order by bin";
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
//Fetch Data in local tables
$i = 0;
while (OCIFetch($parsed1)) {
	$intervals[$i] =  OCIResult($parsed1,1);
	$number[$i] = ((float)OCIResult($parsed1,2)/3600);
	$i++;		
}
//If no data retrieved display "No Data Available Image"
if(empty($intervals)) {
	No_Data_Image();
	exit();
};
//new graph
$graph = new Graph(420,200,"auto");
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("AvgAge of GC Files Timeseries");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(80,20,20,120);
$graph->yaxis->title->Set("Age (hours)");
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(60);
$graph->xaxis->SetTickLabels($intervals);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("time");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->xgrid->Show();
$b1 = new Lineplot($number);
$b1->SetColor("red");
$graph->Add($b1);
$graph->Stroke();
?> 



	
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	
