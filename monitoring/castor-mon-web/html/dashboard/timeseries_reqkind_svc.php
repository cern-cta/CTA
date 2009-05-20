<?php 
//Timeseries for selected request kind(DiskHit,DiskCopy,TapeRecall)
//include necessary libraries
include ("../lib/_oci_no_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_line.php");
include("../lib/no_data.php");
//user account
include ("../../../conf/castor-mon-web/user.php");
//get posted values
$svcclass = $_GET['svcclass'];
$pattern_1 = '/[a-zA-Z0-9]{1,15}/';
preg_match($pattern_1,$svcclass,$match);
$svcclass = $match[0];
if($svcclass == NULL) $svcclass = 'default';
$reqkind = $_GET['reqkind'];
$pattern_1 = '/[a-zA-Z0-9]{1,15}/';
preg_match($pattern_1,$reqkind,$match);
$reqkind = $match[0];
$service = $_GET['service'];
//connection - DB login
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}

$query1 = "select to_char(bin,'HH24:MI') , number_of_req from (
	   select distinct trunc(timestamp,'Mi') bin, count(trunc(timestamp,'Mi')) 
	   over (Partition by trunc(timestamp,'Mi')) number_of_req  
           from ".$db_instances[$service]['schema'].".requests
           where timestamp >= trunc(sysdate - 15/1440,'Mi')
		   and timestamp < trunc(sysdate - 5/1440,'Mi') 
		   and state = :reqkind
		   and svcclass = :svcclass)
           order by bin";
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
ocibindbyname($parsed1,":reqkind",$reqkind);
ocibindbyname($parsed1,":svcclass",$svcclass);
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$i = 0;
//fetch data into local table
while (OCIFetch($parsed1)) {
	$intervals[$i] =  OCIResult($parsed1,1);
	$number[$i] = OCIResult($parsed1,2);
	$i++;		
}
//if no data retrieved then return "no data available" image
if(empty($intervals)) {
	No_Data_Image();
	exit();
};
//create new graph
$graph = new Graph(420,200,"auto");
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("$reqkind Timeseries");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(80,20,20,120);
$graph->yaxis->title->Set("Requests");
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