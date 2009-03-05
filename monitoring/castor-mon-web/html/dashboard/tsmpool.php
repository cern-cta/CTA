<?php 
//Migrated files timeseries for selected service class(last ten minutes)
include ("../lib/_oci_no_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_line.php");
include("../lib/no_data.php");
//user account
include ("../../../conf/castor-mon-web/user.php");
//get posted data 
$svcclass = $_GET['svcclass'];
$pattern_1 = '/[a-zA-Z0-9]{1,15}/';
preg_match($pattern_1,$svcclass,$match);
$svcclass = $match[0];
if($svcclass == NULL) $svcclass = 'default';
$reqkind = $_GET['reqkind'];
preg_match($pattern_1,$reqkind,$match_1);
$reqkind = $match_1[0];
$service = $_GET['service'];
//define interval and format of timeseries
$interval = 'Mi';
$format = 'HH24:MI';
$inter = 'Mi';

//connection - db login
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}

$query1 = "select to_char(bin,'$format') , number_of_req from (
	   select distinct trunc(NN_Mig_timestamp,'$interval') bin, count(trunc(NN_Mig_timestamp,'$interval')) 
	   over (Partition by trunc(NN_Mig_timestamp,'$interval')) number_of_req  
           from ".$db_instances[$service]['schema']."migration
           where NN_Mig_timestamp >= trunc(sysdate - 15/1440,'$inter')
		   and NN_Mig_timestamp < trunc(sysdate -  5/1440,'$inter') 
		   and svcclass = '$svcclass')
           order by bin";
	   
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$i = 0;
//fetch data into local tables
while (OCIFetch($parsed1)) {
	$intervals[$i] =  OCIResult($parsed1,1);
	$number[$i] = OCIResult($parsed1,2);
	$i++;		
}
//if no data retrieved print out "no data available" image
if(empty($intervals)) {
	No_Data_Image();
	exit();
};
//plot data
$graph = new Graph(420,200,"auto");
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Migration Timeseries($svcclass)");
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