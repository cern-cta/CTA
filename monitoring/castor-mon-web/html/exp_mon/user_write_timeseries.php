<?php 
error_reporting(E_ALL ^ E_NOTICE);
include ("../lib/_oci_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_line.php");
include("../lib/no_data.php");
//include user account
include ("../../../conf/castor-mon-web/user.php");
//get posted values
$service = $_GET['service'];
$euid = $_GET['euid'];
//connect to DB	
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if (!$conn) {
     $e = ocierror();
     print htmlentities($e['message']);
     exit();
}
$incoming_reqs = "select distinct to_char(trunc(timestamp,'Mi'), 'HH24:Mi') bin, 
                         sum(requests) over (Partition by trunc(timestamp,'Mi')) total_req,
			 sum(case when type = 'StagePutRequest' then requests else 0 end) over (Partition by trunc(timestamp,'Mi')) number_of_put_req,
			 sum(case when type = 'StagePrepareToPutRequest' then requests else 0 end) over (Partition by trunc(timestamp,'Mi')) number_of_prep_put_req,
			 sum(case when type = 'StagePreparePutDoneRequest' then requests else 0 end) over (Partition by trunc(timestamp,'Mi')) number_of_put_done_req
                  from ".$db_instances[$service]['schema'].".requeststats
                  where timestamp >= trunc(sysdate - 4/24,'Mi')
                    and timestamp <= trunc(sysdate,'Mi')
                    and euid = :euid
                    and type in ('StagePrepareToPutRequest','StagePutRequest','StagePutDoneRequest')
                  order by bin";
if (!($parsed_incoming_reqs = OCIParse($conn, $incoming_reqs))) 
	{ echo "Error Parsing Query"; exit();}
ocibindbyname($parsed_incoming_reqs,":euid",$euid);	
if (!OCIExecute($parsed_incoming_reqs))
	{ echo "Error Executing Query";exit();}

$i = 0;
while (OCIFetch($parsed_incoming_reqs)) {
	$time = OCIResult($parsed_incoming_reqs,1);
	$incoming = OCIResult($parsed_incoming_reqs,2);
	$incoming_get = OCIResult($parsed_incoming_reqs,3);
	$incoming_prep_get = OCIResult($parsed_incoming_reqs,4);
	$tick_labels[$i]= $time;
	$data['incoming'][$time] = $incoming;
	$data['incoming_put'][$time] = $incoming_get;
	$data['incoming_prep_put'][$time] = $incoming_prep_get;
	$data['incoming_put_done'][$time] = $incoming_prep_get;
	$i++;
}
if(empty($tick_labels)) {
	No_Data_Image();
	exit();
};
//create new graph
$graph = new Graph(500,300);
//$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Incoming Write Requests (Last 4 Hours)");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(40,200,40,110);
$graph->SetMarginColor('white');

$graph->SetYScale(0,'lin');
$graph->SetYScale(1,'lin');
$graph->SetYScale(2,'lin');

if(empty($tick_labels)) {
	No_Data_Image();
	exit();
};	
if (sizeof($tick_labels)> 20) {
	for ($i = 0,$j = 0; $i < sizeof($tick_labels); $i++) {
		if ($i % 5 ==  0) {
			$pos[$j] = $i;
			$final_ticks[$j++] = $tick_labels[$i];
		}
	}
	$graph->xaxis->SetTickPositions($pos);
	$graph->xaxis->SetTickLabels($final_ticks);
} 
else
	$graph->xaxis->SetTickLabels($tick_labels);

$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xgrid->Show();

$plot1 = new LinePlot(array_values($data['incoming']));
$plot1->SetColor("black");
$plot1 -> SetLegend("Incoming Req. (#)");
$graph->Add($plot1);

$plot2 = new LinePlot(array_values($data['incoming_put']));
$plot2->SetColor("green");
$plot2 -> SetLegend("Write File Req. (#)");
$graph->AddY(0,$plot2);
$graph->ynaxis[0]->SetColor('green');

$plot3 = new LinePlot(array_values($data['incoming_prep_put']));
$plot3->SetColor("red");
$plot3 -> SetLegend("Prepare To Write Req. (#)");
$graph->AddY(1,$plot3);
$graph->ynaxis[1]->SetColor('red');

$plot4 = new LinePlot(array_values($data['incoming_put_done']));
$plot4->SetColor("orange");
$plot4 -> SetLegend("Write Done Req. (#)");
$graph->AddY(2,$plot4);
$graph->ynaxis[2]->SetColor('orange');

$graph->legend->SetLayout(LEGEND_HOR);
$graph->legend->SetColumns(2);
$graph->legend->SetLineWeight(4);
$graph->legend->Pos(0.043,0.95,"left","bottom");
$graph->Stroke();
?> 
