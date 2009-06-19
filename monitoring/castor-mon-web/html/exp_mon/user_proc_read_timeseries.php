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
$username = $_GET['username'];
//connect to DB	
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if (!$conn) {
     $e = ocierror();
     print htmlentities($e['message']);
     exit();
}
$processed_reqs = "select distinct to_char(trunc(timestamp,'HH24'), 'HH24:Mi') bin, 
                         count(*) over (Partition by trunc(timestamp,'HH24')) total_req,
			 sum(case when state = 'DiskHit' then 1 else 0 end) over (Partition by trunc(timestamp,'HH24')) dh,
			 sum(case when state = 'DiskCopy' then 1 else 0 end) over (Partition by trunc(timestamp,'HH24')) dc,
			 sum(case when state = 'TapeRecall' then 1 else 0 end) over (Partition by trunc(timestamp,'HH24')) tr
                  from ".$db_instances[$service]['schema'].".requests
                  where timestamp >= trunc(sysdate - 4/24,'HH24')
                    and timestamp <= trunc(sysdate,'HH24')
                    and username = :username
                    and type in ('StagePrepareToGetRequest','StageGetRequest')
                  order by bin";
if (!($parsed_processed_reqs = OCIParse($conn, $processed_reqs))) 
	{ echo "Error Parsing Query"; exit();}
ocibindbyname($parsed_processed_reqs,":username",$username);	
if (!OCIExecute($parsed_processed_reqs))
	{ echo "Error Executing Query";exit();}

$i = 0;
while (OCIFetch($parsed_processed_reqs)) {
	$time = OCIResult($parsed_processed_reqs,1);
	$total = OCIResult($parsed_processed_reqs,2);
	$dh = OCIResult($parsed_processed_reqs,3);
	$dc = OCIResult($parsed_processed_reqs,4);
	$tr = OCIResult($parsed_processed_reqs,5);
	$tick_labels[$i]= $time;
	$data['total'][$time] = $total;
	$data['dh'][$time] = $dh;
	$data['dc'][$time] = $dc;
	$data['tr'][$time] = $tr;
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
$graph->title->Set("Processed Read File Requests (Last 4 Hours)");
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

$plot1 = new LinePlot(array_values($data['total']));
$plot1->SetColor("black");
$plot1 -> SetLegend("Total Req. (#)");
$graph->Add($plot1);

$plot2 = new LinePlot(array_values($data['dh']));
$plot2->SetColor("green");
$plot2 -> SetLegend("Disk Cache Hits (#)");
$graph->AddY(0,$plot2);
$graph->ynaxis[0]->SetColor('green');

$plot3 = new LinePlot(array_values($data['dc']));
$plot3->SetColor("orange");
$plot3 -> SetLegend("D2D Copies (#)");
$graph->AddY(1,$plot3);
$graph->ynaxis[1]->SetColor('orange');

$plot4 = new LinePlot(array_values($data['tr']));
$plot4->SetColor("red");
$plot4 -> SetLegend("Tape Recalls (#)");
$graph->AddY(2,$plot4);
$graph->ynaxis[2]->SetColor('red');

$graph->legend->SetLayout(LEGEND_HOR);
$graph->legend->SetColumns(2);
$graph->legend->SetLineWeight(4);
$graph->legend->Pos(0.043,0.95,"left","bottom");
$graph->Stroke();
?> 
