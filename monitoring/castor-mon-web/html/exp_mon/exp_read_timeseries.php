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
//connect to DB	
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if (!$conn) {
     $e = ocierror();
     print htmlentities($e['message']);
     exit();
}
$incoming_reqs = "select distinct to_char(trunc(timestamp,'Mi'), 'HH24:Mi') bin, sum(requests) over (Partition by trunc(timestamp,'Mi')) number_of_req
                  from ".$db_instances[$service]['schema'].".requeststats
                  where timestamp >= trunc(sysdate - 4/24,'Mi')
                    and timestamp <= trunc(sysdate,'Mi')
                    and euid = '-'
                    and requesttype in ('StagePrepareToGetRequest','StageGetRequest')
                  order by bin";
$dispatched_reqs = "select distinct to_char(trunc(timestamp,'Mi'), 'HH24:Mi') bin, nvl(sum(dispatched),0) dispatched, nvl(round(avg(avgqueuetime),3),0) avg_time
		    from ".$db_instances[$service]['schema'].".queuetimestats
                    where timestamp >= trunc(sysdate - 4/24,'Mi')
                      and timestamp <= trunc(sysdate,'Mi')
                      and requesttype = 'StageGetRequest'
                    group by trunc(timestamp,'Mi')
                    order by bin";
$latency_reqs = "select distinct to_char(trunc(timestamp,'Mi'), 'HH24:Mi') bin, nvl(sum(started),0) dispatched, nvl(round(avg(avglatencytime),3),0) avg_time
		 from ".$db_instances[$service]['schema'].".latencystats
                 where timestamp >= trunc(sysdate - 4/24,'Mi')
                   and timestamp <= trunc(sysdate,'Mi')
                   and requesttype = 'StageGetRequest'
                 group by trunc(timestamp,'Mi')
                 order by bin";		 
if (!($parsed_incoming_reqs = OCIParse($conn, $incoming_reqs))) 
	{ echo "Error Parsing Query"; exit();}
if (!($parsed_dispatched_reqs = OCIParse($conn, $dispatched_reqs))) 
	{ echo "Error Parsing Query"; exit();}
if (!($parsed_latency_reqs = OCIParse($conn, $latency_reqs))) 
	{ echo "Error Parsing Query"; exit();}
if (!OCIExecute($parsed_incoming_reqs))
	{ echo "Error Executing Query";exit();}
if (!OCIExecute($parsed_dispatched_reqs))
	{ echo "Error Executing Query";exit();}
if (!OCIExecute($parsed_latency_reqs))
	{ echo "Error Executing Query";exit();}

$i = 0;
while (OCIFetch($parsed_incoming_reqs)) {
	$time = OCIResult($parsed_incoming_reqs,1);
	$incoming = OCIResult($parsed_incoming_reqs,2);
	$tick_labels[$i]= $time;
	$data[$time]['incoming'] = $incoming;
	$i++;
}
while (OCIFetch($parsed_dispatched_reqs)) {
        $time_d = OCIResult($parsed_dispatched_reqs,1);
	$dispatched = OCIResult($parsed_dispatched_reqs,2);
	$dispatched_avg_time = (float)OCIResult($parsed_dispatched_reqs,3);
	if (!in_array($time_d,$tick_labels)){
		$tick_labels[$i] = $time_d;
		$i++;
	}
	$data[$time_d]['dispatched'] = $dispatched;
	$data[$time_d]['avg_queue_time'] = $dispatched_avg_time;
}
while (OCIFetch($parsed_latency_reqs)) {$graph->legend->Pos(0.5,0.95,"center","bottom");
        $time_l = OCIResult($parsed_latency_reqs,1);	
	$processed = OCIResult($parsed_latency_reqs,2);	
	$latency_avg = (float)OCIResult($parsed_latency_reqs,3);
	if (!in_array($time_l,$tick_labels)){
		$tick_labels[$i] = $time_l;
		$i++;
	}
	$data[$time_d]['processed'] = $processed;
	$data[$time_d]['avg_lat_time'] = $latency_avg;
}	  


//create new graph
$graph = new Graph(800,320);
//$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("File Read Requests Timeseries");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(40,200,40,110);
$graph->SetMarginColor('white');

$graph->SetYScale(0,'lin');
$graph->SetYScale(1,'lin');
$graph->SetYScale(2,'lin');
$graph->SetYScale(3,'lin');

if(empty($tick_labels)) {
	No_Data_Image();
	exit();
};
sort($tick_labels);
for ($i = 0,$j = 0; $i < sizeof($tick_labels); $i++) {
  $tick = $tick_labels[$i];
  if ($data[$tick]['incoming'] == NULL)
  	$data_incoming[$tick] = 0;
  else
        $data_incoming[$tick] = $data[$tick]['incoming'];
  if ($data[$tick]['dispatched'] == NULL)
  	$data_disp[$tick] = 0;
  else
  	$data_disp[$tick] = $data[$tick]['dispatched'];
  if ($data[$tick]['avg_queue_time'] == NULL)
  	$data_avg_queue[$tick] = 0;
  else
        $data_avg_queue[$tick] = $data[$tick]['avg_queue_time'];
  if ($data[$tick]['processed'] == NULL)
  	$data_proc[$tick] = 0;
  else 
       	$data_proc[$tick] = $data[$tick]['processed'];
  if ($data[$tick]['avg_lat_time'] == NULL)
  	$data_avg_lat[$tick] = 0;
  else
     	$data_avg_lat[$tick] = $data[$tick]['avg_lat_time'];
}	

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

$plot1 = new LinePlot(array_values($data_incoming));
$plot1->SetColor("black");
$plot1 -> SetLegend("Incoming Req. (#)");
$graph->Add($plot1);

$plot2 = new LinePlot(array_values($data_disp));
$plot2->SetColor("blue");
$plot2 -> SetLegend("Dispatched Req. (#)");
$graph->AddY(0,$plot2);
$graph->ynaxis[0]->SetColor('blue');

$plot3 = new LinePlot(array_values($data_avg_queue));
$plot3->SetColor("red");
$plot3 -> SetLegend("Avg Queue Time (sec)");
$graph->AddY(1,$plot3);
$graph->ynaxis[1]->SetColor('red');

$plot4 = new LinePlot(array_values($data_proc));
$plot4->SetColor("green");
$plot4 -> SetLegend("Processed Req. (#)");
$graph->AddY(2,$plot4);
$graph->ynaxis[2]->SetColor('green');

$plot5 = new LinePlot(array_values($data_avg_lat));
$plot5->SetColor("orange");
$plot5 -> SetLegend("Avg Latency (sec)");
$graph->AddY(3,$plot5);
$graph->ynaxis[3]->SetColor('orange');

$graph->legend->SetLayout(LEGEND_HOR);
$graph->legend->SetColumns(3);
$graph->legend->SetLineWeight(4);
$graph->legend->Pos(0.043,0.95,"left","bottom");
$graph->Stroke();
?> 
