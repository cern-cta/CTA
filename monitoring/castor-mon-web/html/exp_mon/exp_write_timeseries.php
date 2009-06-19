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
$incoming_reqs = "select distinct to_char(trunc(timestamp,'Mi'), 'HH24:Mi') bin, 
			 sum(case when type = 'StagePutRequest' then requests else 0 end) over (Partition by trunc(timestamp,'Mi')) number_of_writes,
			 sum(case when type = 'StagePutDoneRequest' then requests else 0 end) over (Partition by trunc(timestamp,'Mi')) number_of_done,
			 sum(case when type = 'StagePrepareToPutRequest' then requests else 0 end) over (Partition by trunc(timestamp,'Mi')) space_reserve
                  from ".$db_instances[$service]['schema'].".requeststats
                  where timestamp >= trunc(sysdate - 4/24,'Mi')
                    and timestamp <= trunc(sysdate,'Mi')
                    and euid = '-'
                  order by bin";
$dispatched_reqs = "select distinct to_char(trunc(timestamp,'Mi'), 'HH24:Mi') bin, nvl(sum(dispatched),0) dispatched, nvl(round(avg(avgtime),3),0) avg_time
		    from ".$db_instances[$service]['schema'].".queuetimestats
                    where timestamp >= trunc(sysdate - 4/24,'Mi')
                      and timestamp <= trunc(sysdate,'Mi')
                      and type = 'StageGetRequest'
                    group by trunc(timestamp,'Mi')
                    order by bin";
$latency_reqs = "select distinct to_char(trunc(timestamp,'Mi'), 'HH24:Mi') bin, nvl(sum(started),0) dispatched, nvl(round(avg(avgtime),3),0) avg_time
		 from ".$db_instances[$service]['schema'].".latencystats
                 where timestamp >= trunc(sysdate - 4/24,'Mi')
                   and timestamp <= trunc(sysdate,'Mi')
                   and type = 'StageGetRequest'
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
	$put_done = OCIResult($parsed_incoming_reqs,3);
	$space = OCIResult($parsed_incoming_reqs,4);
	$tick_labels[$i]= $time;
	$data[$time]['incoming'] = $incoming;
	$data[$time]['put_done'] = $incoming;
	$data[$time]['space'] = $incoming;
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

if(empty($tick_labels)) {
	No_Data_Image();
	exit();
};

//create new graph
$graph = new Graph(800,320);
//$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Write File Requests Timeseries");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(40,300,40,110);
$graph->SetMarginColor('white');

$graph->SetYScale(0,'lin');
$graph->SetYScale(1,'lin');
$graph->SetYScale(2,'lin');
$graph->SetYScale(3,'lin');
$graph->SetYScale(4,'lin');
$graph->SetYScale(5,'lin');

sort($tick_labels);
for ($i = 0,$j = 0; $i < sizeof($tick_labels); $i++) {
  $tick = $tick_labels[$i];
  if ($data[$tick]['incoming'] == NULL)
  	$data_incoming[$tick] = 0;
  else
        $data_incoming[$tick] = $data[$tick]['incoming'];
  if ($data[$tick]['put_done'] == NULL)
  	$data_put_done[$tick] = 0;
  else
        $data_put_done[$tick] = $data[$tick]['put_done'];
  if ($data[$tick]['space'] == NULL)
  	$data_space[$tick] = 0;
  else
        $data_space[$tick] = $data[$tick]['space'];
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
//$graph->xaxis->title->Set("Time Span");
//$graph->xaxis->SetTitleMargin(40);
//$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->xgrid->Show();

$plot1 = new LinePlot(array_values($data_incoming));
$plot1->SetColor("black");
$plot1 -> SetLegend("Write Req. (#)");
$graph->Add($plot1);

$plot2 = new LinePlot(array_values($data_put_done));
$plot2->SetColor("teal");
$plot2 -> SetLegend("Write Done Req. (#)");
$graph->AddY(0,$plot2);
$graph->ynaxis[0]->SetColor('teal');

$plot3 = new LinePlot(array_values($data_space));
$plot3->SetColor("purple");
$plot3 -> SetLegend("Prepare to Write Req. (#)");
$graph->AddY(1,$plot3);
$graph->ynaxis[1]->SetColor('purple');

$plot4 = new LinePlot(array_values($data_disp));
$plot4->SetColor("blue");
$plot4 -> SetLegend("Dispatched Req. (#)");
$graph->AddY(2,$plot4);
$graph->ynaxis[2]->SetColor('blue');

$plot5 = new LinePlot(array_values($data_avg_queue));
$plot5->SetColor("red");
$plot5 -> SetLegend("Avg Queue Time (sec)");
$graph->AddY(3,$plot5);
$graph->ynaxis[3]->SetColor('red');

$plot6 = new LinePlot(array_values($data_proc));
$plot6->SetColor("green");
$plot6 -> SetLegend("Processed Write Req. (#)");
$graph->AddY(4,$plot6);
$graph->ynaxis[4]->SetColor('green');

$plot7 = new LinePlot(array_values($data_avg_lat));
$plot7->SetColor("orange");
$plot7 -> SetLegend("Avg Latency (sec)");
$graph->AddY(5,$plot7);
$graph->ynaxis[5]->SetColor('orange');

$graph->legend->SetLayout(LEGEND_HOR);
$graph->legend->SetColumns(4);
$graph->legend->SetLineWeight(4);
$graph->legend->Pos(0.043,0.95,"left","bottom");
$graph->Stroke();
?> 
