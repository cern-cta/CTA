<?php 
error_reporting(E_ALL ^ E_NOTICE);
include ("../lib/_oci_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_bar.php");
include("../lib/no_data.php");
//include user account
include ("../../../conf/castor-mon-web/user.php");
//get posted values
$period = $_GET['period'];
$service = $_GET['service'];
$svcclass = $_GET['svcclass'];
$from = (string)$_GET['from'];
$to = (string)$_GET['to'];
if ($period != NULL) { 
	$qn = 1;
	$pattern = '/[0-9]+([\/][0-9]+)?/';
	preg_match($pattern,$period,$matches);
	$period = $matches[0];
	if($period == NULL) $period = 10/1440;
	$dif = $period;
	
}	
else { 
	$qn = 2;
	$pattern1 = '/[0-3][0-9][\/][0-1][0-9][\/][0-9][0-9][0-9][0-9][\s][0-2][0-9][:][0-5][0-9]/';
	preg_match($pattern1,$from,$matches1);preg_match($pattern1,$to,$matches2);
	$from = $matches1[0]; $to = $matches2[0];
	if (($from ==NULL)or($to == NULL)) {
		echo "<h2>Wrong Arguments</h2>";
	}
	$from1 = explode('/',$from);
	$from_final = $from1[1] . "/" . $from1[0] . "/" . $from1[2];
	$t1 = strtotime($from_final);
	$to1 = explode('/',$to);
	$to_final = $to1[1] . "/" . $to1[0] . "/" . $to1[2];
	$t2 = strtotime($to_final);
	$dif = ($t2-$t1)/86400;
}

$pattern_1 = '/[a-zA-Z0-9]{1,15}/';
preg_match($pattern_1,$svcclass,$match);
$svcclass = $match[0];
if ($svcclass == NULL) 
  $query_svc = 0;
else
  $query_svc = 1;
//selecte appropriate interval and format according to given period
if($dif <= 1/24) {
	$interval = 'Mi';
	$format = 'HH24:MI';
}
else if (($dif > 1/24)&&($dif <= 1)) {
 	$interval = 'HH24';
	$format = 'HH24:Mi';
}
else if (($dif > 1)&&($dif < 10000)) {
	$interval = 'DD';
	$format = 'DD-MON-RR';
}
else {
	$interval = 'WW';
	$format = 'DD-MON-RR';
}
//Create new graph, enable image cache by setting countdown period(in minutes) 
//depending on selected $period. If the cached image is valid the script immediately 
//returns the cached image and exits without logining in the DB
if ($period == '10/1440') {
	$period = 10/1440; 
	$graph = new Graph(420,200,"auto");
}
else if ($period == '1/24') {
	$period = 1/24; 
	$graph = new Graph(420,200,"auto");
}
else if ($period == '1') {
	$period = 1;
	$graph = new Graph(420,200,"auto",10);
}
else if ($period == '7') {
	$period = 7;
	$graph = new Graph(420,200,"auto",30);
}
else if ($period == '30') {
	$period = 30;
	$graph = new Graph(420,200,"auto",360);
}
else 
	$graph = new Graph(420,200,"auto");
//connect to DB	
 $con = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
   if (!$con) {
     $e = ocierror();
     print htmlentities($e['message']);
     exit;
   } else {// db functionnality
     if ($qn == 1)  {
       if ($query_svc == 0)
         $time_series =
         "select distinct to_char(trunc(timestamp,:interval), :format) bin,
	   sum (case when type = 'StageGetRequest' then dispatched else 0 end ) read,
	   sum (case when type = 'StagePutRequest' then dispatched else 0 end ) write
         from ".$db_instances[$service]['schema'].".queuetimestats
         where timestamp >= trunc(sysdate - :period,:interval)
	 group by to_char(trunc(timestamp,:interval), :format)
         order by bin";
	else
	$time_series =
         "select distinct to_char(trunc(timestamp,:interval), :format) bin,
	   sum (case when type = 'StageGetRequest' then dispatched else 0 end ) read,
	   sum (case when type = 'StagePutRequest' then dispatched else 0 end ) write
         from ".$db_instances[$service]['schema'].".queuetimestats
         where timestamp >= trunc(sysdate - :period,:interval)
	 and svcclass = :svcclass
	 group by to_char(trunc(timestamp,:interval), :format)
         order by bin";
     } else if ($qn == 2) {
        if ($query_svc == 0) 
	  $time_series =
         "select distinct to_char(trunc(timestamp,:interval), :format) bin, 
	   sum (case when type = 'StageGetRequest' then dispatched else 0 end ) read,
	   sum (case when type = 'StagePutRequest' then dispatched else 0 end ) write
         from ".$db_instances[$service]['schema'].".queuetimestats
         where timestamp >= trunc(to_date(:from_date,'dd/mm/yyyy HH24:Mi'),:interval)
           and timestamp <= trunc(to_date(:to_date,'dd/mm/yyyy HH24:Mi'),:interval)
         group by to_char(trunc(timestamp,:interval), :format)
	 order by bin";
       else
	 $time_series =
         "select distinct to_char(trunc(timestamp,:interval), :format) bin, 
	   sum (case when type = 'StageGetRequest' then dispatched else 0 end ) read,
	   sum (case when type = 'StagePutRequest' then dispatched else 0 end ) write
         from ".$db_instances[$service]['schema'].".queuetimestats
         where timestamp >= trunc(to_date(:from_date,'dd/mm/yyyy HH24:Mi'),:interval)
         and timestamp <= trunc(to_date(:to_date,'dd/mm/yyyy HH24:Mi'),:interval)
	 and svcclass = :svcclass
         group by to_char(trunc(timestamp,:interval), :format)
	 order by bin";
}
     $parsedqry = ociparse($con, $time_series);
     if (!$parsedqry) { 
        echo "Error Parsing Query <br>";
        exit();
     } else {
	ocibindbyname($parsedqry,":interval",$interval); 
	ocibindbyname($parsedqry,":format",$format);
	if ($qn == 1) {
	  ocibindbyname($parsedqry,":period",$period);
 	}
	else if ($qn == 2) {
	  ocibindbyname($parsedqry,":from_date",$from);
	  ocibindbyname($parsedqry,":to_date",$to);
       }
       if ($query_svc == 1) {
       	  ocibindbyname($parsedqry,":svcclass",$svcclass);
       }
       $qryexec = ociexecute($parsedqry);
       if (!$qryexec) {
       echo "Error Executing Query <br>";
       exit();
       } else {
         $i = 0;
	 while (ocifetch($parsedqry)) {
	   $result['BIN'][$i] = ociresult($parsedqry,1);
	   $number_of_reads = ociresult($parsedqry,2);
	   $number_of_writes = ociresult($parsedqry,3);
           $result['PER_OF_READS'][$i] = 100*round($number_of_reads/($number_of_reads + $number_of_writes),3);
	   $result['PER_OF_WRITES'][$i] = 100*round($number_of_writes/($number_of_reads + $number_of_writes),3);
	   $i++;
	 }
       } 
     }
   }
//add x-axis tick label at the end
 //if ($interval == 'WW') {
//	$intervals[$i] = date("d",strtotime($intervals[$i-1]))+7 ."-" .date("M",strtotime($intervals[$i-1]))."-".date("y",strtotime($intervals[$i-1]));
//	$num = $i;
//}
//if no data retrieved then print out "no data available" image
if(empty($result['BIN'])) {
	No_Data_Image();
	exit();
};
//create new graph
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Scheduler Read/Write Ratio Timeseries");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(80,20,20,120);
$graph->legend->Pos(0.5,0.95,"center","bottom");
$graph->yaxis->title->Set("%");
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(60);
$graph->yaxis->scale->SetAutoMax(100);
if (sizeof($result['BIN'])> 20) {
	for ($i = 0,$j = 0; $i < sizeof($result['BIN']); $i++) {
		if ($i % 5 ==  0) {
			$pos[$j] = $i;
			$tick[$j++] = $result['BIN'][$i];
		}
	}
	$graph->xaxis->SetTickPositions($pos);
	$graph->xaxis->SetTickLabels($tick);
} 
else
	$graph->xaxis->SetTickLabels($result['BIN']);
if ($interval == 'WW') {
	for($i=0;$i<=$num;$i++)
 		$tickPositions[$i] = $i;
 	$graph->xaxis->SetTickPositions($tickPositions);
}
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("Time Span");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->xgrid->Show();
$bplot1 = new BarPlot(array_values($result['PER_OF_READS']));
$bplot2 = new BarPlot(array_values($result['PER_OF_WRITES']));
$bplot1->SetFillColor("green");
$bplot1 -> SetLegend("Read Requests");
$bplot2->SetFillColor("red");
$bplot2-> SetLegend("Write Requests");
$abplot = new AccBarPlot(array($bplot1,$bplot2));
$graph->legend->SetLayout(LEGEND_HOR);
$graph->legend->Pos(0.125,0.95,"left","bottom");
$graph->Add($abplot);
$graph->Stroke();
?> 