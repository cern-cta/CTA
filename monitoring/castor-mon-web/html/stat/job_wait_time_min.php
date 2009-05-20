<?php
//This script plots the maximum distribution bar graph for
//Jobs's Wait Time per Request Type
//Include necessary libraries 
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
//Create new graph, enable image cache by setting countdown period(in maxutes) 
//depending on selected $period. If the cached image is valid the script immediately 
//returns the cached image and exits without logining in the DB
if ($period == '10/1440') {
	$period = 10/1440; 
	$graph = new Graph(700,200,"auto");
}
else if ($period == '1/24') {
	$period = 1/24; 
	$graph = new Graph(700,200,"auto");
}
else if ($period == '1') {
	$period = 1;
	$graph = new Graph(700,200,"auto",10);
}
else if ($period == '7') {
	$period = 7;
	$graph = new Graph(700,200,"auto",30);
}
else if ($period == '30') {
	$period = 30;
	$graph = new Graph(700,200,"auto",360);
}
else 
	$graph = new Graph(700,200,"auto");
//connect to DB	
 $con = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
   if (!$con) {
     $e = ocierror();
     print htmlentities($e['message']);
     exit;
   } else {// db functionnality
     if ($qn == 1) {
       $time_series ="select round(min(mintime),4) min, type
		        from ".$db_instances[$service]['schema'].".latencystats
		       where timestamp > sysdate - :period
		      group by type";
     } else if ($qn == 2) {
       $time_series ="select round(min(mintime),4) min, type
		        from ".$db_instances[$service]['schema'].".latencystats
		       where timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
	               and timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
	  	      group by type";
     } 
     $parsedqry = ociparse($con, $time_series);
     if (!$parsedqry) { 
        echo "Error Parsing Query";
        exit();
     } else {
	if ($qn == 1) {
	  ocibindbyname($parsedqry,":period",$period);
 	}
	else if ($qn == 2) {
	  ocibindbyname($parsedqry,":from_date",$from);
	  ocibindbyname($parsedqry,":to_date",$to);
       }
       $qryexec = ociexecute($parsedqry);
       if (!$qryexec) {
       echo "Error Executing Query";
       exit();
       } else {
         $i = 0;
	 while (ocifetch($parsedqry)) {
	   $result['STAT'][$i] = ociresult($parsedqry,1);
           $result['TYPE'][$i] = ociresult($parsedqry,2);
	   $i++;
	 }
       } 
     }
   }

//if no data retrieved then print out "no data available" image
if(empty($result['STAT'])) {
	No_Data_Image();
	exit();
};
//create new graph
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Maximum Wait Time");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(80,20,20,120);
$graph->legend->Pos(0.5,0.95,"center","bottom");
$graph->yaxis->title->Set("Time(sec)");
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(60);
$graph->xaxis->SetTickLabels($result['TYPE']);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD,3);
if(sizeof($result['TYPE']) <= 3) {
	$graph->xaxis->SetLabelAngle(0);
}
else
	$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->title->Set("Request Type");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->xgrid->Show();
$bplot1 = new BarPlot(array_values($result['STAT']));
$bplot1->SetFillColor("red");
$graph->legend->SetLayout(LEGEND_HOR);
$graph->legend->Pos(0.125,0.95,"left","bottom");
$graph->Add($bplot1);
$graph->Stroke();
?> 