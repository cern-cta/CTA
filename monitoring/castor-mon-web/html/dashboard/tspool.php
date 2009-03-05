<?php
//Requested files timeseries for selected service class(last ten minutes) 
error_reporting(E_ALL ^ E_NOTICE);
include ("../lib/_oci_no_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_bar.php");
include("../lib/no_data.php");
//include user account
include ("../../../conf/castor-mon-web");
//get posted values 
$svcclass = $_GET['svcclass'];
$pattern_1 = '/[a-zA-Z0-9]{1,15}/';
preg_match($pattern_1,$svcclass,$match);
$svcclass = $match[0];
if($svcclass == NULL) $svcclass = 'default';
$reqkind = $_GET['reqkind'];
preg_match($pattern_1,$reqkind,$match_1);
$reqkind = $match_1[0];
//define interval and format
$interval = 'Mi';
$format = 'HH24:MI';
$service = $_GET['service'];

$con = oci_connect($db_instances[$service]['username'],$db_instances[$service]['pass'],
       $db_instances[$service]['serv']);
   if (!$con) {
     $e = oci_error();
     print htmlentities($e['message']);
     exit;
   } else {// db functionnality
       $time_series =
         "select distinct to_char(trunc(NN_Requests_timestamp,'$interval'), '$format') bin, 
		   count(case when state='DiskHit' then trunc(NN_Requests_timestamp,'$interval') else null end) over (Partition by trunc(NN_Requests_timestamp,'$interval')) number_of_DH_req, 
		   count(case when state='DiskCopy' then trunc(NN_Requests_timestamp,'$interval') else null end) over (Partition by trunc(NN_Requests_timestamp,'$interval')) number_of_DC_req, 
		   count(case when state='TapeRecall' then trunc(NN_Requests_timestamp,'$interval') else null end) over (Partition by trunc(NN_Requests_timestamp,'$interval')) number_of_TR_req
         from ".$db_instances[$service]['schema']."requests
         where NN_Requests_timestamp >= trunc(sysdate -15/1440,'$interval')
	   and NN_Requests_timestamp < trunc(sysdate - 5/1440,'$interval') 
           and svcclass = '$svcclass'
         order by bin";
          $parsedqry = oci_parse($con, $time_series);
     if (!$parsedqry) { 
        echo "Error Parsing Query <br>";
        exit();
     } else {
       $qryexec = oci_execute($parsedqry);
       if (!$qryexec) {
       echo "Error Executing Query <br>";
       exit();
       } else {
         $i = 0;
	 while ($row = oci_fetch_array($parsedqry, OCI_ASSOC)) {
	   $result['BIN'][$i] = $row['BIN'];
       $result['NUMBER_OF_DH_REQ'][$i] = $row['NUMBER_OF_DH_REQ'];
	   $result['NUMBER_OF_DC_REQ'][$i] = $row['NUMBER_OF_DC_REQ'];
	   $result['NUMBER_OF_TR_REQ'][$i++] = $row['NUMBER_OF_TR_REQ'];
	 }
       } 
     }
   }

//if no data retrieved then print out "no available data image"
if(empty($result['BIN'])) {
	No_Data_Image();
	exit();
};
//plot data
$graph = new Graph(420,200,"auto");
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("$reqkind Timeseries($svcclass)");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(80,20,20,120);
$graph->yaxis->title->Set("Requests");
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(60);
$graph->legend->Pos(0.5,0.95,"center","bottom");
$graph->xaxis->SetTickLabels($result['BIN']);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("Time Span");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$bplot1 = new BarPlot(array_values($result['NUMBER_OF_DH_REQ']));
$bplot2 = new BarPlot(array_values($result['NUMBER_OF_DC_REQ']));
$bplot3 = new BarPlot(array_values($result['NUMBER_OF_TR_REQ']));
$bplot1->SetFillColor("green");
$bplot1 -> SetLegend("DiskHits");
$bplot2->SetFillColor("orange");
$bplot2 -> SetLegend("DiskCopies");
$bplot3->SetFillColor("red");
$bplot3 -> SetLegend("TapeRecall");
$abplot = new AccBarPlot(array($bplot1,$bplot2,$bplot3));
$graph->legend->SetLayout(LEGEND_HOR);
$graph->legend->Pos(0.125,0.95,"left","bottom");
$graph->Add($abplot);
$graph->Stroke();
?> 