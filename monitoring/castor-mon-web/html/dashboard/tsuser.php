<?php 
//Requested timeseries for selected user(last ten minutes)
error_reporting(E_ALL ^ E_NOTICE);
include ("../lib/_oci_no_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_bar.php");
include("../lib/no_data.php");
//include user account
include ("../../../conf/castor-mon-web/user.php");
//get posted values
$username = $_GET['user'];
$pattern_1 = '/[a-zA-Z0-9]{1,15}/';
preg_match($pattern_1,$username,$match);
$username = $match[0];
$reqkind = $_GET['reqkind'];
preg_match($pattern_1,$reqkind,$match_1);
$reqkind = $match_1[0];
$service = $_GET['service'];
//Connect to the db
$con = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
   if (!$con) {
     $e = oci_error();
     print htmlentities($e['message']);
     exit;
   } else {// db functionnality
       $time_series =
         "select distinct to_char(trunc(timestamp,'Mi'), 'HH24:MI') bin, 
		   count(case when state='DiskHit' then trunc(timestamp,'Mi') else null end) over (Partition by trunc(timestamp,'Mi')) number_of_DH_req, 
		   count(case when state='DiskCopy' then trunc(timestamp,'Mi') else null end) over (Partition by trunc(timestamp,'Mi')) number_of_DC_req, 
		   count(case when state='TapeRecall' then trunc(timestamp,'Mi') else null end) over (Partition by trunc(timestamp,'Mi')) number_of_TR_req
         from ".$db_instances[$service]['schema'].".requests
         where timestamp >= trunc(sysdate - 15/1440,'Mi')
	   and timestamp < trunc(sysdate - 5/1440,'Mi') 
           and username = :username
         order by bin";
     $parsedqry = ociparse($con, $time_series);
     if (!$parsedqry) { 
        echo "Error Parsing Query <br>";
        exit();
     } else {
       ocibindbyname($parsedqry,":username",$username);
       $qryexec = ociexecute($parsedqry);
       if (!$qryexec) {
       echo "Error Executing Query <br>";
       exit();
       } else {
         $i = 0;
	 while (ocifetch($parsedqry)) {
	   $result['BIN'][$i] = OCIResult($parsedqry,1);
       $result['NUMBER_OF_DH_REQ'][$i] = OCIResult($parsedqry,2);
	   $result['NUMBER_OF_DC_REQ'][$i] = OCIResult($parsedqry,3);
	   $result['NUMBER_OF_TR_REQ'][$i++] = OCIResult($parsedqry,4);
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
$graph->title->Set("$reqkind Timeseries($username)");
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
$graph->xgrid->Show();
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
