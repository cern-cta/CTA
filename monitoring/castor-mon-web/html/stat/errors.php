<?php
/* Plots Top-Ten Errors for the whole CASTOR Instance
 */ 
//Include user account and necessary libraries
include ("../../../conf/castor-mon-web");
include ("../lib/_oci_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_bar.php");
include("../lib/no_data.php");
//Get posted values of variables
$period = $_GET['period'];
$from = $_GET['from'];
$service = $_GET['service'];
$to = $_GET['to'];
if ($period != NULL) { 
	$qn = 1;
	$pattern = '/[0-9]+([\/][0-9]+)?/';
	preg_match($pattern,$period,$matches);
	$period = $matches[0];
	if($period == NULL) $period = 10/1440;
}	
else { 
	$qn = 2;
	$pattern1 = '/[0-3][0-9][\/][0-1][0-9][\/][0-9][0-9][0-9][0-9][\s][0-2][0-9][:][0-5][0-9]/';
	preg_match($pattern1,$from,$matches1);preg_match($pattern1,$to,$matches2);
	$from = $matches1[0]; $to = $matches2[0];
	if (($from ==NULL)or($to == NULL)) {
		echo "<h2>Wrong Arguments</h2>";
	}
}
//Create new graph, enable image cache by setting countdown period(in minutes) 
//depending on selected $period. If the cached image is valid the script immediately 
//returns the cached image and exits without logining in the DB

if ($period == 10/1440) 
 	$graph = new Graph(800,300,"auto",1);
else if ($period == 1/24)
 	$graph = new Graph(800,300,"auto",5);
else if ($period == 1)
	$graph = new Graph(800,300,"auto",30);
else if ($period == 7)
	$graph = new Graph(800,300,"auto",60);
else if ($period == 30)
	$graph = new Graph(800,300,"auto",360);
else if ($period == 10000) 
	$graph = new Graph(800,400,"auto",360);
else 
	$graph = new Graph(800,400,"auto");

//DB login
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
//query to retrieve top ten CASTOR instance's errors
if ($qn ==1)
	$query1 = "select * from (
		   select fac_name,msg_text,count(*) sum
			   from ".$db_instances[$service]['schema']."errors a,castor_dlf.dlf_msg_texts b, castor_dlf.dlf_facilities c
		   where NN_Errors_timestamp > sysdate - $period
				and b.fac_no = a.facility
				and a.facility = c.fac_no
				and a.msg_no = b.msg_no
		   group by fac_name,msg_text
		   order by sum desc)
		   where rownum < 11 ";
else if ($qn ==2)
	$query1 = "select * from (
	   select fac_name,msg_text,count(*) sum
           from ".$db_instances[$service]['schema']."errors a,castor_dlf.dlf_msg_texts b, castor_dlf.dlf_facilities c
	   where NN_Errors_timestamp >= to_date('$from','dd/mm/yyyy HH24:Mi')
			and NN_Errors_timestamp <= to_date('$to','dd/mm/yyyy HH24:Mi')
            and b.fac_no = a.facility
            and a.facility = c.fac_no
            and a.msg_no = b.msg_no
	   group by fac_name,msg_text
	   order by sum desc)
	   where rownum < 11 ";
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
//Fetch Data in local tables
$i = 0;
while (OCIFetch($parsed1)) {
	$fac = OCIResult($parsed1,1);
	$msg = OCIResult($parsed1,2);
	$msg_text[$i] = $fac."/".$msg;
	$error_num[$i] = OCIResult($parsed1,3);
	$i++;
}
//If no data print "No Data Available" image
if(empty($msg_text)) {
	No_Data_Image();
	exit();
};
//Create new plot
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Top Ten Errors");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,80,40,220);
$graph->yaxis->title->Set("Number of Errors" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);
$graph->xaxis->SetTickLabels($msg_text);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->SetTitleMargin(100);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($error_num);
$b1->SetWidth(0.33);
$b1->SetFillColor("orangered");
$b1->value->SetFont(FF_FONT1,FS_BOLD,10);
$b1->value->Show();
$b1->value->SetFormat('%0.0f');
$graph->Add($b1);
$graph->Stroke();
?>  
