<?php
//Top ten tape volumes used in the whole CASTOR instance
include ("../lib/_oci_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_bar.php");
include("../lib/whiteimage.php");
//include user account
include ("../../../conf/castor-mon-web");
//get posted values
$period = $_GET['period'];
$service = $_GET['service'];
$from = $_GET['from'];
$to = $_GET['to'];
$p = $_GET['p'];
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
$pattern_1 = '/[a-zA-Z0-9]{1,15}/';
preg_match($pattern_1,$p,$match);
$p = $match[0];
if($p == NULL) $p = 'default';
//Create new graph, enable image cache by setting countdown period(in minutes) 
//depending on selected $period. If the cached image is valid the script immediately 
//returns the cached image and exits without logining in the DB
if ($period == 10/1440) 
	$graph = new Graph(700,300,"auto",1);
else if ($period == 1/24)
	$graph = new Graph(700,300,"auto",5);
else if ($period == 1)
	$graph = new Graph(700,300,"auto",30);
else if ($period == 7)
	$graph = new Graph(700,300,"auto",60);
else if ($period == 30)
	$graph = new Graph(700,300,"auto",360);
else if ($period == 10000)
	$graph = new Graph(700,300,"auto",360);
else
	$graph = new Graph(700,300,"auto");
//connection
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
if ($qn == 1)
	$query1 = "select * from (
				select tapeid,count(tapeid) mounts
				from ".$db_instances[$service]['schema']."requests a, ".$db_instances[$service]['schema']."taperecall b
				where a.PK_Requests_subreqid = b.PK_TR_subreqid
			and a.NN_Requests_timestamp >= sysdate - $period
			and b.NN_TR_timestamp >= sysdate - $period
			and a.svcclass = '$p'
				group by tapeid 
				order by mounts desc )
		   where rownum < 11";
else if ($qn ==2)
	$query1 = "select * from (
      		select tapeid,count(tapeid) mounts
      		from ".$db_instances[$service]['schema']."requests a, ".$db_instances[$service]['schema']."taperecall b
      		where a.PK_TR_subreqid = b.PK_TR_subreqid
			and a.NN_Requests_timestamp >= to_date('$from','dd/mm/yyyy HH24:Mi')
			and a.NN_Requests_timestamp <= to_date('$to','dd/mm/yyyy HH24:Mi')
			and b.NN_TR_timestamp >= to_date('$from','dd/mm/yyyy HH24:Mi')
			and b.NN_TR_timestamp <= to_date('$to','dd/mm/yyyy HH24:Mi')
			and a.svcclass = '$p'
      		group by tapeid 
      		order by mounts desc )
	   where rownum < 11";
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$i = 0;
//fetch data into local tables
while (OCIFetch($parsed1)) {
	$tapeid[$i] = OCIResult($parsed1,1);
	$mounts[$i] = OCIResult($parsed1,2);
	$i++;		
}
//if no data retrieved print out "no data available" image
if(empty($tapeid)) {
	No_Data_Image();
	exit();
};
//create new graph
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Top Ten Tapes(POOL: ".$p.")");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
$graph->yaxis->title->Set("Number of Files Requested" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);

$graph->xaxis->SetTickLabels($tapeid);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("Tapeid");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($mounts);
$b1->SetFillColor("tomato");
$b1->value->Show();
$b1->value->SetFormat('%0.0f');
$graph->Add($b1);
$graph->Stroke();
?> 
	   
	   