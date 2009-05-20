<?php
//Number of Migrated Files Distribution per Service Class
//Include necessary libraries
include ("../lib/_oci_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_bar.php");
include("../lib/no_data.php");
//User account
include ("../../../conf/castor-mon-web/user.php");
//Get posted data
$period = $_GET['period'];
$service = $_GET['service'];
$from = $_GET['from'];
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
if ($period == '10/1440') {
	$period = 10/1440;
	$graph = new Graph(700,300,"auto",1);
}
else if ($period == '1/24') {
	$period = 1/24;
	$graph = new Graph(700,300,"auto",5);
}
else if ($period == '1') {
	$period = 1;
	$graph = new Graph(700,300,"auto",30);
}
else if ($period == '7') {
	$period = 7;
	$graph = new Graph(700,300,"auto",60);
}
else if ($period == '30') {
	$period = 30;
	$graph = new Graph(700,300,"auto",360);
}
else $graph = new Graph(700,300,"auto");
//connection -- DB login 
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
if ($qn == 1)
	$query1 = "select svcclass, count(*) migs
		   from ".$db_instances[$service]['schema'].".migration
		   where timestamp > sysdate - :period
		   group by svcclass
		   order by migs desc";
else if ($qn == 2)
	$query1 = "select svcclass, count(*) migs
		   from ".$db_instances[$service]['schema'].".migration
		   where timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
			and timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
		   group by svcclass
		   order by migs desc";
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if ($qn == 1) {
	ocibindbyname($parsed1,":period",$period);
}
else if ($qn == 2) {
	ocibindbyname($parsed1,":from_date",$from);
	ocibindbyname($parsed1,":to_date",$to);
}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
//fetch data into local tables
$i = 0;
while (OCIFetch($parsed1)) {
	$pool_names[$i] = OCIResult($parsed1,1);
	$num_migs[$i] = OCIResult($parsed1,2);
	$i++;		
}
//If no data retrieved then display "no data available" image
if(empty($pool_names)) {
	No_Data_Image();
	exit();
};
//Create new plot
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Files Migrated per SvcClass");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);
$graph->yaxis->SetFont(FF_FONT1,FS_BOLD,8);
$graph->xaxis->SetTickLabels($pool_names);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD,8);
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($num_migs);
$b1->SetFillColor("orangered");
$b1->value->Show(); 
$b1->value->SetFormat('%0.0f');
$graph->Add($b1);
$graph->Stroke();

?> 