<?php
//".$db_instances[$service]['schema']." requests distribution per service class
include ("../lib/_oci_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_bar.php");
include("../lib/no_data.php");
//include user account
include ("../../../conf/castor-mon-web/user.php");
//get posted data
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
if ($period == "10/1440") {
	$period = 10/1440; 
	$graph = new Graph(700,250,"auto",1);
}
else if ($period == "1/24") {
	$period = 1/24; 
	$graph = new Graph(700,250,"auto",5);
}
else if ($period == '1') {
	$period = 1;
	$graph = new Graph(700,250,"auto",30);
}
else if ($period == '7') {
	$period = 7;
	$graph = new Graph(700,250,"auto",60);
}
else if ($period == '30') {
	$period = 30;
	$graph = new Graph(700,250,"auto",360);
}
else 
	$graph = new Graph(700,250,"auto");
//connection
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
if ($qn==1) {
	$query1 = "select distinct svcclass, state, count(*) over (Partition by svcclass,state) reqs
		   from ".$db_instances[$service]['schema'].".requests
		   where timestamp > sysdate - :period
		   order by svcclass";
}
else if ($qn==2){
	$query1 = "select distinct svcclass, state, count(*) over (Partition by svcclass,state) reqs
		   from ".$db_instances[$service]['schema'].".requests
		   where timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
			and timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
		   order by svcclass";
}

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
//initialize variables
$pre_pool = 'foo';
$i = 0;
//fetch and process data 
while (OCIFetch($parsed1)) {
	$pool = OCIResult($parsed1,1);
	if($pool != $pre_pool) {
		$pool_names[$i] = $pool;
		$pre_pool = $pool;
		$i++;
	}
	$state = OCIResult($parsed1,2);
	if ($state == 'DiskHit')
		$diskhits[$pool] = OCIResult($parsed1,3);
	else if ($state == 'DiskCopy')
		$diskcopies[$pool] = OCIResult($parsed1,3);
	else if ($state == 'TapeRecall')
		$taperecalls[$pool] = OCIResult($parsed1,3);
}
if(empty($pool_names)) {
	No_Data_Image();
	exit();
};
//data final processing
$i = 0;
foreach ($pool_names as $pn) {
	if( $diskhits[$pn] != NULL )
		$dh[$i] = $diskhits[$pn];
	else $dh[$i] = 0;
	if( $diskcopies[$pn] != NULL )
		$dc[$i] = $diskcopies[$pn];
	else $dc[$i] = 0;
	if( $taperecalls[$pn] != NULL )
		$tr[$i] = $taperecalls[$pn];
	else $tr[$i] = 0;
	$i++;
}

//create bar graph
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("File Read Requests per SvcClass");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
// $graph->yaxis->title->Set("Number of ".$db_instances[$service]['schema']." requests" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);
$graph->yaxis->SetFont(FF_FONT1,FS_BOLD,8);
$graph->xaxis->SetTickLabels($pool_names);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD,8);
// $graph->xaxis->title->Set("POOL");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($dh);
$b1->SetFillColor("orangered");
$b1 -> SetLegend("Resident Files");
$b2 = new BarPlot($dc);
$b2->SetFillColor("maroon");
$b2 -> SetLegend("D2D Copies");
$b3 = new BarPlot($tr);
$b3->SetFillColor("darkgoldenrod1");
$b3 -> SetLegend("Tape Recalls");
$gbplot = new AccBarPlot(array($b1,$b2,$b3));
$gbplot->value->Show(); 
$graph->legend->SetLayout(LEGEND_HOR);
$graph->legend->Pos(0.5,0.95,"left","bottom");
$graph->Add($gbplot);
$graph->Stroke();

?> 
