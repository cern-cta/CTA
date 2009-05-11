<?php 
//Number of Migrated Files timeseries
include ("../lib/_oci_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_line.php");
include("../lib/no_data.php");
//User account
include ("../../../conf/castor-mon-web/user.php");
//get posted data
$period = $_GET['period'];
$from = (string)$_GET['from'];
$service = $_GET['service'];
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
//Define the correct interval and format in accordance with the selected period
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
	$interval = 'Mi';
	$format = 'HH24:MI';
	$graph = new Graph(420,200,"auto");
}
else if ($period == '1/24') {
	$period = 1/24; 
	$interval = 'Mi';
	$format = 'HH24:MI';
	$graph = new Graph(420,200,"auto");
}
else if ($period == '1') {
	$period = 1;
	$interval = 'HH24';
	$format = 'HH24:MI';
	$graph = new Graph(420,200,"auto",10);
}
else if ($period == '7') {
	$period = 7;
	$interval = 'DD';
	$format = 'DD-MON-RR';
	$graph = new Graph(420,200,"auto",30);
}
else if ($period == '30') {
	$period = 30;
	$interval = 'DD';
	$format = 'DD-MON-RR';
	$graph = new Graph(420,200,"auto",360);
}
else
	$graph = new Graph(420,200,"auto");
//connection -- DB login
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
if ($qn ==1)
	$query1 = "select to_char(bin,:format) , number_of_mig from (
		   select distinct trunc(timestamp,:interval) bin, count(trunc(timestamp,:interval)) 
		   over (Partition by trunc(timestamp,:interval)) number_of_mig  
			   from ".$db_instances[$service]['schema'].".migration
			   where timestamp > trunc(sysdate -:period,:interval)
			   order by bin )";
else if ($qn ==2)
	$query1 = "select to_char(bin,:format) , number_of_mig from (
		   select distinct trunc(timestamp,:interval) bin, count(trunc(timestamp,:interval)) 
		   over (Partition by trunc(timestamp,:interval)) number_of_mig  
			   from ".$db_instances[$service]['schema'].".migration
			   where timestamp >= trunc(to_date(:from_date,'dd/mm/yyyy HH24:Mi'),:interval)
				and timestamp <= trunc(to_date(:to_date,'dd/mm/yyyy HH24:Mi'),:interval)
			   order by bin )";
	   
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if ($qn == 1) {
	ocibindbyname($parsed1,":period",$period);
	ocibindbyname($parsed1,":interval",$interval);
	ocibindbyname($parsed1,":format",$format);
}
else if ($qn == 2) {
	ocibindbyname($parsed1,":from_date",$from);
	ocibindbyname($parsed1,":to_date",$to);
	ocibindbyname($parsed1,":interval",$interval);
	ocibindbyname($parsed1,":format",$format);
}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
//Fetch data into local tables
$i = 0;
while (OCIFetch($parsed1)) {
	$intervals[$i] =  OCIResult($parsed1,1);
	$number[$i] = OCIResult($parsed1,2);
	$i++;		
}
//Add extra x-axis label at the end
 if ($interval == 'WW') {
	$intervals[$i] = date("d",strtotime($intervals[$i-1]))+7 ."-" .date("M",strtotime($intervals[$i-1]))."-".date("y",strtotime($intervals[$i-1]));
	$num = $i;
}
//If no data fetched display no data available image
if(empty($intervals)) {
	No_Data_Image();
	exit();
};
//create new plot
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Files Migrated Timeseries");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(80,20,20,120);
$graph->yaxis->title->Set("Files");
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(60);
if (sizeof($intervals)> 20) {
	for ($i = 0,$j = 0; $i < sizeof($intervals); $i++) {
		if ($i % 5 ==  0) {
			$pos[$j] = $i;
			$tick[$j++] = $intervals[$i];
		}
	}
	$graph->xaxis->SetTickPositions($pos);
	$graph->xaxis->SetTickLabels($tick);
} 
else
	$graph->xaxis->SetTickLabels($intervals);
if ($interval == 'WW') {
	for($i=0;$i<=$num;$i++)
 		$tickPositions[$i] = $i;
 	$graph->xaxis->SetTickPositions($tickPositions);
}
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("time");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new LinePlot($number);
$b1->SetColor("red");
$graph->Add($b1);
$graph->Stroke();
?> 