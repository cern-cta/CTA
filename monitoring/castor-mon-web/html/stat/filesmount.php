<?php
/*Recalled Files per Mount Distribution
 *Data directly retrieved from dlf
 */ 
//Include necessary libraries
//jpgraph, user account
include ("../lib/_oci_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_bar.php"); 
include ("../lib/no_data.php");
include ("../../../conf/castor-mon-web/user.php");
//Get posted values
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
//initialization
$bins = array( 0 =>"=1",1 =>"[2-5)",2 =>"[5-10)Kb",3 =>"[10-40)",4 =>"[40-80)",5 =>"[80-120)",6 =>"[120-160)", 7=> "[160-200)",8=> "[200-250)",9=> "[250-300)",10=> "[300-400)",11=> "[400-500)",12=> "[500-1000)",13=> ">=1000");
//Create new graph, enable image cache by setting countdown period(in minutes) 
//depending on selected $period. If the cached image is valid the script immediately 
//returns the cached image and exits without logining in the DB
if ($period == '10/1440') {
	$period = 10/1440; 
	$graph = new Graph(700,200,"auto",1);
}
else if ($period == '1/24') {
	$period = 1/24;
	$graph = new Graph(700,200,"auto",5);
}
else if ($period == '1') {
	$period = 1;
	$graph = new Graph(700,200,"auto",30);
}
else if ($period == '7') {
	$period = 7;
	$graph = new Graph(700,200,"auto",60);
}
else if ($period == '30') {
	$period = 30;
	$graph = new Graph(700,200,"auto",360);
}
else 
	$graph = new Graph(700,200,"auto");

//connection
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
//query to get recalled files per mount distribution
if ($qn ==1)
	$query1 = "select distinct bin, count(bin) over (Partition by bin) reqs 
		from (
		select files, case when files = 1  then 1
		  when files > 1 and files < 5 then 2
		  when files >= 5 and files < 10 then 3
		  when files >= 10 and files < 40 then 4
		  when files >= 40 and files < 80 then 5
		  when files >= 80 and files < 120 then 6
		  when files >= 120 and files < 160 then 7
		  when files >= 160 and files < 200 then 8
		  when files >= 200 and files < 250 then 9
		  when files >= 250 and files < 300 then 10
		  when files >= 300 and files < 400 then 11
		  when files >= 400 and files < 500 then 12
		  when files >= 500 and files < 1000 then 13
		  else 14 end bin
			from (select files from ".$db_instances[$service]['schema'].".taperecalledstats
		  	      where timestamp > sysdate - :period
		  ) )
		order by bin";
else if ($qn == 2)
	$query1 = "select distinct bin, count(bin) over (Partition by bin) reqs 
	from (
	select files, case when files = 1  then 1
	  when files > 1 and files < 5 then 2
	  when files >= 5 and files < 10 then 3
	  when files >= 10 and files < 40 then 4
	  when files >= 40 and files < 80 then 5
	  when files >= 80 and files < 120 then 6
	  when files >= 120 and files < 160 then 7
	  when files >= 160 and files < 200 then 8
	  when files >= 200 and files < 250 then 9
	  when files >= 250 and files < 300 then 10
	  when files >= 300 and files < 400 then 11
	  when files >= 400 and files < 500 then 12
	  when files >= 500 and files < 1000 then 13
	  else 14 end bin
		from (select files from ".$db_instances[$service]['schema'].".taperecalledstats
	  	      where timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
	                and timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
	  ) )
	order by bin";
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
//initialize table
for($i = 0;$i < 13; $i++)
	$files[$i] = 0;
//fetch data in local table
while (OCIFetch($parsed1)) {
	$bin_num = OCIResult($parsed1,1);
	$files[$bin_num - 1] = OCIResult($parsed1,2);
}

if(empty($bin_num)) {
	No_Data_Image();
	exit();
};

//create graph
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Files Recalled per Mount - Distribution");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
$graph->yaxis->title->Set("Times" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);

$graph->xaxis->SetTickLabels($bins);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("Number of Files");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($files);
$b1->SetFillColor("orangered");
$b1->value->Show();
$b1->value->SetFormat('%0.0f');
$graph->Add($b1);
$graph->Stroke();
?> 
