<?php
//This script plot a distribution bar graph of
//GC files' size
//Include necessary libraries
include ("../lib/_oci_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_bar.php"); 
include ("../lib/no_data.php");
//Include User Account
include ("../../../conf/castor-mon-web/user.php");
//Define Bins
$bins = array( 0 =>"<10sec",1 =>"[10-60)sec",2 =>"[1-15)min",3 =>"[15-30)min",4 =>"[30-60)min",5 =>"[1-6)hours",6 =>"[6 -12)hours", 7=> "[12-24)hours", 8=> "[1-2)days", 9=> "[2-4)days", 10=> "[4-8)days",11=> "[8-16)days", 12=> ">=16days");
$period = $_GET['period'];
$service = $_GET['service'];
$svcclass = $_GET['svcclass'];
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

$pattern_1 = '/[a-zA-Z0-9]{1,15}/';
preg_match($pattern_1,$svcclass,$match);
$svcclass = $match[0];
if ($svcclass == NULL) 
  $query_svc = 0;
else
  $query_svc = 1;


//Create new graph, enable image cache by setting countdown period(in minutes) 
//depending on selected $period. If the cached image is valid the script immediately 
//returns the cached image and exits without logining in the DB
if ($period == '10/1440') {
	$period = 10/1440; 
	$graph = new Graph(700,250,"auto",1);
}
else if ($period == '1/24') {
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
else $graph = new Graph(700,250,"auto");
//Connect - DB Login
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
  if (!$conn) {
   $e = oci_error();
   print htmlentities($e['message']);
   exit;
  }
//Retrive File Size distribution for Garbage Collected Files
if ($qn ==1) {
  if ($query_svc == 0)
	$query1 = "select distinct bin, count(bin) over (Partition by bin) reqs 
		from (
		select round(fileage), case when fileage < 10 then 'bin1'
		  when fileage >= 10 and fileage < 60 then 'bin2'
		  when fileage >= 60 and fileage < 900 then 'bin3'
		  when fileage >= 900 and fileage < 1800 then 'bin4'
		  when fileage >= 1800 and fileage < 3600 then 'bin5'
		  when fileage >= 3600 and fileage < 21600 then 'bin6'
		  when fileage >= 21600 and fileage < 43200 then 'bin7'
		  when fileage >= 43200 and fileage < 86400 then 'bin8'
		  when fileage >= 86400 and fileage < 172800 then 'bin9'
		  when fileage >= 172800 and fileage < 345600 then 'binn10'
		  when fileage >= 345600 and fileage < 691200 then 'binn11'
		when fileage >= 691200 and fileage < 1382400 then 'binn12'
		  else 'binn13' end bin
		from ".$db_instances[$service]['schema'].".gcfiles
		where fileage is not null
		and timestamp > sysdate - :period)
		order by bin ";
  else	$query1 = "select distinct bin, count(bin) over (Partition by bin) reqs 
		from (
		select round(fileage), case when fileage < 10 then 'bin1'
		  when fileage >= 10 and fileage < 60 then 'bin2'
		  when fileage >= 60 and fileage < 900 then 'bin3'
		  when fileage >= 900 and fileage < 1800 then 'bin4'
		  when fileage >= 1800 and fileage < 3600 then 'bin5'
		  when fileage >= 3600 and fileage < 21600 then 'bin6'
		  when fileage >= 21600 and fileage < 43200 then 'bin7'
		  when fileage >= 43200 and fileage < 86400 then 'bin8'
		  when fileage >= 86400 and fileage < 172800 then 'bin9'
		  when fileage >= 172800 and fileage < 345600 then 'binn10'
		  when fileage >= 345600 and fileage < 691200 then 'binn11'
		when fileage >= 691200 and fileage < 1382400 then 'binn12'
		  else 'binn13' end bin
		from ".$db_instances[$service]['schema'].".gcfiles
		where fileage is not null
		and timestamp > sysdate - :period
		and svcclass = :svcclass)
		order by bin ";	
} else if ($qn ==2) {
  if ($query_svc == 0)
	$query1 = "select distinct bin, count(bin) over (Partition by bin) reqs 
	from (
	select round(fileage), case when fileage < 10 then 'bin1'
	  when fileage >= 10 and fileage < 60 then 'bin2'
	  when fileage >= 60 and fileage < 900 then 'bin3'
	  when fileage >= 900 and fileage < 1800 then 'bin4'
	  when fileage >= 1800 and fileage < 3600 then 'bin5'
	  when fileage >= 3600 and fileage < 21600 then 'bin6'
	  when fileage >= 21600 and fileage < 43200 then 'bin7'
	  when fileage >= 43200 and fileage < 86400 then 'bin8'
	  when fileage >= 86400 and fileage < 172800 then 'bin9'
	  when fileage >= 172800 and fileage < 345600 then 'binn10'
	  when fileage >= 345600 and fileage < 691200 then 'binn11'
    when fileage >= 691200 and fileage < 1382400 then 'binn12'
	  else 'binn13' end bin
	from ".$db_instances[$service]['schema'].".gcfiles
	where fileage is not null
	and timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
	and timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi'))
	order by bin ";
  else $query1 = "select distinct bin, count(bin) over (Partition by bin) reqs 
	from (
	select round(fileage), case when fileage < 10 then 'bin1'
	  when fileage >= 10 and fileage < 60 then 'bin2'
	  when fileage >= 60 and fileage < 900 then 'bin3'
	  when fileage >= 900 and fileage < 1800 then 'bin4'
	  when fileage >= 1800 and fileage < 3600 then 'bin5'
	  when fileage >= 3600 and fileage < 21600 then 'bin6'
	  when fileage >= 21600 and fileage < 43200 then 'bin7'
	  when fileage >= 43200 and fileage < 86400 then 'bin8'
	  when fileage >= 86400 and fileage < 172800 then 'bin9'
	  when fileage >= 172800 and fileage < 345600 then 'binn10'
	  when fileage >= 345600 and fileage < 691200 then 'binn11'
    when fileage >= 691200 and fileage < 1382400 then 'binn12'
	  else 'binn13' end bin
	from ".$db_instances[$service]['schema'].".gcfiles
	where fileage is not null
	and timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
	and timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
	and svcclass = :svcclass)
	order by bin ";	
}

$parsed1 = OCIParse($conn, $query1);
if ($qn == 1) {
	ocibindbyname($parsed1,":period",$period);
}
else if ($qn == 2) {
	ocibindbyname($parsed1,":from_date",$from);
	ocibindbyname($parsed1,":to_date",$to);
}
if ($query_svc == 1) {
       	  ocibindbyname($parsed1,":svcclass",$svcclass);
}

OCIExecute($parsed1);
$j=0;
//Fetch Data in local tables
while ( OCIFetch($parsed1) ) {
  $fileage[$j] = OCIResult($parsed1, 2);
  $j++;
}
//If no data retrieved display "No Data Available" Image
if(empty($fileage)) {
	No_Data_Image();
	exit();
};

//Plot 
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("FileAge Distribution of Garbage Collected Files");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
$graph->yaxis->title->Set("Number of Files" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);

$graph->xaxis->SetTickLabels($bins);
$graph->xaxis->SetLabelAngle(0);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("Age");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($fileage);
$b1->SetFillColor("red");
$b1->value->Show();
$b1->value->SetFormat('%0.00f');
$graph->Add($b1);
$graph->Stroke();
?>
