<?php 
//File Migration latencies distribution
//Include necessary libraries
include ("../lib/_oci_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_bar.php");
include("../lib/no_data.php");
//Include user account
include ("../../../conf/castor-mon-web/user.php");
//Get posted values
$period = $_GET['period'];
$svcclass = $_GET['svcclass'];
$from = $_GET['from'];
$to = $_GET['to'];
$service = $_GET['service'];
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

//initialization 
$bins = array( 0 =>"<1sec",1 =>"[1-2)sec",2 =>"[2-4)sec",3 =>"[4-6)sec",4 =>"[6-8)sec",5 =>"[8-10)sec",6 =>"[10-120)sec",7 =>"[2-5)min",8 =>"[5-10)min",9 =>"[10-30)min",10 =>"[30 -60)min", 11=> ">=1 hour");
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
//connection
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
//initialize table
for($i = 0;$i < 12; $i++)
	$lat[$i] = 0;
//Distribution query
if ($qn == 1) {
  if ($query_svc == 0)
	$query1 = "select distinct bin, count(bin) over (Partition by bin) migs 
		from (
		select round(b.totallatency), case when b.totallatency < 1 then 1
		  when b.totallatency >= 1 and b.totallatency < 2 then 2
		  when b.totallatency >= 2 and b.totallatency < 4 then 3
		  when b.totallatency >= 4 and b.totallatency < 6 then 4
		  when b.totallatency >= 6 and b.totallatency < 8 then 5
		  when b.totallatency >= 8 and b.totallatency < 10 then 6
		  when b.totallatency >= 10 and b.totallatency < 120 then 7
		  when b.totallatency >= 120 and b.totallatency < 300 then 8
		  when b.totallatency >= 300 and b.totallatency < 600 then 9
		  when b.totallatency >= 600 and b.totallatency < 1800 then 10
		  when b.totallatency >= 1800 and b.totallatency < 3600 then 11
		  else 12 end bin
		from ".$db_instances[$service]['schema'].".migration a, ".$db_instances[$service]['schema'].".totallatency b
		where a.subreqid = b.subreqid
		and a.timestamp > sysdate - :period
		and b.timestamp > sysdate - :period)
		order by bin ";
  else  $query1 = "select distinct bin, count(bin) over (Partition by bin) migs 
		from (
		select round(b.totallatency), case when b.totallatency < 1 then 1
		  when b.totallatency >= 1 and b.totallatency < 2 then 2
		  when b.totallatency >= 2 and b.totallatency < 4 then 3
		  when b.totallatency >= 4 and b.totallatency < 6 then 4
		  when b.totallatency >= 6 and b.totallatency < 8 then 5
		  when b.totallatency >= 8 and b.totallatency < 10 then 6
		  when b.totallatency >= 10 and b.totallatency < 120 then 7
		  when b.totallatency >= 120 and b.totallatency < 300 then 8
		  when b.totallatency >= 300 and b.totallatency < 600 then 9
		  when b.totallatency >= 600 and b.totallatency < 1800 then 10
		  when b.totallatency >= 1800 and b.totallatency < 3600 then 11
		  else 12 end bin
		from ".$db_instances[$service]['schema'].".migration a, ".$db_instances[$service]['schema'].".totallatency b
		where a.subreqid = b.subreqid
		and a.timestamp > sysdate - :period
		and b.timestamp > sysdate - :period
		and svcclass = :svcclass )
		order by bin ";
} else if ($qn == 2) {
  if ($query_svc == 0)
	$query1 = "select distinct bin, count(bin) over (Partition by bin) migs 
		from (
		select round(b.totallatency), case when b.totallatency < 1 then 1
		  when b.totallatency >= 1 and b.totallatency < 2 then 2
		  when b.totallatency >= 2 and b.totallatency < 4 then 3
		  when b.totallatency >= 4 and b.totallatency < 6 then 4
		  when b.totallatency >= 6 and b.totallatency < 8 then 5
		  when b.totallatency >= 8 and b.totallatency < 10 then 6
		  when b.totallatency >= 10 and b.totallatency < 120 then 7
		  when b.totallatency >= 120 and b.totallatency < 300 then 8
		  when b.totallatency >= 300 and b.totallatency < 600 then 9
		  when b.totallatency >= 600 and b.totallatency < 1800 then 10
		  when b.totallatency >= 1800 and b.totallatency < 3600 then 11
		  else 12 end bin
		from ".$db_instances[$service]['schema'].".migration a, ".$db_instances[$service]['schema'].".totallatency b
		where a.subreqid = b.subreqid
		and a.timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
		and a.timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
		and b.timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
		and b.timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi'))
		order by bin ";
  else  
	$query1 = "select distinct bin, count(bin) over (Partition by bin) migs 
		from (
		select round(b.totallatency), case when b.totallatency < 1 then 1
		  when b.totallatency >= 1 and b.totallatency < 2 then 2
		  when b.totallatency >= 2 and b.totallatency < 4 then 3
		  when b.totallatency >= 4 and b.totallatency < 6 then 4
		  when b.totallatency >= 6 and b.totallatency < 8 then 5
		  when b.totallatency >= 8 and b.totallatency < 10 then 6
		  when b.totallatency >= 10 and b.totallatency < 120 then 7
		  when b.totallatency >= 120 and b.totallatency < 300 then 8
		  when b.totallatency >= 300 and b.totallatency < 600 then 9
		  when b.totallatency >= 600 and b.totallatency < 1800 then 10
		  when b.totallatency >= 1800 and b.totallatency < 3600 then 11
		  else 12 end bin
		from ".$db_instances[$service]['schema'].".migration a, ".$db_instances[$service]['schema'].".totallatency b
		where a.subreqid = b.subreqid
		and a.timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
		and a.timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
		and b.timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
		and b.timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
		and svcclass = :svcclass)
		order by bin "; 
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

if ($query_svc == 1) {
       	  ocibindbyname($parsed1,":svcclass",$svcclass);
}

if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
//fetch data into local tables
while (OCIFetch($parsed1)) {
	$bin_num = OCIResult($parsed1,1);
	$lat[$bin_num - 1] = OCIResult($parsed1,2);
}
//if no data retrieved display "No Data Image"
if(empty($lat)) {
	No_Data_Image();;
	exit();
};
//plot
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Migration Latency Distribution");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
$graph->yaxis->title->Set("Number of Files Migrated" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);

$graph->xaxis->SetTickLabels($bins);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("time");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($lat);
$b1->SetWidth(0.33);
$b1->SetFillColor("orangered");
$b1->value->SetFont(FF_FONT1,FS_BOLD,10);
$b1->value->Show();
$b1->value->SetFormat('%0.0f');
$graph->Add($b1);
$graph->Stroke();
?> 