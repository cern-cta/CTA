<?php
// 
include ("../lib/_oci_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_bar.php");
include("../lib/no_data.php");
include ("../../../conf/castor-mon-web");
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
$bins = array( 0 =>"<1sec",1 =>"[1-10)sec",2 =>"[10-120)sec",3 =>"[2-5)min",4 =>"[5-10)min",5 =>"[10-30)min",6 =>"[30 -60)min", 7=> "[1-5)hours", 8=> "[5-12)hours", 9=> "[12-24)hours", 10=> "[1-2]days", 11=> ">2days");
if ($period == 10/1440) 
	$graph = new Graph(730,300,"auto",1);
else if ($period == 1/24)
	$graph = new Graph(800,300,"auto",5);
else if ($period == 1)
	$graph = new Graph(800,300,"auto",30);
else if ($period == 7)
	$graph = new Graph(800,300,"auto",60);
else if ($period == 30)
	$graph = new Graph(800,300,"auto",360);
else if ($period == 10000)
	$graph = new Graph(800,300,"auto",360);
else $graph = new Graph(800,300,"auto");

//connection
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
for($i = 0;$i < 12; $i++)
	$diskhitlat[$i] = 0;

if ($qn == 1) {
	$query1 = "select distinct bin, count(bin) over (Partition by bin) reqs 
		from (
		select round(b.totallatency), case when b.totallatency < 1 then 1
		  when b.totallatency >= 1 and b.totallatency < 10 then 2
		  when b.totallatency >= 10 and b.totallatency < 120 then 3
		  when b.totallatency >= 120 and b.totallatency < 300 then 4
		  when b.totallatency >= 300 and b.totallatency < 600 then 5
		  when b.totallatency >= 600 and b.totallatency < 1800 then 6
		  when b.totallatency >= 1800 and b.totallatency < 3600 then 7
		  when b.totallatency >= 3600 and b.totallatency < 18000 then 8
		  when b.totallatency >= 18000 and b.totallatency < 43200 then 9
		  when b.totallatency >= 43200 and b.totallatency < 86400 then 10
		  when b.totallatency >= 86400 and b.totallatency < 172800 then 11
		  else 12 end bin
		from ".$db_instances[$service]['schema']."requests a, ".$db_instances[$service]['schema']."totallatency b
		where a.PK_Requests_subreqid = b.PK_TL_subreqid
		and a.NN_Requests_timestamp > sysdate - $period
		and b.NN_TL_timestamp > sysdate - $period
		and a.state='DiskHit')
		order by bin ";
}
else if ($qn == 2) {
	$query1 = "select distinct bin, count(bin) over (Partition by bin) reqs 
		from (
		select round(b.totallatency), case when b.totallatency < 1 then 1
		  when b.totallatency >= 1 and b.totallatency < 10 then 2
		  when b.totallatency >= 10 and b.totallatency < 120 then 3
		  when b.totallatency >= 120 and b.totallatency < 300 then 4
		  when b.totallatency >= 300 and b.totallatency < 600 then 5
		  when b.totallatency >= 600 and b.totallatency < 1800 then 6
		  when b.totallatency >= 1800 and b.totallatency < 3600 then 7
		  when b.totallatency >= 3600 and b.totallatency < 18000 then 8
		  when b.totallatency >= 18000 and b.totallatency < 43200 then 9
		  when b.totallatency >= 43200 and b.totallatency < 86400 then 10
		  when b.totallatency >= 86400 and b.totallatency < 172800 then 11
		  else 12 end bin
		from ".$db_instances[$service]['schema']."requests a, ".$db_instances[$service]['schema']."totallatency b
		where a.PK_Requests_subreqid = b.PK_TL_subreqid
		and a.NN_Requests_timestamp >= to_date('$from','dd/mm/yyyy HH24:Mi')
		and a.NN_Requests_timestamp <= to_date('$to','dd/mm/yyyy HH24:Mi')
		and b.NN_TL_timestamp >= to_date('$from','dd/mm/yyyy HH24:Mi')
		and b.NN_TL_timestamp <= to_date('$to','dd/mm/yyyy HH24:Mi')
		and a.state='DiskHit')
		order by bin ";
}
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
while (OCIFetch($parsed1)) {
	$bin_num = OCIResult($parsed1,1);
	$diskhitlat[$bin_num - 1] = OCIResult($parsed1,2);
}
for($i = 0;$i < 12; $i++)
	$diskcopylat[$i] = 0;
if ($qn == 1) {
	$query2 = "select distinct bin, count(bin) over (Partition by bin) reqs 
		from (
		select round(b.totallatency), case when b.totallatency < 1 then 1
		  when b.totallatency >= 1 and b.totallatency < 10 then 2
		  when b.totallatency >= 10 and b.totallatency < 120 then 3
		  when b.totallatency >= 120 and b.totallatency < 300 then 4
		  when b.totallatency >= 300 and b.totallatency < 600 then 5
		  when b.totallatency >= 600 and b.totallatency < 1800 then 6
		  when b.totallatency >= 1800 and b.totallatency < 3600 then 7
		  when b.totallatency >= 3600 and b.totallatency < 18000 then 8
		  when b.totallatency >= 18000 and b.totallatency < 43200 then 9
		  when b.totallatency >= 43200 and b.totallatency < 86400 then 10
		  when b.totallatency >= 86400 and b.totallatency < 172800 then 11
		  else 12 end bin
		from ".$db_instances[$service]['schema']."requests a, ".$db_instances[$service]['schema']."totallatency b
		where a.PK_Requests_subreqid = b.PK_TL_subreqid
		and a.NN_Requests_timestamp > sysdate - $period
		and b.NN_TL_timestamp > sysdate - $period
		and a.state='DiskCopy')
		order by bin";
}
else if ($qn == 2) {
	$query2 = "select distinct bin, count(bin) over (Partition by bin) reqs 
		from (
		select round(b.totallatency), case when b.totallatency < 1 then 1
		  when b.totallatency >= 1 and b.totallatency < 10 then 2
		  when b.totallatency >= 10 and b.totallatency < 120 then 3
		  when b.totallatency >= 120 and b.totallatency < 300 then 4
		  when b.totallatency >= 300 and b.totallatency < 600 then 5
		  when b.totallatency >= 600 and b.totallatency < 1800 then 6
		  when b.totallatency >= 1800 and b.totallatency < 3600 then 7
		  when b.totallatency >= 3600 and b.totallatency < 18000 then 8
		  when b.totallatency >= 18000 and b.totallatency < 43200 then 9
		  when b.totallatency >= 43200 and b.totallatency < 86400 then 10
		  when b.totallatency >= 86400 and b.totallatency < 172800 then 11
		  else 12 end bin
		from ".$db_instances[$service]['schema']."requests a, ".$db_instances[$service]['schema']."totallatency b
		where a.PK_Requests_subreqid = b.PK_TL_subreqid
		and a.NN_Requests_timestamp >= to_date('$from','dd/mm/yyyy HH24:Mi')
		and a.NN_Requests_timestamp <= to_date('$to','dd/mm/yyyy HH24:Mi')
		and b.NN_TL_timestamp >= to_date('$from','dd/mm/yyyy HH24:Mi')
		and b.NN_TL_timestamp <= to_date('$to','dd/mm/yyyy HH24:Mi')
		and a.state='DiskCopy')
		order by bin";
}
if (!($parsed2 = OCIParse($conn, $query2))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed2))
	{ echo "Error Executing Query";exit();}
while (OCIFetch($parsed2)) {
	$bin_num = OCIResult($parsed1,1);
	$diskcopylat[$bin_num - 1] = OCIResult($parsed2,2);
}
for($i = 0;$i < 12; $i++)
	$tapelat[$i] = 0;
if ($qn == 1) {
	$query3 = "select distinct bin, count(bin) over (Partition by bin) reqs 
		from (
		select round(b.totallatency), case when b.totallatency < 1 then 1
		  when b.totallatency >= 1 and b.totallatency < 10 then 2
		  when b.totallatency >= 10 and b.totallatency < 120 then 3
		  when b.totallatency >= 120 and b.totallatency < 300 then 4
		  when b.totallatency >= 300 and b.totallatency < 600 then 5
		  when b.totallatency >= 600 and b.totallatency < 1800 then 6
		  when b.totallatency >= 1800 and b.totallatency < 3600 then 7
		  when b.totallatency >= 3600 and b.totallatency < 18000 then 8
		  when b.totallatency >= 18000 and b.totallatency < 43200 then 9
		  when b.totallatency >= 43200 and b.totallatency < 86400 then 10
		  when b.totallatency >= 86400 and b.totallatency < 172800 then 11
		  else 12 end bin
		from ".$db_instances[$service]['schema']."requests a, ".$db_instances[$service]['schema']."totallatency b
		where a.PK_Requests_subreqid = b.PK_TL_subreqid
		and a.NN_Requests_timestamp > sysdate - $period
		and b.NN_TL_timestamp > sysdate - $period
		and a.state='TapeRecall')
		order by bin";
}
else if ($qn == 2){
	$query3 = "select distinct bin, count(bin) over (Partition by bin) reqs 
		from (
		select round(b.totallatency), case when b.totallatency < 1 then 1
		  when b.totallatency >= 1 and b.totallatency < 10 then 2
		  when b.totallatency >= 10 and b.totallatency < 120 then 3
		  when b.totallatency >= 120 and b.totallatency < 300 then 4
		  when b.totallatency >= 300 and b.totallatency < 600 then 5
		  when b.totallatency >= 600 and b.totallatency < 1800 then 6
		  when b.totallatency >= 1800 and b.totallatency < 3600 then 7
		  when b.totallatency >= 3600 and b.totallatency < 18000 then 8
		  when b.totallatency >= 18000 and b.totallatency < 43200 then 9
		  when b.totallatency >= 43200 and b.totallatency < 86400 then 10
		  when b.totallatency >= 86400 and b.totallatency < 172800 then 11
		  else 12 end bin
		from ".$db_instances[$service]['schema']."requests a, ".$db_instances[$service]['schema']."totallatency b
		where a.PK_Requests_subreqid = b.PK_TL_subreqid
		and a.NN_Requests_timestamp >= to_date('$from','dd/mm/yyyy HH24:Mi')
		and a.NN_Requests_timestamp <= to_date('$to','dd/mm/yyyy HH24:Mi')
		and b.NN_TL_timestamp >= to_date('$from','dd/mm/yyyy HH24:Mi')
		and b.NN_TL_timestamp <= to_date('$to','dd/mm/yyyy HH24:Mi')
		and a.state='".$db_instances[$service]['schema']." taperecall')
		order by bin";
}
if (!($parsed3 = OCIParse($conn, $query3))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed3))
	{ echo "Error Executing Query";exit();}
while (OCIFetch($parsed3)) {
	$bin_num = OCIResult($parsed3,1);
	$tapelat[$bin_num - 1] = OCIResult($parsed3,2);	
}

if(empty($diskhitlat) && empty($diskcopylat)&&empty($tapelat)) {
	No_Data_Image();;
	exit();
};

//data processing

//plot
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Request Latency Distribution");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
$graph->yaxis->title->Set("Number of Requests" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);

$graph->xaxis->SetTickLabels($bins);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("time");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($diskhitlat);
$b1->SetWidth(0.33);
$b1->SetFillColor("orangered");
$b1->value->SetFont(FF_FONT1,FS_BOLD,10);
$b1->value->SetAngle(90);
$b1->value->SetColor("orangered");
$b1->value->Show();
$b1->value->SetFormat('%0.0f');
$b1 -> SetLegend("Resident Files");
$b2 = new BarPlot($diskcopylat);
$b2->SetWidth(0.33);
$b2->SetFillColor("darkgoldenrod1");
$b2->value->SetFont(FF_FONT1,FS_BOLD,10);
$b2->value->SetColor("darkgoldenrod1");
$b2->value->SetAngle(90);
$b2->value->Show();
$b2->value->SetFormat('%0.0f');
$b2 -> SetLegend("Pool Copies");
$b3 = new BarPlot($tapelat);
$b3->SetWidth(0.34);
$b3->SetFillColor("maroon");
$b3->value->SetFont(FF_FONT1,FS_BOLD,10);
$b3->value->SetAngle(90);
$b3->value->SetColor("maroon");
$b3->value->Show();
$b3->value->SetFormat('%0.0f');
$b3 -> SetLegend("Tape Recalls");
$gbplot  = new GroupBarPlot (array($b1,$b2,$b3)); 
$graph->Add($gbplot);
$graph->Stroke();
?> 