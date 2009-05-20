<?php 
//Time distribuation. Time interval between Garbage Collection and Request of the same file
//Include necessary libraries
include ("../lib/_oci_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_bar.php"); 
include ("../lib/no_data.php");
include ("../../../conf/castor-mon-web/user.php");

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
//initialization
$bins = array( 0 =>"<0.1 h",1 =>"[0.1-0.5) h",2 =>"[0.5-1) h",3 =>"[1-2) h",4 =>"[2-4) h",5 =>"[4-6) h",6 =>"[6-8] h", 7=> "(8-24) h");
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
	$period == 7;
	$graph = new Graph(700,250,"auto",60);
}
else if ($period == '30') {
	$period = 30;
	$graph = new Graph(700,250,"auto",360);
}
else $graph = new Graph(700,250,"auto");
//connection
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
//Retrieve Data by quering materialized view req_del
if($qn ==1) {
  if ($query_svc == 0)
	$query1 = "select distinct bin, count(bin) over (Partition by bin) reqs
			from (
					select dif , case when dif < 0.1 then 1
				when dif >= 0.1 and dif < 0.5 then 2
				when dif >= 0.5 and dif < 1 then 3
				when dif >= 1 and dif < 2 then 4
				when dif >= 2 and dif < 4 then 5
				when dif >= 4 and dif < 6 then 6
				when dif >= 6 and dif <= 8 then 7
					else 8 end bin 
			  from ".$db_instances[$service]['schema'].".ReqDel_MV
			  where timestamp > sysdate - :period)
			order by bin";
  else  $query1 = "select distinct bin, count(bin) over (Partition by bin) reqs
			from (
					select dif , case when dif < 0.1 then 1
				when dif >= 0.1 and dif < 0.5 then 2
				when dif >= 0.5 and dif < 1 then 3
				when dif >= 1 and dif < 2 then 4
				when dif >= 2 and dif < 4 then 5
				when dif >= 4 and dif < 6 then 6
				when dif >= 6 and dif <= 8 then 7
					else 8 end bin 
			  from ".$db_instances[$service]['schema'].".ReqDel_MV
			  where timestamp > sysdate - :period
			  and svcclass = :svcclass)
			order by bin";
} else if ($qn ==2) {
  if ($query_svc == 0)
	$query1 = "select distinct bin, count(bin) over (Partition by bin) reqs
		from (
    			select dif , case when dif < 0.1 then 1
            when dif >= 0.1 and dif < 0.5 then 2
            when dif >= 0.5 and dif < 1 then 3
            when dif >= 1 and dif < 2 then 4
            when dif >= 2 and dif < 4 then 5
            when dif >= 4 and dif < 6 then 6
            when dif >= 6 and dif <= 8 then 7
                else 8 end bin 
          from ".$db_instances[$service]['schema'].".ReqDel_MV
          where timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
			and timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi'))
  		order by bin";
else $query1 = "select distinct bin, count(bin) over (Partition by bin) reqs
		from (
    			select dif , case when dif < 0.1 then 1
            when dif >= 0.1 and dif < 0.5 then 2
            when dif >= 0.5 and dif < 1 then 3
            when dif >= 1 and dif < 2 then 4
            when dif >= 2 and dif < 4 then 5
            when dif >= 4 and dif < 6 then 6
            when dif >= 6 and dif <= 8 then 7
                else 8 end bin 
          from ".$db_instances[$service]['schema'].".ReqDel_MV
          where timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
          and timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
	  and svcclass = :svcclass)
  	  order by bin";		
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
for($i = 0;$i < 8; $i++)
	$interval[$i] = 0;
//Fetch data in local tables
while (OCIFetch($parsed1)) {
	$bin_num = OCIResult($parsed1,1);
	$interval[$bin_num - 1] = OCIResult($parsed1,2);
}

if(empty($bin_num)) {
	No_Data_Image();
	exit();
};
//plot
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Files Requested After Garbage Collection");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
$graph->yaxis->title->Set("Number of Files" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);

$graph->xaxis->SetTickLabels($bins);
$graph->xaxis->SetLabelAngle(0);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("time");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($interval);
$b1->SetFillColor("red");
$b1->value->Show();
$b1->value->SetFormat('%0.0f');
$graph->Add($b1);
$graph->Stroke();
?> 
