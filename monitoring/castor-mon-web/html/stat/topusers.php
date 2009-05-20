<?php
//top users (whole instance)
include ("../lib/_oci_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_bar.php");
include("../lib/no_data.php");
//user account
include ("../../../conf/castor-mon-web/user.php");
//get posted values
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
if ($period == "10/1440") {
	$period = 10/1440; 
	$graph = new Graph(700,300,"auto",1);
}
else if ($period == "1/24") {
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
else 
	$graph = new Graph(700,300,"auto");
//connection - db login
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
//find top ten active users
if ($qn ==1) {
  if ($query_svc == 0)
	$query1 = "select username from (
				select username,count(username) reqs
				from ".$db_instances[$service]['schema'].".requests
				where timestamp > sysdate - :period
				group by username 
				order by reqs desc )
		   where rownum < 11";
  else 
	$query1 = "select username from (
				select username,count(username) reqs
				from ".$db_instances[$service]['schema'].".requests
				where timestamp > sysdate - :period
				and svcclass = :svcclass 
				group by username 
				order by reqs desc)
		   where rownum < 11";
} else if ($qn ==2)  {
  if ($query_svc == 0)
	$query1 = "select username from (
				select username,count(username) reqs
				from ".$db_instances[$service]['schema'].".requests
				where timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
				and timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
				group by username 
				order by reqs desc )
		   where rownum < 11";
   else
        $query1 = "select username from (
				select username,count(username) reqs
				from ".$db_instances[$service]['schema'].".requests
				where timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
				and timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
				and svcclass = :svcclass 
				group by username 
				order by reqs desc )
		   where rownum < 11";
	
	
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
$i = 0;
//fetch retrieved data
while (OCIFetch($parsed1)) {
	$username[$i] = OCIResult($parsed1,1);
	$i++;		
}
//if no data retrieved then print out "no data available" image
if(empty($username)) {
	No_Data_Image();
	exit();
};
$unames = "(";
foreach ($username as $un) {
	$unames .= "'$un'".",";
}
$unames = substr($unames, 0, -1);
$unames .= ")";
//Request Counters for Top Ten users
if ($qn == 1)  {
  if ($query_svc == 0)
	$query2 = "select distinct username,state , count(*) over (Partition by username,state)reqs
			   from ".$db_instances[$service]['schema'].".requests
			   where timestamp > sysdate - :period
					 and username in ".$unames." order by username";
					 
  else $query2 = "select distinct username,state , count(*) over (Partition by username,state)reqs
			   from ".$db_instances[$service]['schema'].".requests
			   where timestamp > sysdate - :period
			   and username in ".$unames."
			   and svcclass = :svcclass 
			   order by username";
}else if ($qn == 2)	{
  if ($query_svc == 0)   
	$query2 = "select distinct username,state , count(*) over (Partition by username,state)reqs
			   from ".$db_instances[$service]['schema'].".requests
			   where timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
					and timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
					 and username in ".$unames." order by username";
					 
  else $query2 = "select distinct username,state , count(*) over (Partition by username,state)reqs
			   from ".$db_instances[$service]['schema'].".requests
			   where timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
			     and timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
			     and username in ".$unames." 
			     and svcclass = :svcclass
			   order by username";
}
if (!($parsed2 = OCIParse($conn, $query2))) 
	{ echo "Error Parsing Query";exit();}
if ($qn == 1) {
	ocibindbyname($parsed2,":period",$period);
}
else if ($qn == 2) {
	ocibindbyname($parsed2,":from_date",$from);
	ocibindbyname($parsed2,":to_date",$to);
}
if ($query_svc == 1) {
       	  ocibindbyname($parsed2,":svcclass",$svcclass);
}
if (!OCIExecute($parsed2))
	{ echo "Error Executing Query";exit();}
//fetch final data
while (OCIFetch($parsed2)) {
	$uname = OCIResult($parsed2,1);
	$state = OCIResult($parsed2,2);
	if ($state == 'DiskHit')
		$diskhits[$uname] = OCIResult($parsed2,3);
	else if ($state == 'DiskCopy')
		$diskcopies[$uname] = OCIResult($parsed2,3);
	else if ($state == 'TapeRecall')
		$taperecalls[$uname] = OCIResult($parsed2,3);
}

$i = 0;
foreach ($username as $un) {
	if( $diskhits[$un] != NULL )
		$dh[$i] = $diskhits[$un];
	else $dh[$i] = 0;
	if( $diskcopies[$un] != NULL )
		$dc[$i] = $diskcopies[$un];
	else $dc[$i] = 0;
	if( $taperecalls[$un] != NULL )
		$tr[$i] = $taperecalls[$un];
	else $tr[$i] = 0;
	$i++;
}
//create graph
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Top Ten Users(Read File Requests - User)");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
// $graph->yaxis->title->Set("Number of ".$db_instances[$service]['schema']." requests" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);

$graph->xaxis->SetTickLabels($username);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
// $graph->xaxis->title->Set("Username");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($dh);
$b1->SetFillColor("green");
$b1 -> SetLegend("Resident Files");
$b2 = new BarPlot($dc);
$b2->SetFillColor("yellow");
$b2 -> SetLegend("D2D Copies");
$b3 = new BarPlot($tr);
$b3->SetFillColor("red");
$b3 -> SetLegend("Tape Recalls");
$gbplot = new AccBarPlot(array($b1,$b2,$b3));
$gbplot->value->Show(); 
$b1->value->SetFormat('%0.0f');
$graph->legend->SetLayout(LEGEND_HOR);
$graph->legend->Pos(0.125,0.95,"left","bottom");
$graph->Add($gbplot);
$graph->Stroke();
?> 



	
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	
