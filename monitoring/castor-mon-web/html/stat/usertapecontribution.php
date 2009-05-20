<?php 
//contribution to tape mount per user
//(Requested files, recalled from tape (Tape was not mounted))
include ("../lib/_oci_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_bar.php");
include("../lib/no_data.php");
//user account
include ("../../../conf/castor-mon-web/user.php");
//get posted data
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
else
	$graph = new Graph(700,250,"auto");
//connection - db login
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
if($qn == 1) {
  if ($query_svc == 0)
	$query1 = "select * from (
	  select username, count(username) con_mount
	  from ".$db_instances[$service]['schema'].".requests a, ".$db_instances[$service]['schema'].".taperecall b
	  where  a.subreqid = b.subreqid
	  and a.timestamp > sysdate - :period
	  and b.timestamp > sysdate - :period
	  and b.tapemountstate in ('TAPE_PENDING','TAPE_WAITDRIVE','TAPE_WAITPOLICY','TAPE_WAITMOUNT') 
	  group by username
	  order by con_mount desc )
	  where rownum < 11";
  else  $query1 = "select * from (
	  select username, count(username) con_mount
	  from ".$db_instances[$service]['schema'].".requests a, ".$db_instances[$service]['schema'].".taperecall b
	  where  a.subreqid = b.subreqid
	  and a.timestamp > sysdate - :period
	  and b.timestamp > sysdate - :period
	  and b.tapemountstate in ('TAPE_PENDING','TAPE_WAITDRIVE','TAPE_WAITPOLICY','TAPE_WAITMOUNT')
	  and svcclass = :svcclass  
	  group by username
	  order by con_mount desc )
	  where rownum < 11";
} else if ($qn ==2) {
  if ($query_svc == 0)
	$query1 = "select * from (
	  select username, count(username) con_mount
	  from ".$db_instances[$service]['schema'].".requests a, ".$db_instances[$service]['schema'].".taperecall b
	  where  a.subreqid = b.subreqid
	  and a.timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
		and a.timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
		and b.timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
		and b.timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
	  and b.tapemountstate in ('TAPE_PENDING','TAPE_WAITDRIVE','TAPE_WAITPOLICY','TAPE_WAITMOUNT') 
	  group by username
	  order by con_mount desc )
	  where rownum < 11";   
  else  $query1 = "select * from (
	  select username, count(username) con_mount
	  from ".$db_instances[$service]['schema'].".requests a, ".$db_instances[$service]['schema'].".taperecall b
	  where  a.subreqid = b.subreqid
	  and a.timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
		and a.timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
		and b.timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
		and b.timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')	
	  and b.tapemountstate in ('TAPE_PENDING','TAPE_WAITDRIVE','TAPE_WAITPOLICY','TAPE_WAITMOUNT') 
	  and svcclass = :svcclass
	  group by username
	  order by con_mount desc )
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

$i=0;
//fetch data into local tables
while (OCIFetch($parsed1)) {
	$username[$i] = OCIResult($parsed1,1);
	$mounts[$i] = OCIResult($parsed1,2);
	$i++;		
}
//if no data retrieved then print out no data available image
if(empty($username)) {
	No_Data_Image();
	exit();
};
//plot data
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Top Ten Mounters");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
$graph->yaxis->title->Set("Number of Requests" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);

$graph->xaxis->SetTickLabels($username);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("Username");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($mounts);
$b1->SetFillColor("yellow");
$b1->value->Show(); 
$b1->value->SetFormat('%0.0f');
$graph->Add($b1);
$graph->Stroke();
?> 



	
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	
