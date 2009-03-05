<?php
//Different file ".$db_instances[$service]['schema']." requests percentages (Pie Graph) -> Castor Statistics
//Include necessary libraries  
include ("../lib/_oci_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_pie.php");
include ("../jpgraph-1.27/src/jpgraph_pie3d.php"); 
include ("../lib/no_data.php");
//include user account
include ("../../../conf/castor-mon-web");
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
if($qn == 1)
	$query = "select count(case when state='DiskHit' then 1 else null end) DiskHits, count(case when state='DiskCopy' then 1 else null end) DiskCopies,
			count(case when state='TapeRecall' then 1 else null end) taperecalls
			from ".$db_instances[$service]['schema']."requests 
			where NN_Requests_timestamp > sysdate - $period";
else if ($qn ==2)
	$query = "select count(case when state='DiskHit' then 1 else null end) DiskHits, count(case when state='DiskCopy' then 1 else null end) DiskCopies,
			count(case when state='TapeRecall' then 1 else null end) taperecalls
			from ".$db_instances[$service]['schema']."requests 
			where NN_Requests_timestamp >= to_date('$from','dd/mm/yyyy HH24:Mi')
				and NN_Requests_timestamp <= to_date('$to','dd/mm/yyyy HH24:Mi')";

//Create new graph, enable image cache by setting countdown period(in minutes) 
//depending on selected period. If the cached image is valid the script immediately 
//returns the cached image and exits without logining in the DB
 if ($period == 10/1440) 
	$graph = new PieGraph(420,200,"auto",1);
 else if ($period == 1/24)
	$graph = new PieGraph(420,200,"auto",5);
 else if ($period == 1)
	$graph = new PieGraph(420,200,"auto",30);
 else if ($period == 7)
	$graph = new PieGraph(420,200,"auto",60);
 else if ($period == 30)
	$graph = new PieGraph(420,200,"auto",360);
 else  if ($period == 10000)
	$graph = new PieGraph(420,200,"auto",360);
 else 
	$graph = new PieGraph(420,200,"auto");
//connect to db
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
if (!($parsed1 = OCIParse($conn, $query))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
//fetch data 
if (OCIFetch($parsed1)) {
	$DiskHits = OCIResult($parsed1,1);
	$DiskCopy = OCIResult($parsed1,2);
	$".$db_instances[$service]['schema']." taperecall = OCIResult($parsed1,3);
};
//if percentages sum equals zero then display no available data image
if(($DiskHits+$DiskCopy+$".$db_instances[$service]['schema']." taperecall) == 0) {
	No_Data_Image();
	exit();
};
//Create new pie graph
$data = array($DiskHits,$DiskCopy,TapeRecall);
$leg = array("Resident Files","Pool Copies","Tape Recalls"); 
$graph = new PieGraph(420,200,"auto");
$graph->SetShadow();
$graph->title-> Set( "File Request Percentages");
$p1 = new PiePlot3D($data);
$p1->SetLegends($leg);  
$p1->SetTheme("sand");
$p1->SetAngle(50);
$graph->Add( $p1); 
$graph->Stroke(); 
?>
