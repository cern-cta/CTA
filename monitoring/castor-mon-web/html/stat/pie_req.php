<?php
//Different file ".$db_instances[$service]['schema']." requests percentages (Pie Graph) -> Castor Statistics
//Include necessary libraries  
include ("../lib/_oci_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_pie.php");
include ("../lib/no_data.php");
//include user account
include ("../../../conf/castor-mon-web/user.php");
//Define Request Categories


function generateData($req_type,$req_number) {
 //User I/O requests 		              ->   u
//User query requests 		              ->   q
//Space query requests  	              ->   s
//Replication requests (start/done)           ->   d
//Internal mover requests (start/done/failed) ->   j
//Garbage collecting + synch requests         ->   g

$requests = array(
	"ChangePrivilege" => 's',
	"Disk2DiskCopyDoneRequest" => 'd',
	"ChangePrivilege"  =>  's',
	"Disk2DiskCopyDoneRequest" => 'd',
	"Disk2DiskCopyStartRequest" => 'd',
	"DiskPoolQuery" => 's',
	"Files2Delete"	=> 'g',
	"FilesDeleted"	=> 'g',
	"FilesDeletionFailed" => 'g',
	"FirstByteWritten" => 'j',
	"GetUpdateDone" => 'j',
	"GetUpdateFailed" => 'j',
	"GetUpdateStartRequest" => 'j',
	"ListPrivileges" => 's',
	"MoverCloseRequest" => 'j',
	"NsFilesDeleted" => 'g',
	"PutFailed" => 'j',
	"PutStartRequest" => 'j',
	"SetFileGCWeight" => 'u',
	"StageFileQueryRequest" => 'q',
	"StageGetRequest" => 'u',
	"StagePrepareToGetRequest" => 'u',
	"StagePrepareToPutRequest" => 'u',
	"StagePrepareToUpdateRequest" => 'u',
	"StagePutDoneRequest" => 'u',
	"StagePutRequest" => 'u',
	"StageRmRequest" => 'u',
	"StageUpdateRequest" =>	'u',
	"StgFilesDeleted" => 'g'
);
$data = array(
 	  'u' => 0,
	  'q' => 0,
	  's' => 0,
	  'd' => 0,
	  'j' => 0,
	  'g' => 0
);
 
for($j = 0;$j < sizeof($req_type);$j++) {
 	$category = $requests[$req_type[$j]];
	$data[$category] += $req_number[$j];
	}
 return($data);
};

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
	$query = "select type, sum(requests) 
		  from ".$db_instances[$service]['schema'].".requeststats 
		  where timestamp > sysdate - :period
		    and euid = '-'
		  group by type";
else if ($qn ==2)
	$query = "select type, sum(requests)
		  from ".$db_instances[$service]['schema'].".requeststats
		  where timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
		    and timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
		    and euid = '-'
		  group by type";

//Create new graph, enable image cache by setting countdown period(in minutes) 
//depending on selected period. If the cached image is valid the script immediately 
//returns the cached image and exits without logining in the DB
 if ($period == '10/1440') {
 	$period = 10/1440; 
	$graph = new PieGraph(700,200,"auto",1);
}
 else if ($period == '1/24') {
 	$period = 1/24;
	$graph = new PieGraph(700,200,"auto",5);
}
 else if ($period == '1') {
 	$period = 1;
	$graph = new PieGraph(700,200,"auto",30);
}
 else if ($period == '7') {
 	$period = 7;
	$graph = new PieGraph(700,200,"auto",60);
}
 else if ($period == '30') {
 	$period = 30;
	$graph = new PieGraph(700,200,"auto",360);
}
 else 
	$graph = new PieGraph(700,200,"auto");
//connect to db
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
if (!($parsed1 = OCIParse($conn, $query))) 
	{ echo "Error Parsing Query";exit();}
if ($qn == 1) {
	ocibindbyname($parsed1,":period",$period);
}
else if ($qn == 2) {
	ocibindbyname($parsed1,":from_date",$from);
	ocibindbyname($parsed1,":to_date",$to);
};
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
//fetch data 
$i = 0;
while (OCIFetch($parsed1)) {
	$req_type[$i] = OCIResult($parsed1,1);
	$req_number[$i] = OCIResult($parsed1,2);
	$i++;
};
//if percentages sum equals zero then display no available data image
if(empty($req_type)) {
	No_Data_Image();
	exit();
};

//Create new pie graph
$data = generateData($req_type,$req_number);
//print_r($data);
$leg = array("User I/O requests","User query requests","Space query requests","Replication requests(start/done)","Internal mover requests (start/dome/failed)","Garbage collecting + synch requests"); 
$graph->SetShadow();
$graph->title-> Set( "Request Percentages");
$p1 = new PiePlot(array_values($data));
$p1->SetLegends($leg);  
$p1->SetCenter(0.3,0.5);
$graph ->legend->Pos( 0.02,0.5,"right" ,"center");
$graph->Add( $p1); 
$graph->Stroke(); 
?>
