<?php
/*Plots Errors Distribution per CASTOR facility
 * for the given Service Class 
 */ 
/*inlcude necessary php libs
 *oracle login libs 
 *jpgraph libs
 */
include ("../lib/_oci_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_bar.php");
include("../lib/whiteimage.php");
//Include user account
include ("../../../conf/castor-mon-web/user.php");
//Get posted values of variables
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
$f = $_GET['f'];
$pattern_1 = '/[a-zA-Z0-9]{1,15}/';
preg_match($pattern_1,$f,$match);
$f = $match[0];
$n = $_GET['n'];
preg_match($pattern_1,$n,$match_1);
$n = $match_1[0];
//Create new graph, enable image cache by setting countdown period(in minutes) 
//depending on selected $period. If the cached image is valid the script immediately 
//returns the cached image and exits without logining in the DB
if ($period == '10/1440') {
 	$period = 10/1440; 
 	$graph = new Graph(800,300,"auto",1);
}
else if ($period == '1/24') {
        $period = 1/24;
 	$graph = new Graph(800,300,"auto",5);
}
else if ($period == '1') {
   	$period = 1;
 	$graph = new Graph(800,300,"auto",60);
}
else if ($period == '7') {
 	$period = 7;
 	$graph = new Graph(800,300,"auto",90);
}
else if ($period == '30') {
        $period = 30;
 	$graph = new Graph(800,300,"auto",360);
}
else $graph = new Graph(800,400,"auto");
//DB login  
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
//Query to retrieve Error Counters for specified Facility (whole CASTOR Instance)
if ($qn ==1)
	$query1 = "select * from (select msg_text,count(*) sum
	           from castor_dlf.dlf_messages a,castor_dlf.dlf_msg_texts b
		   where timestamp > sysdate - :period
			and b.fac_no= :f
			and b.fac_no = a.facility
			and a.msg_no = b.msg_no 
		   group by msg_text
		   order by sum desc) where rownum < 11";
else if ($qn ==2)
	$query1 = "select * from (select msg_text,count(*) sum
     	           from castor_dlf.dlf_messages a,castor_dlf.dlf_msg_texts b
		   where timestamp >= to_date(:from_date,'dd/mm/yyyy HH24:Mi')
			and timestamp <= to_date(:to_date,'dd/mm/yyyy HH24:Mi')
            		and b.fac_no= :f
			and b.fac_no = a.facility
            		and a.msg_no = b.msg_no 
	   	   group by msg_text
	   	   order by sum desc) where rownum < 11";
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
ocibindbyname($parsed1,":f",$f);
if ($qn == 1) {
	ocibindbyname($parsed1,":period",$period);
}
else if ($qn == 2) {
	ocibindbyname($parsed1,":from_date",$from);
	ocibindbyname($parsed1,":to_date",$to);
}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
//Fetch Data in local tables
$i = 0;
while (OCIFetch($parsed1)) {
	$msg_text[$i] = OCIResult($parsed1,1);;
	$num[$i] = OCIResult($parsed1,2);
	$i++;
}
//If no data fetched print "No Data Available" Image
if(empty($msg_text)) {
	No_Data_Image();
	exit();
};

//Create new plot
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Number of errors Distribution(Facility: ".$n.")");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,220);
$graph->yaxis->title->Set("Number of Errors" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);

$graph->xaxis->SetTickLabels($msg_text);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("Errors");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($num);
$b1->SetWidth(0.33);
$b1->SetFillColor("orangered");
$b1->value->SetFont(FF_FONT1,FS_BOLD,10);
$b1->value->Show();
$b1->value->SetFormat('%0.0f');
$graph->Add($b1);
$graph->Stroke();
?>  
