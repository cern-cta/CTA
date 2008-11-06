<?php
include ("lib.php");
include ("../jpgraph-2.3.3/src/jpgraph.php");
include ("../jpgraph-2.3.3/src/jpgraph_bar.php");
include("no_data.php");
include ("user.php");
$period = $_GET['period'];
if($period == NULL) $period = 10000;
//connection
$conn = ocilogon(username,pass,serv);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}


   
$query1 = "select distinct svcclass, state, count(*) over (Partition by svcclass,state) reqs
	   from requests
	   where timestamp > sysdate - $period
	   order by svcclass";

if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$pre_pool = 'foo';
$i = 0;
while (OCIFetch($parsed1)) {
	$pool = OCIResult($parsed1,1);
	if($pool != $pre_pool) {
		$pool_names[$i] = $pool;
		$pre_pool = $pool;
		$i++;
	}
	$state = OCIResult($parsed1,2);
	if ($state == 'DiskHit')
		$diskhits[$pool] = OCIResult($parsed1,3);
	else if ($state == 'DiskCopy')
		$diskcopies[$pool] = OCIResult($parsed1,3);
	else if ($state == 'TapeRecall')
		$taperecalls[$pool] = OCIResult($parsed1,3);
}
if(empty($pool_names)) {
	No_Data_Image();
	exit();
};
$i = 0;
foreach ($pool_names as $pn) {
	if( $diskhits[$pn] != NULL )
		$dh[$i] = $diskhits[$pn];
	else $dh[$i] = 0;
	if( $diskcopies[$pn] != NULL )
		$dc[$i] = $diskcopies[$pn];
	else $dc[$i] = 0;
	if( $taperecalls[$pn] != NULL )
		$tr[$i] = $taperecalls[$pn];
	else $tr[$i] = 0;
	$i++;
}

if ($period == 10/1440) 
	$graph = new Graph(700,350,"auto",1);
else if ($period == 1/24)
	$graph = new Graph(700,350,"auto",5);
else if ($period == 1)
	$graph = new Graph(700,350,"auto",30);
else if ($period == 7)
	$graph = new Graph(700,350,"auto",60);
else if ($period == 30)
	$graph = new Graph(700,350,"auto",360);
else 
	$graph = new Graph(700,350,"auto",360);
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Requests per POOL");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
// $graph->yaxis->title->Set("Number of Requests" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);
$graph->yaxis->SetFont(FF_FONT1,FS_BOLD,8);
$graph->xaxis->SetTickLabels($pool_names);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD,8);
// $graph->xaxis->title->Set("POOL");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($dh);
$b1->SetFillColor("orangered");
$b1 -> SetLegend("Resident Files");
$b2 = new BarPlot($dc);
$b2->SetFillColor("maroon");
$b2 -> SetLegend("Pool Copies");
$b3 = new BarPlot($tr);
$b3->SetFillColor("darkgoldenrod1");
$b3 -> SetLegend("Tape Recalls");
$gbplot = new AccBarPlot(array($b1,$b2,$b3));
$gbplot->value->Show(); 
$graph->Add($gbplot);
$graph->Stroke();

?> 



	
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	
