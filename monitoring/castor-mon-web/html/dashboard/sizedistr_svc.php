<?php 
//same as sdistr.php (last 10 minutes)
include ("../lib/_oci_no_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_bar.php");
include("../lib/no_data.php");
include ("../../../conf/castor-mon-web/user.php");
$svcclass = $_GET['svcclass'];
$pattern_1 = '/[a-zA-Z0-9]{1,15}/';
preg_match($pattern_1,$svcclass,$match);
$svcclass = $match[0];
if($svcclass == NULL) $svcclass = 'default';
$service = $_GET['service'];
//initialization
$bins = array( 0 =>"<1Mb",1 =>"[1-10)Mb",2 =>"[10-100)Mb",3 =>"[100Mb-1Gb)",4 =>"[1-1.5)Gb",5 =>"[1.5-2)Gb",6 =>"[2-2.5]Gb",7 =>">2.5");
//connection
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
$query1 = "select distinct bin, count(bin) over (Partition by bin) reqs 
	from (
	 select round(filesize/1024,4) , case when filesize < 1048576 then 1
	  when filesize >= 1048576 and filesize < 10485760 then 2
	  when filesize >= 10485760 and filesize < 104857600 then 3
	  when filesize >= 104857600 and filesize <= 1073741824 then 4
          when filesize >= 1073741824 and filesize <= 1610612736 then 5
          when filesize >= 1610612736 and filesize <= 2147483648 then 6
          when filesize >= 2147483648 and filesize <= 2684354560  then 7
	  else 8 end bin
	from ".$db_instances[$service]['schema'].".requests
	where state = 'TapeRecall'
	and timestamp >= sysdate - 15/1440
	and timestamp < sysdate - 5/1440 
	and svcclass = :svcclass
	and filesize!=0)
	order by bin";


if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
ocibindbyname($parsed1,":svcclass",$svcclass);
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
for($i = 0;$i < 8; $i++)
	$fsize[$i] = 0;
while (OCIFetch($parsed1)) {
	$bin_num = OCIResult($parsed1,1);
	$fsize[$bin_num - 1] = OCIResult($parsed1,2);
}

if(empty($bin_num)) {
	No_Data_Image();
	exit();
};
//data processing

//plot
$graph = new Graph(700,300,"auto");
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Filesize Distribution[TapeRecalled Files($svcclass)]");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(60,40,40,120);
$graph->yaxis->title->Set("Number of Files" );
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(40);

$graph->xaxis->SetTickLabels($bins);
$graph->xaxis->SetLabelAngle(0);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("FileSize");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$b1 = new BarPlot($fsize);
$b1->SetFillColor("orangered");
$b1->value->Show();
$b1->value->SetFormat('%0.0f');
$graph->Add($b1);
$graph->Stroke();
?> 



	
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	
