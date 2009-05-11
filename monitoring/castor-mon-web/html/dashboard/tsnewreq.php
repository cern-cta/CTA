<?php 
//Total number of Requests timeseries for selected Request Type (last ten minutes)
include ("../lib/_oci_no_cache.php");
include ("../jpgraph-1.27/src/jpgraph.php");
include ("../jpgraph-1.27/src/jpgraph_bar.php");
// include ("../jpgraph-2.3.3/src/jpgraph_line.php");
include("../lib/no_data.php");
//user account
include ("../../../conf/castor-mon-web/user.php");
//get posted data
$type = $_GET['type'];
$pattern_1 = '/[a-zA-Z0-9]{1,30}/';
preg_match($pattern_1,$type,$match);
$type = $match[0];
if($type == NULL) $type = 'StageGetRequest';
$service = $_GET['service'];
//define interval and format of timeseries

$con = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],
       $db_instances[$service]['serv']);
   if (!$con) {
     $e = ocierror();
     print htmlentities($e['message']);
     exit;
   } else {// db functionnality
       $time_series = "select euid, sum(requests) number_of_req
	     		 from castor_dlf.requeststats
	    		where timestamp >= sysdate - 15/1440
	    		and timestamp < sysdate - 5/1440
	    		and type = :type
			and euid <> '-'
            	       group by euid
		       order by number_of_req desc";
     $parsedqry = ociparse($con, $time_series);
     if (!$parsedqry) { 
        echo "Error Parsing Query <br>";
        exit();
     } else {
       ocibindbyname($parsedqry,":type",$type);
       $qryexec = ociexecute($parsedqry);       
       if (!$qryexec) {
         echo "Error Executing Query <br>";
         exit();
       } else {
         $i = 0;
	 while (ocifetch($parsedqry)) {
	   $result['EUID'][$i] = OCIResult($parsedqry,1);
           $result['NUMBER_OF_REQ'][$i++] = OCIResult($parsedqry,2);
	 }
       } 
     }
   }

//if no data retrieved print out "no data available" image
if(empty($result['EUID'])) {
	No_Data_Image();
	exit();
};
//plot data
$graph = new Graph(420,200,"auto");
$graph->SetShadow();
$graph->SetScale("textlin");
$graph->title->Set("Total $type Requests");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->img->SetMargin(80,20,20,120);
$graph->yaxis->title->Set("Requests");
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->SetTitleMargin(60);
$graph->xaxis->SetTickLabels($result['EUID']);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->Set("User ID");
$graph->xaxis->SetTitleMargin(80);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->xgrid->Show();
$bplot = new BarPlot(array_values($result['NUMBER_OF_REQ']));
$bplot->SetFillColor("red");
$graph->Add($bplot);
$graph->Stroke();
?>