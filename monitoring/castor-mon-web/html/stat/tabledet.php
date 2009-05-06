<?php
//main statistical page 
	error_reporting(E_ALL ^ E_NOTICE);
	$timewindow = $_REQUEST["timewindow"];
	$pattern = '/[0-9]+([\/][0-9]+)?/';
	preg_match($pattern,$timewindow,$matches);
	$timewindow = $matches[0];
	$date1 = $_REQUEST["date1"];
	$pattern = '/[0-3][0-9][\/][0-1][0-9][\/][0-9][0-9][0-9][0-9]/';
	preg_match($pattern,$date1,$matches1);
	$date1 = $matches1[0];
	$date1hour = $_REQUEST["date1hour"];
	$pattern1 = '/[0-2][0-9][:][0-5][0-9]/';
	preg_match($pattern1,$date1hour,$matches2);
	$date1hour = $matches2[0];
	$date2 = $_REQUEST["date2"];
	$pattern3 = '/[0-3][0-9][\/][0-1][0-9][\/][0-9][0-9][0-9][0-9]/';
	preg_match($pattern3,$date2,$matches3);
	$date2 = $matches3[0];
	$date2hour = $_REQUEST["date2hour"];
	$pattern4 = '/[0-2][0-9][:][0-5][0-9]/';
	preg_match($pattern1,$date2hour,$matches4);
	$date2hour = $matches4[0];
	$stat = $_GET["stat"];
	$det = $_GET["det"];
	$service = $_GET['service'];
	$custom_date = 0;
	if(($date1 != NULL)and($date2 != NULL)and($date1hour != NULL)and($date2hour != NULL)) $custom_date =1;
	if ($stat == NULL)
		$stat = "General"; 
	if ($timewindow == "all")
		$timewindow = "10000";
?>
<html>
	<head>
		<LINK rel="stylesheet" type="text/css" title="compact" href="../lib/hmenu.css">
		<SCRIPT LANGUAGE="JavaScript" SRC="../calendar/CalendarPopup.js"></SCRIPT>
		<script language="javascript" type="text/javascript">
		function showdiv(name){
			var obj = (document.getElementById)?document.getElementById(name) : eval("document.all[name]");
			if (obj.style.display=="none"){
			obj.style.display="";
			}
			else{
			obj.style.display="none";
			}
		}
   		</script>
		<style>
		#ora {
			font: bold 25px/50px arial, helvetica, sans-serif;
			color: #fff;
			background: orangered;
			text-transform: uppercase;
		}
		</style>
		<!--[if IE]>
		<style type="text/css" media="screen">	
		body {
			behavior: url('../lib/csshover.htc');
			font-size: 100%;
		} 
		#menu ul li {float: left; width: 100%; height: 1%;}
		#menu ul li a {height: 1%;} 
		
		#menu a, #menu h2 {
			font: bold 0.7em/1.4em arial, helvetica, sans-serif;
		} 
		</style>
		<![endif]-->
	</head>
	<body>
		<table><tr>
			<td valign = "top">
				<?php 
					if ($stat == "FileReq") {
					if ($custom_date == 1) {
							echo "<div id='ora'> $service: File Request Statistics - From: $date1 $date1hour To $date2 $date2hour</div>";
						}
						else {
							if($timewindow == "10/1440")
								echo "<div id='ora'> $service: File Request Statistics - Last Ten Minutes </div>";
							else if($timewindow == "1/24")
								echo "<div id='ora'> $service: File Request Statistics - Last Hour </div>";
							else if($timewindow == "1")
								echo "<div id='ora'> $service: File Request Statistics - Last Day </div>";
							else if($timewindow == "7")
								echo "<div id='ora'> $service: File Request Statistics - Last Week </div>";
							else if($timewindow == "30")
								echo "<div id='ora'> $service: File Request Statistics - Last Month </div>";
						}
					}
					else if ($stat == "Latency") {
						if ($custom_date == 1) {
							echo "<div id='ora'> $service: Latency Statistics - From: $date1 $date1hour To $date2 $date2hour</div>";
						}
						else {
							if($timewindow == "10/1440")
								echo "<div id='ora'> $service: Latency Statistics - Last Ten Minutes </div>";
							else if($timewindow == "1/24")
								echo "<div id='ora'> $service: Latency Statistics - Last Hour </div>";
							else if($timewindow == "1")
								echo "<div id='ora'> $service: Latency Statistics - Last Day </div>";
							else if($timewindow == "7")
								echo "<div id='ora'> $service: Latency Statistics - Last Week </div>";
							else if($timewindow == "30")
								echo "<div id='ora'> $service: Latency Statistics - Last Month </div>";
						}
					}
					else if ($stat == "File") {
						if ($custom_date == 1) {
							echo "<div id='ora'> $service: File System Statistics - From: $date1 $date1hour To $date2 $date2hour</div>";
						}
						else {
							if($timewindow == "10/1440")
								echo "<div id='ora'> $service: File System Statistics - Last Ten Minutes </div>";
							else if($timewindow == "1/24")
								echo "<div id='ora'> $service: File System Statistics - Last Hour </div>";
							else if($timewindow == "1")
								echo "<div id='ora'> $service: File System Statistics - Last Day </div>";
							else if($timewindow == "7")
								echo "<div id='ora'> $service: File System Statistics - Last Week </div>";
							else if($timewindow == "30")
								echo "<div id='ora'> $service: File System Statistics - Last Month </div>";
						}
					}	
					else if ($stat == "Tape") {
						if ($custom_date == 1) {
							echo "<div id='ora'> $service: Tape Statistics - From: $date1 $date1hour To $date2 $date2hour</div>";
						}
						else {
							if($timewindow == "10/1440")
								echo "<div id='ora'> $service: Tape Statistics - Last Ten Minutes </div>";
							else if($timewindow == "1/24")
								echo "<div id='ora'> $service: Tape Statistics - Last Hour </div>";
							else if($timewindow == "1")
								echo "<div id='ora'> $service: Tape Statistics - Last Day </div>";
							else if($timewindow == "7")
								echo "<div id='ora'> $service: Tape Statistics - Last Week </div>";
							else if($timewindow == "30")
								echo "<div id='ora'> $service: Tape Statistics - Last Month </div>";
						}
					}
					else if ($stat == "Scheduler") {
						if ($custom_date == 1) {
							echo "<div id='ora'> $service: Scheduler Statistics - From: $date1 $date1hour To $date2 $date2hour</div>";
						}
						else {
							if($timewindow == "10/1440")
								echo "<div id='ora'> $service: Scheduler Statistics - Last Ten Minutes </div>";
							else if($timewindow == "1/24")
								echo "<div id='ora'> $service: Scheduler Statistics - Last Hour </div>";
							else if($timewindow == "1")
								echo "<div id='ora'> $service: Scheduler Statistics - Last Day </div>";
							else if($timewindow == "7")
								echo "<div id='ora'> $service: Scheduler Statistics - Last Week </div>";
							else if($timewindow == "30")
								echo "<div id='ora'> $service: Scheduler Statistics - Last Month </div>";
						}
					}
					else if ($stat == "GC") {
						if ($custom_date == 1) {
							echo "<div id='ora'> $service: Garbage Collection Statistics - From: $date1 $date1hour To $date2 $date2hour</div>";
						}
						else {
							if($timewindow == "10/1440")
								echo "<div id='ora'> $service: Garbage Collection Statistics - Last Ten Minutes </div>";
							else if($timewindow == "1/24")
								echo "<div id='ora'> $service: Garbage Collection Statistics - Last Hour </div>";
							else if($timewindow == "1")
								echo "<div id='ora'> $service: Garbage Collection Statistics - Last Day </div>";
							else if($timewindow == "7")
								echo "<div id='ora'> $service: Garbage Collection Statistics - Last Week </div>";
							else if($timewindow == "30")
								echo "<div id='ora'> $service: Garbage Collection Statistics - Last Month </div>";
						}
					}
					else if ($stat == "Migration") {
						if ($custom_date == 1) {
							echo "<div id='ora'> $service: File Migration Statistics - From: $date1 $date1hour To $date2 $date2hour</div>";
						}
						else {
							if($timewindow == "10/1440")
								echo "<div id='ora'> $service: File Migration Statistics - Last Ten Minutes </div>";
							else if($timewindow == "1/24")
								echo "<div id='ora'> $service: File Migration Statistics - Last Hour </div>";
							else if($timewindow == "1")
								echo "<div id='ora'> $service: File Migration Statistics - Last Day </div>";
							else if($timewindow == "7")
								echo "<div id='ora'> $service: File Migration Statistics - Last Week </div>";
							else if($timewindow == "30")
								echo "<div id='ora'> $service: File Migration Statistics - Last Month </div>";
						}
					}
					else {
						if ($custom_date == 1) {
							echo "<div id='ora'> $service: General Statistics - From: $date1 $date1hour To $date2 $date2hour</div>";
						}
						else {				
							if($timewindow == "10/1440")
								echo "<div id='ora'> $service: General Statistics - Last Ten Minutes </div>";
							else if($timewindow == "1/24")
								echo "<div id='ora'> $service: General Statistics - Last Hour </div>";
							else if($timewindow == "1")
								echo "<div id='ora'> $service: General Statistics - Last Day </div>";
							else if($timewindow == "7")
								echo "<div id='ora'> $service: General Statistics - Last Week </div>";
							else if($timewindow == "30")
								echo "<div id='ora'> $service: General Statistics - Last Month </div>";
						}
					}
					
						?>
				<table><tr><td colspan = "2" valign = "top">
				<div id="menu">
					<ul><li><a href="../dashboard/dashboard.php?service=<?php echo $service;?>">HOME</a></li></ul>
					<ul>
						<li><h2>Statistics</h2>
							<ul> 
							   <?php if ($custom_date == 0) {?>
								<li><a href="tabledet.php?timewindow=<?php echo $timewindow;?>&service=<?php echo $service;?>">General Statistics</a></li>
								<li><a href="tabledet.php?timewindow=<?php echo $timewindow;?>&service=<?php echo $service;?>&stat=FileReq">File Request Statistics</a></li>
								<li><a href="tabledet.php?timewindow=<?php echo $timewindow;?>&service=<?php echo $service;?>&stat=Migration">File Migration Statistics</a></li>
								<li><a href="tabledet.php?timewindow=<?php echo $timewindow;?>&service=<?php echo $service;?>&stat=Latency">Latency Statistics</a></li>
								<li><a href="tabledet.php?timewindow=<?php echo $timewindow;?>&service=<?php echo $service;?>&stat=File">File System Statistics</a></li>
								<li><a href="tabledet.php?timewindow=<?php echo $timewindow;?>&service=<?php echo $service;?>&stat=Tape">Tape Statistics</a></li>
								<li><a href="tabledet.php?timewindow=<?php echo $timewindow;?>&service=<?php echo $service;?>&stat=Scheduler">Scheduler Statistics</a></li>
								<li><a href="tabledet.php?timewindow=<?php echo $timewindow;?>&service=<?php echo $service;?>&stat=GC">Garbage Collection Statistics</a></li>
							   <?} else { ?>
							   	<li><a href="tabledet.php?date1=<?php echo $date1;?>&date1hour=<?php echo $date1hour;?>&date2=<?php echo $date2;?>&date2hour=<?php echo $date2hour;?>&service=<?php echo $service;?>">General Statistics</a></li>
								<li><a href="tabledet.php?date1=<?php echo $date1;?>&date1hour=<?php echo $date1hour;?>&date2=<?php echo $date2;?>&date2hour=<?php echo $date2hour;?>&service=<?php echo $service;?>&stat=FileReq">File Request Statistics</a></li>
								<li><a href="tabledet.php?date1=<?php echo $date1;?>&date1hour=<?php echo $date1hour;?>&date2=<?php echo $date2;?>&date2hour=<?php echo $date2hour;?>&service=<?php echo $service;?>&stat=Migration">File Migration Statistics</a></li>
								<li><a href="tabledet.php?date1=<?php echo $date1;?>&date1hour=<?php echo $date1hour;?>&date2=<?php echo $date2;?>&date2hour=<?php echo $date2hour;?>&service=<?php echo $service;?>&stat=Latency">Latency Statistics</a></li>
								<li><a href="tabledet.php?date1=<?php echo $date1;?>&date1hour=<?php echo $date1hour;?>&date2=<?php echo $date2;?>&date2hour=<?php echo $date2hour;?>&service=<?php echo $service;?>&stat=File">File System Statistics</a></li>
								<li><a href="tabledet.php?date1=<?php echo $date1;?>&date1hour=<?php echo $date1hour;?>&date2=<?php echo $date2;?>&date2hour=<?php echo $date2hour;?>&service=<?php echo $service;?>&stat=Tape">Tape Statistics</a></li>
								<li><a href="tabledet.php?date1=<?php echo $date1;?>&date1hour=<?php echo $date1hour;?>&date2=<?php echo $date2;?>&date2hour=<?php echo $date2hour;?>&service=<?php echo $service;?>&stat=Scheduler">Scheduler Statistics</a></li>
								<li><a href="tabledet.php?date1=<?php echo $date1;?>&date1hour=<?php echo $date1hour;?>&date2=<?php echo $date2;?>&date2hour=<?php echo $date2hour;?>&service=<?php echo $service;?>&stat=GC">Garbage Collection Statistics</a></li>
							    <? }?>
							</ul>
						</li>
					</ul>
					<ul>
						<li><h2>Links</h2>
							<ul>
								<li><a href="http://c2adm/" target="_blank">CASTOR Dashboard</a></li>
								<li><a href="http://castortapelog/tapelog-web/" target="_blank">Tape Statistics</a></li>
								<li><a href="http://sls.cern.ch/sls/service.php?id=CASTOR" target="_blank">Service Level Status Overview</a></li>
								<li><a href="http://lemonweb.cern.ch/lemon-web/info.php?entity=castor2&type=host&cluster=1" target="_blank">Lemon Monitoring</a></li>
							</ul>
						</li>
					</ul>
				</div>
				</td></tr>
				<tr><td valign = "top">
					<?php if($stat == NULL) 
						echo "<form action='tabledet.php?service=$service' method='post'>";
					      else 
					        echo "<form action='tabledet.php?service=$service&stat=$stat' method='post'>";?>
					<b> Fixed Timewindow: </b>
					<select name="timewindow">
	  				<option onclick="javascript:this.form.submit()" value ="10/1440" selected = "selected">Last 10 minutes</option>
	  				<option onclick="javascript:this.form.submit()" value ="1/24">Last Hour</option>
	  				<option onclick="javascript:this.form.submit()" value ="1" >Last Day</option>
	  				<option onclick="javascript:this.form.submit()" value ="7">Last Week</option>
	  				<option onclick="javascript:this.form.submit()" value ="30" >Last Month</option>
	  				</select>
					</form>
				</td></tr>
				<tr><td valign = "top">
					<?php if($stat == NULL) 
						echo "<form action='tabledet.php?service=$service' method='post'>";
					      else 
					        echo "<form action='tabledet.php?service=$service&stat=$stat' method='post'>";?>
					<SCRIPT LANGUAGE="JavaScript" ID="calendar">var cal1 = new CalendarPopup();</SCRIPT>
					<SCRIPT LANGUAGE="JavaScript">writeSource("calendar");</SCRIPT>
					<b>From:</b> <INPUT TYPE="text" NAME="date1" VALUE="dd/MM/yyyy" SIZE=9 onClick="cal1.select(document.forms[1].date1,'anchor1','dd/MM/yyyy'); return false;" TITLE="Select Starting Date" NAME="anchor1" ID="anchor1">
				    <INPUT TYPE="text" NAME="date1hour" VALUE="00:00" SIZE=4>
					<b>To:</b> <INPUT TYPE="text" NAME="date2" VALUE="dd/MM/yyyy" SIZE=9 onClick="cal1.select(document.forms[1].date2,'anchor2','dd/MM/yyyy',(document.forms[1].date2.value=='')?document.forms[1].date1.value:null); return false;" TITLE="Select Ending Date" NAME="anchor2" ID="anchor2">
					<INPUT TYPE="text" NAME="date2hour" VALUE="00:00" SIZE=4>
					<input type="submit" name = "submit" value = "Print">
					</form>
				</td></tr>
				</td></tr></table>
				<?php
					if($stat == "FileReq") {
						if ($custom_date ==0) {
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='requestsperpool.php?service=$service&period=$timewindow' title='File requests per SvcClass \n Three Categories of File requests \n a) File Present on Disk (DiskHit) \n b) File Copied from another ScvClass \n c) File Recalled from Tape'>
								</td>
								<td>
								 <img src ='topusers.php?service=$service&period=$timewindow' title = 'Top Users - File requests \n File requests are distinguished into three categories diskhits, \ncopies from another svcclass and tape recalls'>
								</td>
								  </tr>
								  </table>
								  </div>
								  <div>
								  <table>
									<tr>
								<td valign ='top'>
									<img src ='prefetch.php?service=$service&period=$timewindow' title = 'Direct and Prestaged File requests per SvcClass. We focus on tape recalled files.'>
								</td>
								<td valign ='top'>
									<img src ='prestage_users.php?service=$service&period=$timewindow' title = 'Direct and Prestaged File requests per User. We focus on tape recalled files'>
								</td>
									  </tr>
									  </table>
										  </div>";
						}
						else 
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='requestsperpool.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title='File requests per SvcClass \n Three Categories of File requests \n a) File Present on Disk (DiskHit) \n b) File Copied from another ScvClass \n c) File Recalled from Tape'>
								</td>
								<td>
								 <img src ='topusers.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title = 'Top Users - File requests \n File requests are distinguished into three categories diskhits, \ncopies from another svcclass and tape recalls'>
								</td>
								  </tr>
								  </table>
								  </div>
								  <div>
								  <table>
									<tr>
								<td valign ='top'>
									<img src ='prefetch.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title = 'Direct and Prestaged File requests per SvcClass. We focus on tape recalled files.'>
								</td>
								<td valign ='top'>
									<img src ='prestage_users.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title = 'Direct and Prestaged File requests per User. We focus on tape recalled files'>
								</td>
									  </tr>
									  </table>
										  </div>";
					}
					else if($stat == "Latency"){
						if ($custom_date == 0) {
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='latencies.php?service=$service&period=$timewindow' title ='Total Latency from the arrival of a new request until the file is returned to the user.\n (TotalWaitTime summary message from dlf)'>
								</td>
								  </tr>
								  <tr>
									<td>
								 <img src ='miglat.php?service=$service&period=$timewindow' title ='Total Latency until the file is migrated to tape.\n (TotalWaitTime summary message from dlf)'>
								</td>
								  </tr>
								    <tr>
									<td>
								 <img src ='job_wait_time_min.php?service=$service&period=$timewindow'>
								</td>
								  </tr>
								    <tr>
									<td>
								 <img src ='job_wait_time_max.php?service=$service&period=$timewindow'>
								</td>
								  </tr>
								    <tr>
									<td>
								 <img src ='job_wait_time_mean.php?service=$service&period=$timewindow'>
								</td>
								  </tr>
								  </table>
								  </div>
								";
						}
						else
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='latencies.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title ='Total Latency from the arrival of a new request until the file is returned to the user.\n (TotalWaitTime summary message from dlf)'>
								</td>
								  </tr>
								  <tr>
									<td>
								 <img src ='miglat.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title ='Total Latency until the file is migrated to tape.\n (TotalWaitTime summary message from dlf)'>
								</td>
								  </tr>
								  <tr>
									<td>
								 <img src ='job_wait_time_min.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour'>
								</td>
								  </tr>
								  <tr>
									<td>
								 <img src ='job_wait_time_max.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour'>
								</td>
								  </tr>
								  <tr>
									<td>
								 <img src ='job_wait_time_mean.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour'>
								</td>
								  </tr>
								  </table>
								  </div>
								";
					}
					else if($stat == "File"){
						if ($custom_date == 0) {
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='sizedistr.php?service=$service&period=$timewindow' title='File Size Distribution of Tape Recalled Files. Detailed Graphs per SvcClass'>
								</td>
								<td>
								 <input type=\"button\" value=\"Show\Hide\" Full Table onClick= \"showdiv('det')\">
								</td>
								  </tr>
								  </table>
								  </div><div id=\"det\" style=\"display:none\">";
							$period = $timewindow;
							include("sizedet.php");
							echo "</div>";
						}
						else {
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='sizedistr.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title='File Size Distribution of Tape Recalled Files. Detailed Graphs per SvcClass'>
								</td>
								<td>
								 <input type=\"button\" value=\"Show\Hide\" Full Table onClick= \"showdiv('det')\">
								</td>
								  </tr>
								  </table>
								  </div><div id=\"det\" style=\"display:none\">";
							include("sizedet.php");
							echo "</div>";
						}
					}
					else if($stat == "Error"){
						if ($custom_date == 0) {
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='errors.php?service=$service&period=$timewindow' title='Top 10 Errors (Whole Castor2 Instance)'>
								</td>
								<td>
								 <input type=\"button\" value=\"Show\Hide\" Full Table onClick= \"showdiv('det')\">
								</td>
								  </tr>
								  </table>
								  </div><div id=\"det\" style=\"display:none\">";
							$period = $timewindow;
							include("errordet.php");
							echo "</div>";
						}
						else {
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='errors.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title='Top 10 Errors (Whole Castor2 Instance)'>
								</td>
								<td>
								 <input type=\"button\" value=\"Show\Hide\" Full Table onClick= \"showdiv('det')\">
								</td>
								  </tr>
								  </table>
								  </div><div id=\"det\" style=\"display:none\">";
							include("errordet.php");
							echo "</div>";
						}
					}
					else if($stat == "Tape"){
						if($custom_date == 0) {
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='toptapes.php?service=$service&period=$timewindow' title=' Files Recalled per Tape Volume irrespective of tape mount status(Usage per Tape Volume). \n Detailed Graphs per SvcClass'>
								</td>
								<td>
								 <input type=\"button\" value=\"Show\Hide\" Full Table onClick= \"showdiv('det')\">
								</td>
								  </tr>
								  </table>
								  </div>
								  <div id=\"det\" style=\"display:none\">";
							$period=$timewindow;
							include("toptapesdet.php");
							echo "</div>";    
							echo "<div>
								<table>
								  <tr><td align = 'center'>
								<img src='usertapecontribution.php?service=$service&period=$timewindow' title =' File requests per User, where file was recalled from tape and the tape wasn't already mounted.\n Thus User Tape Mount Contribution'>
								  </td></tr>
								  </table>
								  </div>
								  <div>
								<table>
								  <tr><td align = 'center'>
								<img src='filesmount.php?service=$service&period=$timewindow' title =' Number of files recalled from a tape volume during a single mount. Number of Recalled files Distribution'>
								  </td></tr>
								  </table>
								  </div>";
						}
						else {
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='toptapes.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title=' Files Recalled per Tape Volume irrespective of tape mount status(Usage per Tape Volume). \n Detailed Graphs per SvcClass'>
								</td>
								<td>
								 <input type=\"button\" value=\"Show\Hide\" Full Table onClick= \"showdiv('det')\">
								</td>
								  </tr>
								  </table>
								  </div>
								  <div id=\"det\" style=\"display:none\">";
							include("toptapesdet.php");
							echo "</div>";    
							echo "<div>
								<table>
								  <tr><td align = 'center'>
								<img src='usertapecontribution.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title =' File requests per User, where file was recalled from tape and the tape wasn't already mounted.\n Thus User Tape Mount Contribution'>
								  </td></tr>
								  </table>
								  </div>
								  <div>
								<table>
								  <tr><td align = 'center'>
								<img src='filesmount.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title =' Number of files recalled from a tape volume during a single mount. Number of Recalled files Distribution'>
								  </td></tr>
								  </table>
								  </div>";
						}
					}
					else if($stat == "Scheduler"){
						if($custom_date == 0) {
							echo "<div>
								  <table>
								  <tr>
									<td>
									<img src='scheduler_read_write_ratio.php?service=$service&period=$timewindow' title =' Read/Write Requests Ratio'>
									</td>
								  <tr>
									<td align = 'center'>
									<b> Total number of requests </b>";
									include("scheduler_table_total.php");
							echo	" 	</td>
									<td align = 'center'>
									<b> Minimum queuing time of requests </b>";
									include("scheduler_table_min.php");
							echo	" 	</td>
								  </tr>
								  <tr>
									<td align = 'center'>
									<b> Maximum queuing time of requests </b>";
									include("scheduler_table_max.php");
							echo	" 	</td>
									<td align = 'center'>
									<b> Average queuing time of requests </b>";
									include("scheduler_table_avg.php");
							echo	" 	</td>
								  </tr>
								  </table>
								  </div>";
						}
						else {
							$from=$date1 . " " . $date1hour;
							$to=$date2 . " " . $date2hour; 
							echo "<div>
								  <table>
								  <tr>
									<td>
									<img src='scheduler_read_write_ratio.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title =' Read/Write Requests Ratio'>
									</td>
								  <tr>
                                                                        <td align = 'center'>
                                                                        <b> Total number of requests </b>";
                                                                        include("scheduler_table_total.php");
                                                        echo    "       </td>
                                                                        <td align = 'center'>
                                                                        <b> Minimum queuing time of requests </b>";
                                                                        include("scheduler_table_min.php");
                                                        echo    "       </td>
                                                                  </tr>
                                                                  <tr>
                                                                        <td align = 'center'>
                                                                        <b> Maximum queuing time of requests </b>";
                                                                        include("scheduler_table_max.php");
                                                        echo    "       </td>
                                                                        <td align = 'center'>
                                                                        <b> Average queuing time of requests </b>";
                                                                        include("scheduler_table_avg.php");
                                                        echo    "       </td>
                                                                  </tr>
                                                                  </table>
                                                                  </div>";
						}
					}
					else if($stat == "GC"){
						if($custom_date == 0) {
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='gc_total_number_ts.php?service=$service&period=$timewindow' title =' Total Number of Garbage Collected Files Timeseries'>
								</td>
								  </tr>
								  <tr>
									<td>
								 <img src ='gcfileage.php?service=$service&period=$timewindow' title =' File Age Distribution of Garbage Collected Files'>
								</td>
								  </tr>
								  <tr>
									<td>
								 <img src ='gcsizedistr.php?service=$service&period=$timewindow' title =' File Size Distribution of Garbage Collected Files'>
								</td>
								  </tr>
								  <tr>
									<td>
								 <img src ='gcreq.php?service=$service&period=$timewindow' title ='Files Requested (Tape Recalled) after Garbage Collection. Distribution of time intervals'>
								</td>
								  </tr>
								  </table>
								  </div>";
						}
						else {
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='gc_total_number_ts.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title =' Total Number of Garbage Collected Files Timeseries'>
								</td>
								  <tr>
									<td>
								 <img src ='gcfileage.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title =' File Age Distribution of Garbage Collected Files'>
								</td>
								  </tr>
								  <tr>
									<td>
								 <img src ='gcsizedistr.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title =' File Size Distribution of Garbage Collected Files'>
								</td>
								  </tr>
								  <tr>
									<td>
								 <img src ='gcreq.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title ='Files Requested (Tape Recalled) after Garbage Collection. Distribution of time intervals'>
								</td>
								  </tr>
								  </table>
								  </div>";
						}
					}
					else if($stat == "Migration"){
						if($custom_date ==0) {
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='migsperpool.php?service=$service&period=$timewindow' title =' Number of File Migrations per SvcClass'>
								</td>
								  </tr>
								  <tr>
									<td>
								 <img src ='migtimeseries.php?service=$service&period=$timewindow'>
								</td>
								  </tr>
								  </table>
								  </div>";
						}
						else {
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='migsperpool.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title =' Number of File Migrations per SvcClass'>
								</td>
								  </tr>
								  <tr>
									<td>
								 <img src ='migtimeseries.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour'>
								</td>
								  </tr>
								  </table>
								  </div>";
						}
					}
					else {
						if($custom_date == 0) {
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='pie_graph.php?service=$service&period=$timewindow'>
								</td>
								<td>
								 <img src ='timeseries.php?service=$service&period=$timewindow'>
								</td>
								  </tr>
								  </table>
								  </div>
								  <div>
								  <table>
									<tr>
								<td valign ='top'>
									<img src ='new_requests_ts.php?service=$service&period=$timewindow'>
								</td></tr>
								<tr><td valign ='top'>
								<b> Pool Transactions (Files Copied) </b>"; 
								include("pool_transaction.php");
								echo "</td>
									  </tr>
									  </table>
										  </div>";
						}
						else {
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='pie_graph.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour'>
								</td>
								<td>
								 <img src ='timeseries.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour'>
								</td>
								  </tr>
								  </table>
								  </div>
								  <div>
								  <table>
									<tr>
								<td valign ='top'>
									<img src ='new_requests_ts.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour'>
								</td></tr>
								<tr><td valign ='top'>
								<b> Pool Transactions (Files Copied) </b>";
								$from=$date1 . " " . $date1hour;
								$to=$date2 . " " . $date2hour; 
								include("pool_transaction.php");
								echo "</td>
									  </tr>
									  </table>
										  </div>";
						}
					}
						
				?>
		</tr></table>
</html>
