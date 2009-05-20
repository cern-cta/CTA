<?php
//main statistical page 
	error_reporting(E_ALL ^ E_NOTICE);
	include ("../../../conf/castor-mon-web/user.php");
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
	$svcclass = $_GET['svcclass'];
	$pattern5 = '/[a-zA-Z0-9]{1,15}/';
	preg_match($pattern5,$svcclass,$match5);
	$svcclass = $match5[0];
	if ($svcclass != NULL)
		$title_svcclass = "($svcclass)";
	else 
		$title_svcclass = "";
	$custom_date = 0;
	$no_svc = 0;
	if(($date1 != NULL)and($date2 != NULL)and($date1hour != NULL)and($date2hour != NULL)) $custom_date =1;
	if ($stat == NULL)
		$stat = "General"; 
	if ($timewindow == "all")
		$timewindow = "10000";
	$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
	if(!$conn) {
		$e = oci_error();
		print htmlentities($e['message']);
		exit;
	}
	$query1 = "select svcclass from ".$db_instances[$service]['schema'].".SVCCLASSMAP_MV order by svcclass";
	if (!($parsed1 = OCIParse($conn, $query1))) 
		{ echo "Error Parsing Query";exit();}
	if (!OCIExecute($parsed1))
		{ echo "Error Executing Query";exit();}
	$i = 0;
	//fetch data(different service classes)
	while (OCIFetch($parsed1)) {
		$svc[$i] = OCIResult($parsed1,1);
		$i++;		
	}
	if (empty($svc))
		$no_svc = 1; 
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
					if ($stat == "GenLat") {
					if ($custom_date == 1) {
							echo "<div id='ora'> $service $title_svcclass: General Latency Statistics - From: $date1 $date1hour To $date2 $date2hour</div>";
						}
						else {
							if($timewindow == "15/1440")
								echo "<div id='ora'> $service $title_svcclass: General Latency Statistics - Last Ten Minutes </div>";
							else if($timewindow == "1/24")
								echo "<div id='ora'> $service $title_svcclass: General Latency Statistics - Last Hour </div>";
							else if($timewindow == "1")
								echo "<div id='ora'> $service $title_svcclass: General Latency Statistics - Last Day </div>";
							else if($timewindow == "7")
								echo "<div id='ora'> $service $title_svcclass: General Latency Statistics - Last Week </div>";
							else if($timewindow == "30")
								echo "<div id='ora'> $service $title_svcclass: General Latency Statistics - Last Month </div>";
						}
					}
					else if ($stat == "GenScheduler") {
						if ($custom_date == 1) {
							echo "<div id='ora'> $service $title_svcclass: General Scheduler Statistics - From: $date1 $date1hour To $date2 $date2hour</div>";
						}
						else {
							if($timewindow == "15/1440")
								echo "<div id='ora'> $service $title_svcclass: General Scheduler Statistics - Last Ten Minutes </div>";
							else if($timewindow == "1/24")
								echo "<div id='ora'> $service $title_svcclass: General Scheduler Statistics - Last Hour </div>";
							else if($timewindow == "1")
								echo "<div id='ora'> $service $title_svcclass: General Scheduler Statistics - Last Day </div>";
							else if($timewindow == "7")
								echo "<div id='ora'> $service $title_svcclass: General Scheduler Statistics - Last Week </div>";
							else if($timewindow == "30")
								echo "<div id='ora'> $service $title_svcclass: General Scheduler Statistics - Last Month </div>";
						}
					}
					else if ($stat == "GenOther") {
						if ($custom_date == 1) {
							echo "<div id='ora'> $service $title_svcclass: Other General Statistics - From: $date1 $date1hour To $date2 $date2hour</div>";
						}
						else {
							if($timewindow == "15/1440")
								echo "<div id='ora'> $service $title_svcclass: Other General Statistics - Last Ten Minutes </div>";
							else if($timewindow == "1/24")
								echo "<div id='ora'> $service $title_svcclass: Other General Statistics - Last Hour </div>";
							else if($timewindow == "1")
								echo "<div id='ora'> $service $title_svcclass: Other General Statistics - Last Day </div>";
							else if($timewindow == "7")
								echo "<div id='ora'> $service $title_svcclass: Other General Statistics - Last Week </div>";
							else if($timewindow == "30")
								echo "<div id='ora'> $service $title_svcclass: Other General Statistics - Last Month </div>";
						}
					}	
					else if ($stat == "Tape") {
						if ($custom_date == 1) {
							echo "<div id='ora'> $service $title_svcclass: Tape Statistics - From: $date1 $date1hour To $date2 $date2hour</div>";
						}
						else {
							if($timewindow == "15/1440")
								echo "<div id='ora'> $service $title_svcclass: Tape Statistics - Last Ten Minutes </div>";
							else if($timewindow == "1/24")
								echo "<div id='ora'> $service $title_svcclass: Tape Statistics - Last Hour </div>";
							else if($timewindow == "1")
								echo "<div id='ora'> $service $title_svcclass: Tape Statistics - Last Day </div>";
							else if($timewindow == "7")
								echo "<div id='ora'> $service $title_svcclass: Tape Statistics - Last Week </div>";
							else if($timewindow == "30")
								echo "<div id='ora'> $service $title_svcclass: Tape Statistics - Last Month </div>";
						}
					}
					else if ($stat == "GC") {
						if ($custom_date == 1) {
							echo "<div id='ora'> $service $title_svcclass: Garbage Collection Statistics - From: $date1 $date1hour To $date2 $date2hour</div>";
						}
						else {
							if($timewindow == "15/1440")
								echo "<div id='ora'> $service $title_svcclass: Garbage Collection Statistics - Last Ten Minutes </div>";
							else if($timewindow == "1/24")
								echo "<div id='ora'> $service $title_svcclass: Garbage Collection Statistics - Last Hour </div>";
							else if($timewindow == "1")
								echo "<div id='ora'> $service $title_svcclass: Garbage Collection Statistics - Last Day </div>";
							else if($timewindow == "7")
								echo "<div id='ora'> $service $title_svcclass: Garbage Collection Statistics - Last Week </div>";
							else if($timewindow == "30")
								echo "<div id='ora'> $service $title_svcclass: Garbage Collection Statistics - Last Month </div>";
						}
					}
					else if ($stat == "File") {
						if ($custom_date == 1) {
							echo "<div id='ora'> $service $title_svcclass: File Read Requests Statistics - From: $date1 $date1hour To $date2 $date2hour</div>";
						}
						else {
							if($timewindow == "15/1440")
								echo "<div id='ora'> $service $title_svcclass: File Read Requests Statistics - Last Ten Minutes </div>";
							else if($timewindow == "1/24")
								echo "<div id='ora'> $service $title_svcclass: File Read Requests Statistics - Last Hour </div>";
							else if($timewindow == "1")
								echo "<div id='ora'> $service $title_svcclass: File Read Requests Statistics - Last Day </div>";
							else if($timewindow == "7")
								echo "<div id='ora'> $service $title_svcclass: File Read Requests Statistics - Last Week </div>";
							else if($timewindow == "30")
								echo "<div id='ora'> $service $title_svcclass: File Read Requests Statistics - Last Month </div>";
						}
					}
					else if ($stat == "Lat") {
						if ($custom_date == 1) {
							echo "<div id='ora'> $service $title_svcclass: Latency Statistics - From: $date1 $date1hour To $date2 $date2hour</div>";
						}
						else {
							if($timewindow == "15/1440")
								echo "<div id='ora'> $service $title_svcclass: Latency Statistics - Last Ten Minutes </div>";
							else if($timewindow == "1/24")
								echo "<div id='ora'> $service $title_svcclass: Latency Statistics - Last Hour </div>";
							else if($timewindow == "1")
								echo "<div id='ora'> $service $title_svcclass: Latency Statistics - Last Day </div>";
							else if($timewindow == "7")
								echo "<div id='ora'> $service $title_svcclass: Latency Statistics - Last Week </div>";
							else if($timewindow == "30")
								echo "<div id='ora'> $service $title_svcclass: Latency Statistics - Last Month </div>";
						}
					}
					else if ($stat == "Other") {
						if ($custom_date == 1) {
							echo "<div id='ora'> $service $title_svcclass: Other Statistics - From: $date1 $date1hour To $date2 $date2hour</div>";
						}
						else {
							if($timewindow == "15/1440")
								echo "<div id='ora'> $service $title_svcclass: Other Statistics - Last Ten Minutes </div>";
							else if($timewindow == "1/24")
								echo "<div id='ora'> $service $title_svcclass: Other Statistics - Last Hour </div>";
							else if($timewindow == "1")
								echo "<div id='ora'> $service $title_svcclass: Other Statistics - Last Day </div>";
							else if($timewindow == "7")
								echo "<div id='ora'> $service $title_svcclass: Other Statistics - Last Week </div>";
							else if($timewindow == "30")
								echo "<div id='ora'> $service $title_svcclass: Other Statistics - Last Month </div>";
						}
					}
					else {
						if ($custom_date == 1) {
							echo "<div id='ora'> $service $title_svcclass: General Request Statistics - From: $date1 $date1hour To $date2 $date2hour</div>";
						}
						else {				
							if($timewindow == "15/1440")
								echo "<div id='ora'> $service $title_svcclass: General Request Statistics - Last Ten Minutes </div>";
							else if($timewindow == "1/24")
								echo "<div id='ora'> $service $title_svcclass: General Request Statistics - Last Hour </div>";
							else if($timewindow == "1")
								echo "<div id='ora'> $service $title_svcclass: General Request Statistics - Last Day </div>";
							else if($timewindow == "7")
								echo "<div id='ora'> $service $title_svcclass: General Request Statistics - Last Week </div>";
							else if($timewindow == "30")
								echo "<div id='ora'> $service $title_svcclass: General Request Statistics - Last Month </div>";
						}
					}
					
						?>
				<table><tr><td colspan = "2" valign = "top">
				<div id="menu">
					<ul><li><a href="../dashboard/dashboard.php?service=<?php echo $service;?>">DASHBOARD</a></li></ul>
					<ul>
						<li><h2>Statistics (CASTOR Instance)</h2>
							<ul> 
							   <?php if ($custom_date == 0) {?>
								<li><a href="tabledet.php?timewindow=<?php echo $timewindow;?>&service=<?php echo $service;?>">General Request Statistics</a></li>
								<li><a href="tabledet.php?timewindow=<?php echo $timewindow;?>&service=<?php echo $service;?>&stat=GenLat">General Latency Statistics</a></li>
								<li><a href="tabledet.php?timewindow=<?php echo $timewindow;?>&service=<?php echo $service;?>&stat=GenScheduler">General Scheduler Statistics</a></li>
								<li><a href="tabledet.php?timewindow=<?php echo $timewindow;?>&service=<?php echo $service;?>&stat=GenOther">Other General Statistics</a></li>
								<li><a href="tabledet.php?timewindow=<?php echo $timewindow;?>&service=<?php echo $service;?>&stat=File">File Read Requests Statistics</a>
									<ul>
									<li><h3>SvcClass</h3></li>
									<?php
									if($no_svc == 0) {
										foreach($svc as $dpool) {
											echo "<li><a href='tabledet.php?timewindow=$timewindow&service=$service&stat=File&svcclass=$dpool'>$dpool</a></li>";
										}
									}
									else	{
										echo "<li><h2>No SvcClass Map Available</h2></li>";}
									?></ul>
								</li>
								<li><a href="tabledet.php?timewindow=<?php echo $timewindow;?>&service=<?php echo $service;?>&stat=Lat">Latency Statistics</a>
									<ul><li><h3>SvcClass</h3></li>
									<?php
									if($no_svc == 0) {
										foreach($svc as $dpool) {
											echo "<li><a href='tabledet.php?timewindow=$timewindow&service=$service&stat=Lat&svcclass=$dpool'>$dpool</a></li>";
										}
									}
									else	{
										echo "<li><h2>No SvcClass Map Available</h2></li>";}
									?></ul>
								</li>
								<li><a href="tabledet.php?timewindow=<?php echo $timewindow;?>&service=<?php echo $service;?>&stat=Tape">Tape Statistics</a>
									<ul><li><h3>SvcClass</h3></li>
									<?php
									if($no_svc == 0) {
										foreach($svc as $dpool) {
											echo "<li><a href='tabledet.php?timewindow=$timewindow&service=$service&stat=Tape&svcclass=$dpool'>$dpool</a></li>";
										}
									}
									else	{
										echo "<li><h2>No SvcClass Map Available</h2></li>";}
									?></ul>
								</li>
								<li><a href="tabledet.php?timewindow=<?php echo $timewindow;?>&service=<?php echo $service;?>&stat=GC">Garbage Collection Statistics</a>
									<ul><li><h3>SvcClass</h3></li>
									<?php
									if($no_svc == 0) {
										foreach($svc as $dpool) {
											echo "<li><a href='tabledet.php?timewindow=$timewindow&service=$service&stat=GC&svcclass=$dpool'>$dpool</a></li>";
										}
									}
									else	{
										echo "<li><h2>No SvcClass Map Available</h2></li>";}
									?></ul>
								</li>
								<li><a href="tabledet.php?timewindow=<?php echo $timewindow;?>&service=<?php echo $service;?>&stat=Other">Other Statistics</a>
									<ul><li><h3>SvcClass</h3></li>
									<?php
									if($no_svc == 0) {
										foreach($svc as $dpool) {
											echo "<li><a href='tabledet.php?timewindow=$timewindow&service=$service&stat=Other&svcclass=$dpool'>$dpool</a></li>";
										}
									}
									else	{
										echo "<li><h2>No SvcClass Map Available</h2></li>";}
									?></ul>
								</li>
							   <?} else { ?>
							   	<li><a href="tabledet.php?date1=<?php echo $date1;?>&date1hour=<?php echo $date1hour;?>&date2=<?php echo $date2;?>&date2hour=<?php echo $date2hour;?>&service=<?php echo $service;?>">General Request Statistics</a></li>
								<li><a href="tabledet.php?date1=<?php echo $date1;?>&date1hour=<?php echo $date1hour;?>&date2=<?php echo $date2;?>&date2hour=<?php echo $date2hour;?>&service=<?php echo $service;?>&stat=GenLat">General Latency Statistics</a></li>
								<li><a href="tabledet.php?date1=<?php echo $date1;?>&date1hour=<?php echo $date1hour;?>&date2=<?php echo $date2;?>&date2hour=<?php echo $date2hour;?>&service=<?php echo $service;?>&stat=GenScheduler">General Scheduler Statistics</a></li>
								<li><a href="tabledet.php?date1=<?php echo $date1;?>&date1hour=<?php echo $date1hour;?>&date2=<?php echo $date2;?>&date2hour=<?php echo $date2hour;?>&service=<?php echo $service;?>&stat=GenOther">Other General Statistcs</a></li>
								<li><a href="tabledet.php?date1=<?php echo $date1;?>&date1hour=<?php echo $date1hour;?>&date2=<?php echo $date2;?>&date2hour=<?php echo $date2hour;?>&service=<?php echo $service;?>&stat=File">File Read Request Statistics</a>
									    <ul><li><h3>SvcClass</h3></li>
									    <?php
									    	if ($no_svc == 0) {
											foreach($svc as $dpool) {
												echo "<li><a href='tabledet.php?date1=$date1&date1hour=$date1hour&date2=$date2&date2hour=$date2hour&service=$service&stat=File&svcclass=$dpool'>$dpool</a></li>";
											}
										}
										else {
											echo "<li><h2>No SvcClass Map Available</h2></li>";}
										?></ul>
								</li>
								<li><a href="tabledet.php?date1=<?php echo $date1;?>&date1hour=<?php echo $date1hour;?>&date2=<?php echo $date2;?>&date2hour=<?php echo $date2hour;?>&service=<?php echo $service;?>&stat=Lat">Latency Statistics</a>
									    <ul><li><h3>SvcClass</h3></li>
									    <?php
									    	if ($no_svc == 0) {
											foreach($svc as $dpool) {
												echo "<li><a href='tabledet.php?date1=$date1&date1hour=$date1hour&date2=$date2&date2hour=$date2hour&service=$service&stat=Lat&svcclass=$dpool'>$dpool</a></li>";
											}
										}
										else {
											echo "<li><h2>No SvcClass Map Available</h2></li>";}
										?></ul>

								</li>
								<li><a href="tabledet.php?date1=<?php echo $date1;?>&date1hour=<?php echo $date1hour;?>&date2=<?php echo $date2;?>&date2hour=<?php echo $date2hour;?>&service=<?php echo $service;?>&stat=Tape">Tape Statistics</a>
									    <ul><li><h3>SvcClass</h3></li>
									    <?php
									    	if ($no_svc == 0) {
											foreach($svc as $dpool) {
												echo "<li><a href='tabledet.php?date1=$date1&date1hour=$date1hour&date2=$date2&date2hour=$date2hour&service=$service&stat=Tape&svcclass=$dpool'>$dpool</a></li>";
											}
										}
										else {
											echo "<li><h2>No SvcClass Map Available</h2></li>";}
										?></ul>

								</li>
								<li><a href="tabledet.php?date1=<?php echo $date1;?>&date1hour=<?php echo $date1hour;?>&date2=<?php echo $date2;?>&date2hour=<?php echo $date2hour;?>&service=<?php echo $service;?>&stat=GC">Garbage Collection Statistics</a>
									    <ul><li><h3>SvcClass</h3></li>
									    <?php
									    	if ($no_svc == 0) {
											foreach($svc as $dpool) {
												echo "<li><a href='tabledet.php?date1=$date1&date1hour=$date1hour&date2=$date2&date2hour=$date2hour&service=$service&stat=GC&svcclass=$dpool'>$dpool</a></li>";
											}
										}
										else {
											echo "<li><h2>No SvcClass Map Available</h2></li>";}
										?></ul>

								</li>
								<li><a href="tabledet.php?date1=<?php echo $date1;?>&date1hour=<?php echo $date1hour;?>&date2=<?php echo $date2;?>&date2hour=<?php echo $date2hour;?>&service=<?php echo $service;?>&stat=Other">Other Statistics</a>
									    <ul><li><h3>SvcClass</h3></li>
									    <?php
									    	if ($no_svc == 0) {
											foreach($svc as $dpool) {
												echo "<li><a href='tabledet.php?date1=$date1&date1hour=$date1hour&date2=$date2&date2hour=$date2hour&service=$service&stat=Other&svcclass=$dpool'>$dpool</a></li>";
											}
										}
										else {
											echo "<li><h2>No SvcClass Map Available</h2></li>";}
										?></ul>

								</li>
							    <? }?>
							</ul>
						</li>
					</ul>
				</div>
				</td></tr>
				<tr><td valign = "top">
					<?php if($stat == NULL) { 
						if ($svcclass == NULL)
							echo "<form action='tabledet.php?service=$service' method='post'>";
						else 
							echo "<form action='tabledet.php?service=$service&svcclass=$svcclass' method='post'>";
					      }
					      else{ 
					        if ($svcclass == NULL)
							echo "<form action='tabledet.php?service=$service&stat=$stat' method='post'>";
						else 
							echo "<form action='tabledet.php?service=$service&svcclass=$svcclass&stat=$stat' method='post'>";
					      };?>
					<b> Fixed Timewindow: </b>
					<select name="timewindow">
	  				<option onclick="javascript:this.form.submit()" value ="15/1440" selected = "selected">Last 10 minutes</option>
	  				<option onclick="javascript:this.form.submit()" value ="1/24">Last Hour</option>
	  				<option onclick="javascript:this.form.submit()" value ="1" >Last Day</option>
	  				<option onclick="javascript:this.form.submit()" value ="7">Last Week</option>
	  				<option onclick="javascript:this.form.submit()" value ="30" >Last Month</option>
	  				</select>
					</form>
				<tr><td valign = "top">
					<?php if($stat == NULL) { 
						if ($svcclass == NULL)
							echo "<form action='tabledet.php?service=$service' method='post'>";
						else 
							echo "<form action='tabledet.php?service=$service&svcclass=$svcclass' method='post'>";
					      }
					      else{ 
					        if ($svcclass == NULL)
							echo "<form action='tabledet.php?service=$service&stat=$stat' method='post'>";
						else 
							echo "<form action='tabledet.php?service=$service&svcclass=$svcclass&stat=$stat' method='post'>";
					      };?>
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
					if($stat == "GenLat"){
						if ($custom_date == 0) {
							echo "<div>
								  <table>
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
					else if($stat == "GenScheduler"){
						if($custom_date == 0) {
							echo "<div>
								  <table>
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
					else if($stat == "GenOther"){
						if($custom_date == 0) {
							echo "<div>
								  <table>
								  <tr>
								  <td align = 'center'><b>Recalled Files per Tape Mount</b></td>
								  <td align = 'center'><b>Number of Garbage Collected Files (Timeseries)</b></td>
								  </tr>
								  <tr><td align = 'center'>
									<img src='filesmount.php?service=$service&period=$timewindow' title =' Number of files recalled from a tape volume during a single mount. Number of Recalled files Distribution'>
								  </td>
								  <td align = 'center'>
								   <img src ='gc_total_number_ts.php?service=$service&period=$timewindow' title =' Total Number of Garbage Collected Files Timeseries'>
								  </td>
								  </tr>
								  </table>
								  </div>";
						}
						else { 
							echo "<div>
								  <table>
								  <tr>
								  <td align = 'center'><b>Recalled Files per Tape Mount</b></td>
								  <td align = 'center'><b>Number of Garbage Collected Files (Timeseries)</b></td>
								  </tr>
								  <tr><td align = 'center'>
									<img src='filesmount.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title =' Number of files recalled from a tape volume during a single mount. Number of Recalled files Distribution'>
								  </td>
								  <td align = 'center'>
								   <img src ='gc_total_number_ts.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title =' Total Number of Garbage Collected Files Timeseries'>
								  </td>
								  </tr>
								  </table>
								  </div>";
						}
					}
					else if($stat == "File") {
						if ($custom_date ==0) {
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='pie_graph.php?service=$service&period=$timewindow&svcclass=$svcclass'>
								</td>
								<td>
								 <img src ='timeseries.php?service=$service&period=$timewindow&svcclass=$svcclass'>
								</td>
								  </tr>
								  <tr>
								<td>
								 <img src ='topusers.php?service=$service&period=$timewindow&svcclass=$svcclass' title = 'Top Users - File requests \n File requests are distinguished into three categories diskhits, \ncopies from another svcclass and tape recalls'>
								</td>
								<td>
									<img src ='prestage_users.php?service=$service&period=$timewindow&svcclass=$svcclass' title = 'Direct and Prestaged File requests per User. We focus on tape recalled files'>
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
								 <img src ='pie_graph.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour&svcclass=$svcclass'>
								</td>
								<td>
								 <img src ='timeseries.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour&svcclass=$svcclass'>
								</td>
								  </tr>
								  <tr>
								<td>
								 <img src ='topusers.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour&svcclass=$svcclass' title = 'Top Users - File requests \n File requests are distinguished into three categories diskhits, \ncopies from another svcclass and tape recalls'>
								</td>
								<td>
									<img src ='prestage_users.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour&svcclass=$svcclass' title = 'Direct and Prestaged File requests per User. We focus on tape recalled files'>
								</td>
								</tr>
								</table>
								</div>";
					}
					else if($stat == "Lat"){
						if ($custom_date == 0) {
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='latencies.php?service=$service&period=$timewindow&svcclass=$svcclass' title ='Total Latency from the arrival of a new request until the file is returned to the user.\n (TotalWaitTime summary message from dlf)'>
								</td>
								  </tr>
								  <tr>
									<td>
								 <img src ='miglat.php?service=$service&period=$timewindow&svcclass=$svcclass' title ='Total Latency until the file is migrated to tape.\n (TotalWaitTime summary message from dlf)'>
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
								 <img src ='latencies.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour&svcclass=$svcclass' title ='Total Latency from the arrival of a new request until the file is returned to the user.\n (TotalWaitTime summary message from dlf)'>
								</td>
								  </tr>
								  <tr>
									<td>
								 <img src ='miglat.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour&svcclass=$svcclass' title ='Total Latency until the file is migrated to tape.\n (TotalWaitTime summary message from dlf)'>
								</td>
								  </tr>
								  </table>
								  </div>
								";
					}
					else if($stat == "Tape"){
						if($custom_date == 0) {
							echo "<div>
							       <table>
								 <tr>
							    	  <td align = 'center'>
								   <img src ='toptapes.php?service=$service&period=$timewindow&svcclass=$svcclass' title=' Files Recalled per Tape Volume irrespective of tape mount status(Usage per Tape Volume). \n Detailed Graphs per SvcClass'>
								  </td>
								 <tr>
								  <td align = 'center'>
								   <img src='usertapecontribution.php?service=$service&period=$timewindow&svcclass=$svcclass' title =' File requests per User, where file was recalled from tape and the tape was not already mounted.'>
								  </td>
								 </tr>
								 <tr>
								  <td>
								   <img src ='sizedistr.php?service=$service&period=$timewindow&svcclass=$svcclass' title='File Size Distribution of Tape Recalled Files. Detailed Graphs per SvcClass'>
								  </td>
								 </tr>
							       </table>
							      </div>
								  ";
						}
						else {
							echo "<div>
							       <table>
								 <tr>
							    	  <td align = 'center'>
								   <img src ='toptapes.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour&svcclass=$svcclass' title=' Files Recalled per Tape Volume irrespective of tape mount status(Usage per Tape Volume). \n Detailed Graphs per SvcClass'>
								  </td>
								 <tr>
								  <td align = 'center'>
								   <img src='usertapecontribution.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour&svcclass=$svcclass' title =' File requests per User, where file was recalled from tape and the tape wasn't already mounted.\n Thus User Tape Mount Contribution'>
								  </td>
								 </tr>
								 <tr>
								  <td>
								   <img src ='sizedistr.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour&svcclass=$svcclass' title='File Size Distribution of Tape Recalled Files. Detailed Graphs per SvcClass'>
								  </td>
								 </tr>
							       </table>
							      </div>
								  ";
						}
					}
					else if($stat == "GC"){
						if($custom_date == 0) {
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='gcfileage.php?service=$service&period=$timewindow&svcclass=$svcclass' title =' File Age Distribution of Garbage Collected Files'>
								</td>
								  </tr>
								  <tr>
									<td>
								 <img src ='gcsizedistr.php?service=$service&period=$timewindow&svcclass=$svcclass' title =' File Size Distribution of Garbage Collected Files'>
								</td>
								  </tr>
								  <tr>
									<td>
								 <img src ='gcreq.php?service=$service&period=$timewindow&svcclass=$svcclass' title ='Files Requested (Tape Recalled) after Garbage Collection. Distribution of time intervals'>
								</td>
								  </tr></table>
								  </div>";
						}
						else {
							echo "<div>
								  <table>
								  <tr>
									<td>
								 <img src ='gcfileage.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour&svcclass=$svcclass' title =' File Age Distribution of Garbage Collected Files'>
								</td>
								  </tr>
								  <tr>
									<td>
								 <img src ='gcsizedistr.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour&svcclass=$svcclass' title =' File Size Distribution of Garbage Collected Files'>
								</td>
								  </tr>
								  <tr>
									<td>
								 <img src ='gcreq.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour&svcclass=$svcclass' title ='Files Requested (Tape Recalled) after Garbage Collection. Distribution of time intervals'>
								</td>
								  </tr>
								  </table>
								  </div>";
						}
					}
					else if($stat == "Other"){
						if($custom_date == 0) {
							echo "<div>
								  <table>
								  <tr>
								    <td align = 'center'><b> Scheduler Read/Write Ratio (timeseries)</b></td>
								    <td align = 'center'><b> Number of Migrated Files (timeseries)</b></td>
								  </tr>
								  <tr>
								    <td align = 'center'>
									<img src='scheduler_read_write_ratio.php?service=$service&period=$timewindow&svcclass=$svcclass' title =' Read/Write Requests Ratio'>
								    </td>
								    <td align = 'center'>
								    	<img src ='migtimeseries.php?service=$service&period=$timewindow&svcclass=$svcclass'>
								    </td>
								  </tr>
								  </table>
							      </div>";
						}
						else {
							echo "<div>
								  <table>
								  <tr>
								    <td align = 'center'><b> Scheduler Read/Write Ratio (timeseries)</b></td>
								    <td align = 'center'><b> Number of Migrated Files (timeseries)</b></td>
								  </tr>
								  <tr>
								    <td align = 'center'>
									<img src='scheduler_read_write_ratio.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour&svcclass=$svcclass' title =' Read/Write Requests Ratio'>
								    </td>
								    <td align = 'center'>
								    	<img src ='migtimeseries.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour&svcclass=$svcclass'>
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
								   	<img src ='pie_req.php?service=$service&period=$timewindow'>
								   </td>
								   <td>
								 	<img src ='new_requests_ts.php?service=$service&period=$timewindow'>
								   </td>
								  </tr>
								  <tr>
								   <td>
								 	<img src ='requestsperpool.php?service=$service&period=$timewindow' title='File requests per SvcClass \n Three Categories of File requests \n a) File Present on Disk (DiskHit) \n b) File Copied from another ScvClass \n c) File Recalled from Tape'>
								   </td>
								   <td valign ='top'>
								     <b> SvcClass Transactions (D2D File Copies) </b>"; 
								     include("pool_transaction.php");
								     echo "</td>
								  </tr>
								  <tr>
								   <td valign ='top'>
									<img src ='prefetch.php?service=$service&period=$timewindow' title = 'Direct and Prestaged File requests per SvcClass. We focus on tape recalled files.'>
								   </td>
								   <td valign ='top'>
									<img src ='migsperpool.php?service=$service&period=$timewindow'>
								   </td>
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
								   	<img src ='pie_req.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour'>
								   </td>
								   <td>
								 	<img src ='new_requests_ts.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour'>
								   </td>
								  </tr>
								  <tr>
								   <td>
								 	<img src ='requestsperpool.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title='File requests per SvcClass \n Three Categories of File requests \n a) File Present on Disk (DiskHit) \n b) File Copied from another ScvClass \n c) File Recalled from Tape'>
								   </td>
								   <td valign ='top'>
								     <b> SvcClass Transactions (D2D File Copies) </b>"; 
								     include("pool_transaction.php");
								     echo "</td>
								  </tr>
								  <tr>
								   <td valign ='top'>
									<img src ='prefetch.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour' title = 'Direct and Prestaged File requests per SvcClass. We focus on tape recalled files.'>
								   </td>
								   <td valign ='top'>
									<img src ='migsperpool.php?service=$service&from=$date1 $date1hour&to=$date2 $date2hour'>
								   </td>
								  </tr>
								 </table>
							    </div>";
						}
					}	
				?>
		</tr></table>
</html>
