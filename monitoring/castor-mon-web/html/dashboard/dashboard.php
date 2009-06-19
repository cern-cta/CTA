<?php
/*Dashboard Feature. Used to monitor CASTOR's status
 *Requests (DiskHits, External DiskCopies, TapeRecalls)
 *Migrated Files
 *Garbage Collected Files
 *Pool - to - Pool Transactions (Internal / External DiskCopies)
 */
?>
<?php
	include('../../../conf/castor-mon-web/user.php');
	error_reporting(E_ALL ^ E_NOTICE);
	$service = $_REQUEST['service'];
	if ($db_instances[$service]['username']==NULL){
		echo "<p><b>Unknown Instance: $service</b></p>
			  <a href='../Default.html'>Back</a>";
		exit;
	}
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
<?php 
 /* Define apropriate refresh time.
  * Define different appearance styles
  */?>

<html>
	<head>
		<META HTTP-EQUIV="Refresh" CONTENT="300; URL=dashboard.php?service=<?php echo $service;?>"> 
		<LINK rel="stylesheet" type="text/css" title="compact" href="../lib/hmenu.css">
		<style>
			a {
			color: #000;
			background: #fff;
			text-decoration: none;
			}
			td a.outer {
			color: #000;
			background: #C0C0C0;
			text-decoration: none;
			}
			td a.outer:hover {
			color: #a00;
			background: #C0C0C0;
			}
			td a.inner {
			color: #000;
			text-decoration: none;
			}
			td a.inner:hover {
			color: #a00;
			}
			
			a:hover {
			color: #a00;
			background: #fff;
			}
		</style>
		<style>
			#ora {

				font: bold 30px/50px arial, helvetica, sans-serif;
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
		#menu ul li {float: left; width: 100%;}
		#menu ul li a {height: 1%;} 
		
		#menu a, #menu h2 {
			font: bold 0.7em/1.4em arial, helvetica, sans-serif;
		} 
		</style>
		<![endif]-->
	</head>
	<body>
		<table>
			<tr valign = "top">
				<td>
					<table>
						<tr valign = "top" width = "5%">
							<td><div id="ora"><?php echo $service?>: STATUS MONITOR</div></td>
						</tr>
						<tr>
							<td><div id="menu">
							<ul><li><a href="../exp_mon/exp_dashboard.php?service=<?php echo $service;?>">EXPERIMENTS DASHBOARD</a></li></ul>
								<ul>
						<li><h2>Statistics (CASTOR Instance)</h2>
							<ul> 
								<li><a href="../stat/tabledet.php?timewindow=1&service=<?php echo $service;?>">General Request Statistics</a></li>
								<li><a href="../stat/tabledet.php?timewindow=1&service=<?php echo $service;?>&stat=GenLat">General Latency Statistics</a></li>
								<li><a href="../stat/tabledet.php?timewindow=1&service=<?php echo $service;?>&stat=GenScheduler">General Scheduler Statistics</a></li>
								<li><a href="../stat/tabledet.php?timewindow=1&service=<?php echo $service;?>&stat=GenOther">Other General Statistics</a></li>
								<li><a href="../stat/tabledet.php?timewindow=1&service=<?php echo $service;?>&stat=File">File Read Requests Statistics</a>
									<ul>
									<li><h3>SvcClass</h3></li>
									<?php
									if($no_svc == 0) {
										foreach($svc as $dpool) {
											echo "<li><a href='../stat/tabledet.php?timewindow=1&service=$service&stat=File&svcclass=$dpool'>$dpool</a></li>";
										}
									}
									else	{
										echo "<li><h2>No SvcClass Map Available</h2></li>";}
									?></ul>
								</li>
								<li><a href="../stat/tabledet.php?timewindow=1&service=<?php echo $service;?>&stat=Lat">Latency Statistics</a>
									<ul><li><h3>SvcClass</h3></li>
									<?php
									if($no_svc == 0) {
										foreach($svc as $dpool) {
											echo "<li><a href='../stat/tabledet.php?timewindow=1&service=$service&stat=Lat&svcclass=$dpool'>$dpool</a></li>";
										}
									}
									else	{
										echo "<li><h2>No SvcClass Map Available</h2></li>";}
									?></ul>
								</li>
								<li><a href="../stat/tabledet.php?timewindow=1&service=<?php echo $service;?>&stat=Tape">Tape Statistics</a>
									<ul><li><h3>SvcClass</h3></li>
									<?php
									if($no_svc == 0) {
										foreach($svc as $dpool) {
											echo "<li><a href='../stat/tabledet.php?timewindow=1&service=$service&stat=Tape&svcclass=$dpool'>$dpool</a></li>";
										}
									}
									else	{
										echo "<li><h2>No SvcClass Map Available</h2></li>";}
									?></ul>
								</li>
								<li><a href="../stat/tabledet.php?timewindow=1&service=<?php echo $service;?>&stat=GC">Garbage Collection Statistics</a>
									<ul><li><h3>SvcClass</h3></li>
									<?php
									if($no_svc == 0) {
										foreach($svc as $dpool) {
											echo "<li><a href='../stat/tabledet.php?timewindow=1&service=$service&stat=GC&svcclass=$dpool'>$dpool</a></li>";
										}
									}
									else	{
										echo "<li><h2>No SvcClass Map Available</h2></li>";}
									?></ul>
								</li>
								<li><a href="../stat/tabledet.php?timewindow=1&service=<?php echo $service;?>&stat=Other">Other Statistics</a>
									<ul><li><h3>SvcClass</h3></li>
									<?php
									if($no_svc == 0) {
										foreach($svc as $dpool) {
											echo "<li><a href='../stat/tabledet.php?timewindow=1&service=$service&stat=Other&svcclass=$dpool'>$dpool</a></li>";
										}
									}
									else	{
										echo "<li><h2>No SvcClass Map Available</h2></li>";}
									?></ul>
								</li>
							</ul>
						</li>
					</ul>
							</div></td>
						</tr>
						<tr><td>
						<table>
							<tr>
								<td valign="top">
									<table>
										<tr>
											<td colspan = 3 align = 'center' style='background-color: orangered'><b> Requests Monitor</b></td>
										</tr>
										<tr>
											<td align = 'center' style='background-color: orange'> File Read Requests Percentages (Total Instance) </td>
											<td align = 'center' style='background-color: orange'> File Read Requests Counters per SvcClass </td>
											<td align = 'center' style='background-color: orange'>New Incoming Requests Monitor (Top 5)</td>
										</tr>
										<tr>
											<td align = "center">
												<img src="pie_graph1.php?service=<?php echo $service?>">
											</td>
											<td align ="center">
												<?php include("svcclassmonitor.php");?>
											</td>
											<td align ="center">
												<?php include("newreqmonitor.php");?>
											</td>
										</tr>
										</tr>
										<tr>
											<td align = 'center' style='background-color: orangered'><b> Migration Monitor</b></td>
											<td align = 'center' style='background-color: orangered'><b> Pool Transactions Monitor</b></td>
											<td align = 'center' style='background-color: orangered'><b> Garbage Collection Monitor</b></td>
										</tr>
										<tr>
											<td  align = 'center' style='background-color: orange'> Migration Counters per SvcClass</td>
											<td  align = 'center' style='background-color: orange'> External/Internal DiskCopy Counters</td>
											<td align = 'center' style='background-color: orange'>GC Counters </td>	
										<tr>
											<td align ="center">
												<?php include("migsmonitor.php");?>
											</td>
											<td align ="center">
												<?php include("pool_transaction_special.php");?>
											</td>
											<td align ="left">
												<?php include("gcmonitor.php");?>
											</td>
											<td></td>
										</tr>
										<tr>
											<td align = 'center' style='background-color: orangered'><b> Error Monitor (Last 10 minutes) </b></td>
										</tr>
										<tr>
											<td align ="center">
												<?php include("error_monitor.php");?>
											</td>
										</tr>
										<tr>
											<td colspan = 3><hr size='1'/></td>
										</tr>
										<tr>
											<td colspan = 3><font color = red size = 2>All graphs and tables contain data referring to a 5 min interval. There is a latency of 5 min between monitoring data and incoming log messages. The page gets refreshed every 5 minutes</td>
										</tr>
									</table>
								</td>
							 </tr>
					 </table></td>
					</tr>
					</table>
				</td>
			</tr>
		</table>
	</body>
</html>
