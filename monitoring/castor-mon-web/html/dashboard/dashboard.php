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
			  <a href='Default.html'>Back</a>";
		exit;
	}
	
?>
<?php 
 /* Define apropriate refresh time.
  * Define different appearance styles
  */?>

<html>
	<head>
		<META HTTP-EQUIV="Refresh" CONTENT="10; URL=dashboard.php?service=<?php echo $service;?>"> 
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
							<td><div id="ora"><?php echo $service?>: STATUS MONITOR </div></td>
						</tr>
						<tr>
							<td><div id="menu">
								<ul>	
									<li><h2>Statistics</h2>
									<ul>
									<li><a href="../stat/tabledet.php?timewindow=1&service=<?php echo $service;?>">General Statistics</a></li>
									<li><a href="../stat/tabledet.php?timewindow=1&service=<?php echo $service;?>&stat=FileReq">File Request Statistics</a></li>
									<li><a href="../stat/tabledet.php?timewindow=1&service=<?php echo $service;?>&stat=Migration">File Migration Statistics</a></li>
									<li><a href="../stat/tabledet.php?timewindow=1&service=<?php echo $service;?>&stat=Latency">Latency Statistics</a></li>
									<li><a href="../stat/tabledet.php?timewindow=1&service=<?php echo $service;?>&stat=File">File System Statistics</a></li>
									<li><a href="../stat/tabledet.php?timewindow=1&service=<?php echo $service;?>&stat=Tape">Tape Statistics</a></li>
									<li><a href="../stat/tabledet.php?timewindow=1&service=<?php echo $service;?>&stat=Scheduler">Scheduler Statistics</a></li>
									<li><a href="../stat/tabledet.php?timewindow=1&service=<?php echo $service;?>&stat=GC">Garbage Collection Statistics</a></li>
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
											<td colspan = 3 align = 'center' style='background-color: orangered'><b> Requests Monitor </b></td>
										</tr>
										<tr>
											<td align = 'center' style='background-color: orange'> File Read Requests Percentages(Total Instance) </td>
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
											<td align = 'center' style='background-color: orangered'><b> Migration Monitor </b></td>
											<td align = 'center' style='background-color: orangered'><b> Pool Transactions Monitor </b></td>
											<td align = 'center' style='background-color: orangered'><b> Garbage Collection Monitor </b></td>
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
