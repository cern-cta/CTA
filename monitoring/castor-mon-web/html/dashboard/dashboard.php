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
			  <a href='main.html'>Back</a>";
		exit;
	}
	
?>
<?php 
 /* Define apropriate refresh time.
  * Define different appearance styles
  */?>

<html>
	<head>
		<META HTTP-EQUIV="Refresh" CONTENT="10; URL=http://project-castor-monitoring.web.cern.ch/project-castor-monitoring/dashboard/dashboard.php?service=<?php echo $service;?>"> 
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
								<ul><li><a href="http://project-castor-monitoring.web.cern.ch/project-castor-monitoring/dashboard/main.html" >HOME</a></li></ul>
								<ul>
									<li><a href="../stat/tabledet.php?service=<?php echo $service;?>">Statistics</a></li>
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
							</div></td>
						</tr>
						<tr><td>
						<table>
							<tr>
								<td valign="top">
									<table>
										<tr>
											<td colspan = 2 align = 'center' style='background-color: orangered'><b> Requests Monitor </b></td>
										</tr>
										<tr>
											<td align = 'center' style='background-color: orange'> Requests Percentages(Total Instance) </td>
											<td align = 'center' style='background-color: orange'> Requests Counters per SvcClass </td>
										<tr>
											<td align = "center">
												<img src="pie_graph1.php?service=<?php echo $service?>">
											</td>
											<td align ="left">
												<?php include("svcclassmonitor.php");?>
											</td>
										</tr>
										<tr>
											<td align = 'center' style='background-color: orangered'><b> Migration Monitor </b></td>
											<td align = 'center' style='background-color: orangered'><b> Pool Transactions Monitor </b></td>
										</tr>
										<tr>
											<td  align = 'center' style='background-color: orange'> Migration Counters per SvcClass</td>
											<td  align = 'center' style='background-color: orange'> External/Internal DiskCopy Counters</td>
										<tr>
											<td align ="center">
												<?php include("migsmonitor.php");?>
											</td>
											<td align ="center">
												<?php include("pool_transaction_special.php");?>
											</td>
										</tr>
										<tr>
											<td align = 'center' style='background-color: orangered'><b> GC Monitor </b></td>
											<td></td>
										</tr>
										<tr>
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
