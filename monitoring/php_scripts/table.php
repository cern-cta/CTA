/******************************************************************************
 *              table.php
 *
 * This file is part of the Castor Monitoring project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2008  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * Author Theodoros Rekatsinas, 
 *****************************************************************************/
<?php
	error_reporting(E_ALL ^ E_NOTICE);
	$timewindow = $_REQUEST["timewindow"];
	if ($timewindow == "all")
		$timewindow = "10000";
	else if($timewindow == NULL)
		$timewindow = "7";
?>
<html>
	<head>
		<LINK rel="stylesheet" type="text/css" title="compact" href="hmenu.css">
		<style>
			a {
			color: #000;
			background: #fff;
			text-decoration: none;
			}

			a:hover {
			color: #a00;
			background: #fff;
			}
		</style>
		<!--[if IE]>
		<style type="text/css" media="screen">	
		body {
			behavior: url(csshover.htc);
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
		<table><tr valign = "top">
			<td valign = "top" width = "5%">
				<img src="casmon.bmp" width=61" height="500">
			</td>
			<td>
					<div id="menu">
						<ul><li><a href="http://cern.ch/castor-monitoring" target="new">HOME</a></li></ul>
						<ul>
							<li><h2>Experiment</h2>
								<ul>
									<li><a href="tableatlas.php">ATLAS</a></li>
									<li><a href="">CMS</a></li>
									<li><a href="">ALICE</a></li>
									<li><a href="">LHCB</a></li>
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
					<form action="table.php" method="post">
					<b> Timewindow: </b>
					<select name="timewindow">
  				<option value ="10/1440" selected = "selected">Last 10 minutes</option>
  				<option value ="1/24">Last Hour</option>
  				<option value ="1" >Last Day</option>
  				<option value ="7">Last Week</option>
  				<option value ="30" >Last Month</option>
					<option value ="all" > Everything</option>
  				</select>
  				<input type="submit" name = "submit" value = "Print">
				</div>
				<div>
					<table>
							<tr>
								<table>
								<td valign="top">
									<table>
										<tr ><td align = "center"><a href="tableatlas.php"><b>ATLAS</b></a></td></tr>
										<tr>
										<?php echo "<td><img src ='pie_graph.php?period=$timewindow'></td>"?>
										</tr>
										<tr>
										<?php echo "<td><img src ='timeseries.php?period=$timewindow'></td>"?>
										</tr>
									</table>
								</td>
								<td valign="top" >
									<table>
										<tr ><td align = "center"><a href=""><b>CMS</b></a></td></tr>
										<tr>
										
										</tr>
										<tr>
										
										</tr>
									</table>
								</td>
							</tr>
							<tr>
								<td valign="top">
									<table>
										<tr ><td align = "center"><a href=""><b>LHCB</b></a></td></tr>
										<tr>
										
										</tr>
										<tr>
										
										</tr>
									</table>
								</td>
								<td valign="top">
									<table>
										<tr><td valign="top" align = "center"><a href=""><b>ALICE</b></a></td></tr>
										<tr>
										
										</tr>
										<tr>
										
										</tr>
									</table>
								</td>
							</tr>
						</table>
					</td>
				</div>
		</tr></table>
</html>
