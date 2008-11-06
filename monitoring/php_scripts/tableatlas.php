/******************************************************************************
 *              tableatlas.php
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
	$stat = $_GET["stat"];
	$det = $_GET["det"];
	if ($stat == NULL)
		$stat = "General"; 
	if ($timewindow == "all")
		$timewindow = "10000";
	else if($timewindow == NULL)
		$timewindow = "7";
?>
<html>
	<head>
		<LINK rel="stylesheet" type="text/css" title="compact" href="hmenu.css">
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
			font: bold 30px/50px arial, helvetica, sans-serif;
			color: #fff;
			background: orangered;
			text-transform: uppercase;
		}
		</style>
		<!--[if IE]>
		<style type="text/css" media="screen">	
		body {
			behavior: url(csshover.htc);
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
			<td valign = "top" width = "5%">
				<img src="castoratlas.bmp" width="61" height="700">
			</td>
			<td valign = "top">
				<?php 
					if ($stat == "FileReq") {
						if($timewindow == "10/1440")
							echo "<div id='ora'> File Request Statistics - Last Ten Minutes </div>";
						else if($timewindow == "1/24")
							echo "<div id='ora'> File Request Statistics - Last Hour </div>";
						else if($timewindow == "1")
							echo "<div id='ora'> File Request Statistics - Last Day </div>";
						else if($timewindow == "7")
							echo "<div id='ora'> File Request Statistics - Last Week </div>";
						else if($timewindow == "30")
							echo "<div id='ora'> File Request Statistics - Last Month </div>";
						else 
							echo "<div id='ora'> File Request Statistics - Everything </div>";
					}
					else if ($stat == "Latency") {
						if($timewindow == "10/1440")
							echo "<div id='ora'> Latency Statistics - Last Ten Minutes </div>";
						else if($timewindow == "1/24")
							echo "<div id='ora'> Latency Statistics - Last Hour </div>";
						else if($timewindow == "1")
							echo "<div id='ora'> Latency Statistics - Last Day </div>";
						else if($timewindow == "7")
							echo "<div id='ora'> Latency Statistics - Last Week </div>";
						else if($timewindow == "30")
							echo "<div id='ora'> Latency Statistics - Last Month </div>";
						else 
							echo "<div id='ora'> Latency Statistics - Everything </div>";
					}
					else if ($stat == "File") {
						if($timewindow == "10/1440")
							echo "<div id='ora'> File System Statistics - Last Ten Minutes </div>";
						else if($timewindow == "1/24")
							echo "<div id='ora'> File System Statistics - Last Hour </div>";
						else if($timewindow == "1")
							echo "<div id='ora'> File System Statistics - Last Day </div>";
						else if($timewindow == "7")
							echo "<div id='ora'> File System Statistics - Last Week </div>";
						else if($timewindow == "30")
							echo "<div id='ora'> File System Statistics - Last Month </div>";
						else 
							echo "<div id='ora'> File System Statistics - Everything </div>";
					}	
					else if ($stat == "Tape") {
						if($timewindow == "10/1440")
							echo "<div id='ora'> Tape Statistics - Last Ten Minutes </div>";
						else if($timewindow == "1/24")
							echo "<div id='ora'> Tape Statistics - Last Hour </div>";
						else if($timewindow == "1")
							echo "<div id='ora'> Tape Statistics - Last Day </div>";
						else if($timewindow == "7")
							echo "<div id='ora'> Tape Statistics - Last Week </div>";
						else if($timewindow == "30")
							echo "<div id='ora'> Tape Statistics - Last Month </div>";
						else 
							echo "<div id='ora'> Tape Statistics - Everything </div>";
					}
					else if ($stat == "GC") {
						if($timewindow == "10/1440")
							echo "<div id='ora'> Garbage Collection Statistics - Last Ten Minutes </div>";
						else if($timewindow == "1/24")
							echo "<div id='ora'> Garbage Collection Statistics - Last Hour </div>";
						else if($timewindow == "1")
							echo "<div id='ora'> Garbage Collection Statistics - Last Day </div>";
						else if($timewindow == "7")
							echo "<div id='ora'> Garbage Collection Statistics - Last Week </div>";
						else if($timewindow == "30")
							echo "<div id='ora'> Garbage Collection Statistics - Last Month </div>";
						else 
							echo "<div id='ora'> Garbage Collection Statistics - Everything </div>";
					}
					else if ($stat == "Migration") {
						if($timewindow == "10/1440")
							echo "<div id='ora'> File Migration Statistics - Last Ten Minutes </div>";
						else if($timewindow == "1/24")
							echo "<div id='ora'> File Migration Statistics - Last Hour </div>";
						else if($timewindow == "1")
							echo "<div id='ora'> File Migration Statistics - Last Day </div>";
						else if($timewindow == "7")
							echo "<div id='ora'> File Migration Statistics - Last Week </div>";
						else if($timewindow == "30")
							echo "<div id='ora'> File Migration Statistics - Last Month </div>";
						else 
							echo "<div id='ora'> File Migration Statistics - Everything </div>";
					}
					else {
						if($timewindow == "10/1440")
							echo "<div id='ora'> General Statistics - Last Ten Minutes </div>";
						else if($timewindow == "1/24")
							echo "<div id='ora'> General Statistics - Last Hour </div>";
						else if($timewindow == "1")
							echo "<div id='ora'> General Statistics - Last Day </div>";
						else if($timewindow == "7")
							echo "<div id='ora'> General Statistics - Last Week </div>";
						else if($timewindow == "30")
							echo "<div id='ora'> General Statistics - Last Month </div>";
						else 
							echo "<div id='ora'> General Statistics - Everything </div>";
					}
					
						?>
				<table><tr><td valign = "top">
				<div id="menu">
					<ul><li><a href="table.php">HOME</a></li></ul>
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
						<li><h2>Statistics</h2>
							<ul>
								<li><a href="tableatlas.php">General Statistics</a></li>
								<li><a href="tableatlas.php?stat=FileReq">File Request Statistics</a></li>
								<li><a href="tableatlas.php?stat=Migration">File Migration Statistics</a></li>
								<li><a href="tableatlas.php?stat=Latency">Latency Statistics</a></li>
								<li><a href="tableatlas.php?stat=File">File System Statistics</a></li>
								<li><a href="fileexplorer.html" target="_blank">File System Overview</a></li>
								<li><a href="tableatlas.php?stat=Tape">Tape Statistics</a></li>
								<li><a href="tableatlas.php?stat=GC">Garbage Collection Statistics</a></li>
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
				</td>
				<td valign = "top">
					<?php if($stat == NULL) 
						echo "<form action='tableatlas.php' method='post'>";
					      else 
					        echo "<form action='tableatlas.php?stat=$stat' method='post'>";?>
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
				</td></tr></table>
				<?php
					if($stat == "FileReq") {
						echo "<div>
						      <table>
						      <tr>
						      	<td>
							 <img src ='requestsperpool.php?period=$timewindow'>
							</td>
							<td>
							 <img src ='topusers.php?period=$timewindow'>
							</td>
						      </tr>
						      </table>
						      </div>
						      <div>
						      <table>
						      	<tr>
							<td valign ='top'>
								<img src ='prefetch.php?period=$timewindow'>
							</td>
							<td valign ='top'>
								<img src ='prestage_users.php?period=$timewindow'>
							</td>
							      </tr>
							      </table>
						              </div>";
					}
					else if($stat == "Latency"){
						echo "<div>
						      <table>
						      <tr>
						      	<td>
							 <img src ='latencies.php?period=$timewindow'>
							</td>
						      </tr>
						      <tr>
						      	<td>
							 <img src ='miglat.php?period=$timewindow'>
							</td>
						      </tr>
						      </table>
						      </div>
						    ";
					}
					else if($stat == "File"){
						echo "<div>
						      <table>
						      <tr>
						      	<td>
							 <img src ='sizedistr.php?period=$timewindow'>
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
					else if($stat == "Tape"){
						echo "<div>
						      <table>
						      <tr>
						      	<td>
							 <img src ='toptapes.php?period=$timewindow'>
							</td>
							<td>
							 <input type=\"button\" value=\"Show\Hide\" Full Table onClick= \"showdiv('det')\">
							</td>
						      </tr>
						      </table>
						      </div>
						      <div id=\"det\" style=\"display:none\">";
						$period = $timewindow;
						include("toptapesdet.php");
						echo "</div>";    
						echo "<div>
							<table>
						      <tr><td align = 'center'>
							<img src='usertapecontribution.php?period=$timewindow'>
						      </td></tr>
						      </table>
						      </div>
						      <div>
							<table>
						      <tr><td align = 'center'>
							<img src='filesmount.php?period=$timewindow'>
						      </td></tr>
						      </table>
						      </div>";
							
					}
					else if($stat == "GC"){
						echo "<div>
						      <table>
						      <tr>
						      	<td>
							 <img src ='gcfileage.php?period=$timewindow'>
							</td>
						      </tr>
						      <tr>
						      	<td>
							 <img src ='gcsizedistr.php?period=$timewindow'>
							</td>
						      </tr>
						      <tr>
						      	<td>
							 <img src ='gcreq.php?period=$timewindow'>
							</td>
						      </tr>
						      </table>
						      </div>";
					}
					else if($stat == "Migration"){
						echo "<div>
						      <table>
						      <tr>
						      	<td>
							 <img src ='migsperpool.php?period=$timewindow'>
							</td>
						      </tr>
						      <tr>
						      	<td>
							 <img src ='migtimeseries.php?period=$timewindow'>
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
							 <img src ='pie_graph.php?period=$timewindow'>
							</td>
							<td>
							 <img src ='timeseries.php?period=$timewindow'>
							</td>
						      </tr>
						      </table>
						      </div>
						      <div>
						      <table>
						      	<tr>
							<td valign ='top'>
								<img src ='requestsperpool.php?period=$timewindow'>
							</td></tr>
							<tr><td valign ='top'>
							<b> Pool Transactions (Files Copied) </b>";
							$period=$timewindow; include("pool_transaction.php");
							echo "</td>
							      </tr>
							      </table>
						              </div>";
					}
						
				?>
		</tr></table>
</html>
