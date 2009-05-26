<html>
	<head>
		<style type='text/css'>
		#main{
			width: 100%;
			heght:	100%;
		}
		#selection{
			width:1200px;
			height: 500px;
			background-image: url("images/backdrop.png");
			background-position: center;
			background-repeat: no-repeat;
		}
		</style>
		<LINK rel="stylesheet" type="text/css" title="compact" href="lib/hmenu.css">
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
		<table id='main'>
		<tr>
			<td align ='left' style='background-color: #B0B0B0;font: bold 20px/30px arial, helvetica, sans-serif'>
				<i>CASTOR Monitoring Web Page</i>
			</td>
		</tr>
		<tr>
			<td><div id='menu'><ul>
				<li><h2>External Links</h2>
				<ul>
					<li><a href="http://c2adm/" target="_blank">CASTOR Dashboard</a></li>
					<li><a href="http://castortapelog/tapelog-web/" target="_blank">Tape Statistics</a></li>
					<li><a href="http://sls.cern.ch/sls/service.php?id=CASTOR" target="_blank">Service Level Status Overview</a></li>
					<li><a href="http://lemonweb.cern.ch/lemon-web/info.php?entity=castor2&type=host&cluster=1" target="_blank">Lemon Monitoring</a></li>
					</ul>
				</li>
			</ul></div></td>
		</tr>
		<tr>
			<td><hr size='1'/></td>
		</tr>
		<tr id='selection'><td align='center'>
		<form action='dashboard/dashboard.php' method='post'>
		<table border=1 rules=none frame=box style='border:solid'>
			<th colspan='3' style='background-color: #B0B0B0'> Select Instance </th>
			<tbody>
				<tr>
					<td>
						Use DataBase:
					</td>
					<td>
					<?php 
					include("../../conf/castor-mon-web/user.php");
					 	if(empty($db_instances)) {
							echo "<b>No Database Available</b>";
						}
						else {
							echo '<select name="service">';
							$selected = 0;
							$keys = array_keys($db_instances);
							for ($i = 0;$i < sizeof($keys);$i++) {
								$uname = $db_instances[$keys[$i]]['username'];
								$serv = $db_instances[$keys[$i]]['serv'];
								if ($i == 0) {
									echo "<option value='$keys[$i]' selected='selected'>$uname@$serv</option>"; 
								}
								else
									echo "<option value='$keys[$i]'>$uname@$serv</option>"; 
							}
							echo '</select>';
							
						}
					?>
					</td>
					<td>
						<input type="submit" name = "submit" value = "Connect">
					</td>
				</tr>
			</tbody>
		</table>
		</td></tr>
		<tr>
			<td><hr size='1'/></td>
		</tr>
		</table>
	</body>
</html>
