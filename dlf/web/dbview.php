<?php

/******************************************************************************************************
 *                                                                                                    *
 * dbview.php                                                                                         *
 * Copyright (C) 2005 CERN IT/FIO (castor-dev@cern.ch)                                                *
 *                                                                                                    *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU *
 * General Public License as published by the Free Software Foundation; either version 2 of the       *
 * License, or (at your option) any later version.                                                    *
 *                                                                                                    *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without  *
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU     *
 * General Public License for more details.                                                           *
 *                                                                                                    *
 * You should have received a copy of the GNU General Public License along with this program; if not, *
 * write to the Free Software Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307,  *
 * USA.                                                                                               *
 *                                                                                                    *
 ******************************************************************************************************/

/**
 * $Id: dbview.php,v 1.1 2006/09/06 12:53:43 waldron Exp $
 */

require("utils.php");
include("config.php");
include("db/".strtolower($db_instances[$_GET['instance']]['type']).".php");

$gen_start   = getmicrotime();
$query_count = 0;
$dbh         = db_connect($_GET['instance'], 1, 1);

/* parameters */
$entry = $_GET['entry'];
$param = $_GET['param'];

/* functions */
function navigation_form() {

	include("config.php");

	echo "<form action=\"dbview.php\" method=\"get\" name=\"table\" id=\"table\">Table:Field&nbsp;";
		echo "<input name=\"instance\" type=\"hidden\" value=\"".$_GET['instance']."\"/>";
		echo "<input name=\"link\" type=\"hidden\" value=\"yes\"/>";
		echo "<select name=\"entry\">";
			sort(array_keys($stager_sql_tables));
			foreach (array_keys($stager_sql_tables) as $name) {
				if ($_GET['entry'] == $name) {
					echo "<option value=\"".$name."\" selected=\"selected\"";
				} else {
					echo "<option value=\"".$name."\"";
				}
				echo "<option value=\"".$name."\"";
				echo ">".$name.":".$stager_sql_tables[$name]['lookup']."</option>";
			}					
		echo "</select>&nbsp;&nbsp;";
		echo "<input name=\"param\" type=\"text\" value=\"".$_GET['param']."\" size=\"10\" maxlength=\"10\" />&nbsp;&nbsp;";
		echo "<input type=\"submit\" name=\"Submit\" value=\"Refresh\" class=\"button\"/>&nbsp;";
		echo "<input type=\"button\" value=\"Back\" onclick=\"history.back()\" class=\"button\">";
	echo "</form>";	
}

?>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
	<meta name="author" content="Dennis Waldron" />
	<meta name="description" content="Distributed Logging Facility" />
	<meta http-equiv="Content-Type" content="text/html; charset=us-ascii" />
	<meta http-equiv="Pragma" content="no-cache" />
	<meta http-equiv="Cache-Control" content="no-cache" />
	<meta http-equiv="Default-Style" content="compact" />
	<meta http-equiv="Content-Style-Type" content="text/css" />
	<title>Distributed Logging Facility - Database View</title>
	<link href="site.css" rel="stylesheet" type="text/css" />
</head>
<body>
	<table class="workspace" cellspacing="0" cellpadding="0">
	
	<!-- header -->
  	<tr class="header">
		<td colspan="3">CASTOR Distributed Logging Facility - Database View</td>
  	</tr>
  	<tr>
		<td colspan="3"><hr size="1"/></td>
  	</tr>
		
	<!-- content -->
  	<tr class="content">
    	<td colspan="3" valign="top">
			<table width="100%" border="0">
				<tr>
					<?php	
					if (!$entry || !$param || !$_GET['instance']) {
						echo "<td><strong>Invalid input parameters specified</strong></td>";
					} else if (!$stager_sql_tables[$entry]) {
						echo "<td><strong>No stager database defined for instance '".$_GET['instance']."'</strong></td>";
					} else {

						/* tabular data */
						echo "<td width=\"80%\" valign=\"top\">";

						/* navigation */
						navigation_form();
											
						/* execute query */
						$query_count++;
						$query_lookup = $stager_sql_tables[$entry]['lookup'];
						
						/* adjust the query to use a linklookup if needed */
						if (($_GET['link'] == "yes") && ($stager_sql_tables[$entry]['linklookup'])) {
							$query_lookup = $stager_sql_tables[$entry]['linklookup'];
						} 
	
						$results = db_query("SELECT * FROM ".$stager_sql_tables[$entry]['table']." WHERE $query_lookup = '$param'", $dbh);
						if (!$results) {
					
						}
	
						/* start table information */
						while ($row = db_fetch_row($results)) {
							echo "<table width=\"50%\" border=\"1\" cellspacing=\"3\" cellpadding=\"3\">";
							echo "<tr class=\"banner\"><td colspan=\"2\">$entry</td></tr>";
						
							foreach ($stager_sql_tables[$entry]['fields'] as $config) {
						
								$fields = explode(":", $config);
						
								/* parameters */
								$name   = $fields[0];
								$value  = db_result($results, strtoupper($name));
												
								echo "<tr>";
								echo "<td class=\"header\" width=\"110\">$name</td>";
							
								/* process options */
								array_flip($fields);
								array_shift($fields);
			
								if (in_array("link", $fields)) {
									$value = "";
		
									/* create links */
									foreach ($stager_sql_tables[$entry]['links'][$name] as $link) {
										$link_info = explode(":", $link);
										$value .= "<a href=\"dbview.php?instance=".$_GET['instance']."&entry=$link_info[0]&param=$param&link=$link_info[1]\">$link_info[0]</a><br />";	
									}	
	
									/* keep xhtml happy */
									$value = str_replace("param=$param", "param=".db_result($results, strtoupper($name)), $value);			
									$value = str_replace("&", "&amp;", $value);	
									
								} else if (in_array("size", $fields)) {
								
									/* convert the size in bytes to human readable size 
									 *   - http://ch2.php.net/manual/en/function.filesize.php
									 */
									$i = 0;
									$iec = array("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB");
									while (($value / 1024) > 1) {
										$value = $value / 1024;
										$i++;
									}
									$value = substr($value, 0, strpos($value, '.') + 4)."<b> ".$iec[$i]."</b>";
								
								} else if (in_array("time", $fields)) {
									$value = date("d-m-Y G:i:s", $value);						
								} else if (in_array("status", $fields)) {
									$value = strtoupper($stager_sql_tables[$entry]['status'][$value]);
								} 
								
								/* expand fields */
								else if (in_array("expand", $fields)) {
								
									$sub_value = "<b>[".$name."] (</b>";
									$i = 0;
									foreach ($stager_sql_tables[$entry]['expands'][$name] as $expand) {
							
										/* parameters */			
										$sub_fields = explode(":", $expand);
										$sub_name   = $sub_fields[0];
																			
										/* execute query */
										$query_count++;
										$sub_results = db_query("SELECT * FROM ".$stager_sql_tables[$name]['table']." WHERE ".$stager_sql_tables[$name]['lookup']. " = '$value'", $dbh);
										if (!$sub_results) {
						
										}
										$row = db_fetch_row($sub_results);
										
										/* seperate values */
										if (!$i) {
											$sub_value .= "<b>$sub_name</b> = ";
										} else {
											$sub_value .= " - <b>$sub_name</b> = ";
										}
										
										/* process options */
										array_flip($sub_fields);
										array_shift($sub_fields);
										
										if (in_array("status", $sub_fields)) {
											$sub_value .= strtoupper($stager_sql_tables[$name]['status'][db_result($sub_results, strtoupper($sub_name))]);
										} else {	
											$sub_value .= db_result($sub_results, strtoupper($sub_name));									
										}
										$i++;
									}
									$sub_value .= ")";
									
									$value = $sub_value;							
								}
								
								if ($value != "") {
									echo "<td>$value</td>";
								} else {
									echo "<td>&nbsp;</td>";
								}
								echo "</tr>";
							}				
						
							/* end table information */
							echo "</table>";
							echo "<br>";
						}
							
						/* navigation */
						navigation_form();
						
						echo "</td>";
					}
					?>
				</tr>
			</table>
		</td>
  	</tr>	
	
	<!-- connectivity information -->
	<tr>
		<td colspan="3">Connected to:
			<?php
			if ($show_db_version) { 
				echo $db_instances[$_GET['instance']]['stagerdb']['username']."@".$db_instances[$_GET['instance']]['stagerdb']['server']." (".$db_instances[$_GET['instance']]['type']." ".db_server_version($dbh).")";
			} else {
				echo $db_instances[$_GET['instance']]['stagerdb']['username']."@".$db_instances[$_GET['instance']]['stagerdb']['server']." (".$db_instances[$_GET['instance']]['type'].")";
			}
			?>	
		</td>
	</tr>
	
	<!-- footer -->
  	<tr>
		<td colspan="3"><hr size="1"/></td>
	</tr>
  	<tr class="footer">
    	<td width="33%" align="left"  >DLF interface version: <?php echo $version ?></td>
    	<td width="33%" align="center"><?php printf("Page generated in %.3f seconds with %d sql queries.", getmicrotime() - $gen_start, $query_count); ?></td>
    	<td width="33%" align="right" >
			<a href="http://validator.w3.org/check?uri=referer"><img src="images/xhtml.png" alt="Valid XHTML 1.0 Strict" /></a>&nbsp;
			<a href="http://jigsaw.w3.org/css-validator/check/referer"><img src="images/css.png" alt="Valid CSS 2.0" /></a>&nbsp;
			<a href="http://www.php.net/"><img src="images/php.png" alt="Powered by PHP" /></a>		
		</td>
  	</tr>
	</table>
</body>
</html>
