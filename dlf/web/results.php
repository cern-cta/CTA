<?php

/******************************************************************************************************
 *                                                                                                    *
 * results.php                                                                                        *
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

require("utils.php");
include("config.php");
include("db/".strtolower($db_instances[$_GET['instance']]['type']).".php");

$gen_start  = getmicrotime();
$query_count = 0;

$dbh = db_connect($_GET['instance'], 1, 0);

?>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">

<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
<head>
	<meta name="author" content="Dennis Waldron" />
	<meta name="description" content="Distributed Logging Facility" />
	<meta http-equiv="Content-Type" content="text/html; charset=us-ascii" />
	<meta http-equiv="Pragma" content="no-cache" />
	<meta http-equiv="Cache-Control" content="no-cache" />
	<meta http-equiv="Default-Style" content="compact" />
	<meta http-equiv="Content-Style-Type" content="text/css" />
	<link rel="stylesheet" type="text/css" title="compact" href="site.css" />
	<title><?php printf("%s - ", strtoupper($_GET['instance'])); ?>Distributed Logging Facility - Results</title>

	<?php

	/* form adjustments 
	 *   - needed for form navigation and message text selection
	 */
	if ($_GET['msgtext'] == -1) { 
		$_GET['msgtext'] = "All"; 
	}
	if ($_GET['nav'] == "Previous") { 
		$_GET['page']--; 
	} else if ($_GET['nav'] == "Next") {
		$_GET['page']++;
	}

	/* flag columns required for display */
	foreach (array_keys($dlf_sql_columns) as $name) {
		if (($_GET['columns'] == "default") || ($_GET[$name] == "on")) {
			$dlf_sql_columns[$name]['display']  = 1;
			$dlf_sql_columns[$name]['required'] = 1;
		}
	}

	/* the use may have specified a where constraint yet decided not to display the column
	 * the constraint refers too. Here we flag that a join on a particular field/column
	 * is necessary to satisfy the constraint
	 */
	foreach (array_keys($dlf_sql_conditions) as $condition) {

		/* ignore arrays with/or values set to "All" */
		if ((is_array($_GET[$condition])) && ($_GET[$condition][0] == "All")) {
			continue;
		}
		if ((trim($_GET[$condition]) != "") && ($_GET[$condition] != "All")) {
			$dlf_sql_conditions[$condition]['defined'] = 1;
		}
	}

	/* the follow fields are mandatory */
	$query_fields = "t1.id t1.timestamp t1.timeusec t1.severity t1.facility t1.hostid t1.nshostid";

	/* append option fields */
	foreach (array_keys($dlf_sql_columns) as $name) {
		if ($dlf_sql_columns[$name]['display']) {
			$query_fields .= " ".$dlf_sql_columns[$name]['field'];
		}
	}
	$query_fields = str_replace(" ", ",", trim($query_fields));

	/* determine period of lookup */
	if (DB_LAYER == "mysql") {
		if ($_GET['drilltime']) {
			$period = " ((t1.timestamp >= DATE_SUB(STR_TO_DATE('".$_GET['drilltime']."', '%Y-%m-%d %H:%i:%s'), interval ".$db_drilldown_time." hour)) AND
						(t1.timestamp < DATE_ADD(STR_TO_DATE('".$_GET['drilltime']."', '%Y-%m-%d %H:%i:%s'), interval ".$db_drilldown_time." hour)))";
		} else if ($_GET['last'] != 0) {
			$period .= " t1.timestamp > DATE_SUB(NOW(), interval ".$_GET['last']." minute)";
		} else if ($_GET['mode'] != "drilldown") {
			$period .= " ((t1.timestamp >= STR_TO_DATE('".$_GET['from']." ".$_GET['fromtime'].":00', '%d/%m/%Y %H:%i:%s')) AND
						(t1.timestamp <  STR_TO_DATE('". $_GET['to']. " " .$_GET['totime'].":00', '%d/%m/%Y %H:%i:%s')))";
		}  	
	} else {
		if ($_GET['drilltime']) {
			$period .= " ((t1.timestamp >= (TO_DATE('".$_GET['drilltime']."', 'DD/MM/YYYY HH24:MI:SS') - ".$db_drilldown_time."/24)) AND
						(t1.timestamp <  (TO_DATE('".$_GET['drilltime']."', 'DD/MM/YYYY HH24:MI:SS') + ".$db_drilldown_time."/24)))"; 
		} else if ($_GET['last'] != 0) {
			$period .= " t1.timestamp > (SYSDATE - ".$_GET['last']."/1440)";
		} else if ($_GET['mode'] != "drilldown") {
			$period .= " ((t1.timestamp >= TO_DATE('".$_GET['from'] ." " .$_GET['fromtime'].":00', 'DD/MM/YYYY HH24:MI:SS')) AND
						(t1.timestamp <  TO_DATE('".$_GET['to']." " .$_GET['totime'].":00', 'DD/MM/YYYY HH24:MI:SS')))";  
		}   	
	}

	/* generate the join criteria needed to satisfy the field requirements */
	foreach (array_keys($dlf_sql_columns) as $name) {
		if ($dlf_sql_columns[$name]['required']) {
			$query_joins .= " ".str_replace("timestamp_predicate", str_replace("t1.", substr($dlf_sql_columns[$name]['field'],0,3), $period), $dlf_sql_columns[$name]['join']);
		}
	}

	/* generate the where portion of the query
	 *   - note: it just so happens that all mandatory fields come from the primary dlf table
	 *		   and do not require any joins to resolve data
	 */
	foreach (array_keys($dlf_sql_conditions) as $column) {
		if ($dlf_sql_conditions[$column]['defined'] != 1) {
			continue;
		}

		/* first condition */
		$query_where .= $query_conditions ? " AND" : " WHERE";
		$query_conditions++;

		/* an array requires the use of 'OR' */
		if (is_array($_GET[$column])) {
			for ($i = 0, $query_where .= " ("; $i < count($_GET[$column]); $i++) {
				$query_where .= "(".trim($dlf_sql_conditions[$column]['field'])." = '".trim($_GET[$column][$i])."')";
				if (($i + 1) != count($_GET[$column])) {
					$query_where .= " OR";
				}
			}
			$query_where .= ")";
		}

		/* wildcards allowed in this field ? */
		else if (($dlf_sql_conditions[$column]['wildcard']) && (strpos(trim($_GET[$column]), "%"))) {
			$query_where .= " ".$dlf_sql_conditions[$column]['field']." LIKE '".trim($_GET[$column])."' ";
		} else {
			$query_where .= " ".$dlf_sql_conditions[$column]['field']." = '".trim($_GET[$column])."' ";
		}
	}

	/*
	 * parameters are special as they require sql statements across two tables
	 */
	if ((trim($_GET['paramvalue']) || trim($_GET['paramname']))) {
		$query_joins .= " LEFT JOIN dlf_str_param_values t9 on (t1.id = t9.id) LEFT JOIN dlf_num_param_values t10 on (t1.id = t10.id)";
	
		/* loop over tables */
		foreach (explode(" ", "paramname paramvalue") as $name) {
			if (!trim($_GET[$name])) {
				continue;
			}
			$field = ($name == "paramname") ? "name" : "value";
	
			/* first condition ? */
			$query_where .= $query_conditions ? " AND" : " WHERE";
			$query_conditions++;

			/* wildcards allowed in this field ? */
			if (strpos(trim($_GET[$name]), "%")) {
				$query_where .= " ((t9.".$field." LIKE '".trim($_GET[$name])."') OR (t10.".$field." LIKE '".trim($_GET[$name])."'))";
			} else {
				$query_where .= " ((t9.".$field." = '".trim($_GET[$name])."') OR (t10.".$field." = '".trim($_GET[$name])."'))";
			}
		}
	}

	/* append time period to query */
	$query_period .= $query_conditions ? " AND" : " WHERE";
	$query_period .= $period;
	
	$query_conditions++;
	
	/* finalise the query (limits) */
	if (DB_LAYER == "mysql") {
		$query_fields   = "SELECT ".$query_fields;
		$query_finalise = " ORDER BY t1.timestamp DESC, t1.timeusec DESC LIMIT ".(($_GET['page'] - 1) * $_GET['limit']).", ".$_GET['limit'];
	} else {

		/* oracle doesn't have the concept of the 'limit' keyword as a result we have to use a
		 * nested query and place restrictions on the rownum
		 */
		$query_fields   = "SELECT * FROM (SELECT p.*, ROWNUM RNUM FROM (SELECT ".$query_fields;
		$query_finalise = " ORDER BY t1.timestamp DESC, t1.timeusec DESC) p ) WHERE RNUM > ".($_GET['page'] - 1) * $_GET['limit']." AND RNUM <= ".($_GET['page'] * $_GET['limit']);
	}

	/* trim the sql variables */
	$query_fields   = trim($query_fields);
	$query_joins	= trim($query_joins);
	$query_where	= trim($query_where);
	$query_period   = trim($query_period);
	$query_finalise = trim($query_finalise);
  
	/* debugging (html comments) */
	echo "<!-- Query 1: $query_fields FROM dlf_messages t1 $query_joins $query_where $query_period $query_finalise -->\n";

	/* execute query */
	$query_count++;
	$results = db_query($query_fields." FROM dlf_messages t1 ".$query_joins." ".$query_where." ".$query_period." ".$query_finalise, $dbh);
	if (!$results) {
		exit;
	}

	/* loop over results */
	while ($row = db_fetch_row($results)) {
	
		/* mandatory fields */
		$id = $row[0];
		$data[$id]['Timestamp']	   = $row[1].".".$row[2];
		$data[$id]['m_severity']   = $row[3];
		$data[$id]['m_facility']   = $row[4];
		$data[$id]['m_hostname']   = $row[5];
		$data[$id]['m_nshostname'] = $row[6];

		$i = 7;
		foreach (array_keys($dlf_sql_columns) as $name) {
			if (!$dlf_sql_columns[$name]['display']) {
				continue;
			}
			$data[$id][$dlf_sql_columns[$name]['name']] = $row[$i];
			$i++;
		}
	}

	/* count the total number of possible results in the resultset */
	$query_count++;
	$results = db_query("SELECT count(*) FROM dlf_messages t1 ".$query_joins." ".$query_where." ".$query_period, $dbh);
	if (!$results) {
		exit;
	}
	$row = db_fetch_row($results);
	$query_total = $row[0];

	/* fetch the parameters associated with the messages in the resultset */
	if ((($_GET['col_params']) || ($_GET['columns'] == "default")) && (count($data))) {

		/* construct a list of message id's */
		for ($i = 0, $query_list = "(", $keys = array_keys($data); $i < count($data); $i++) {
			$query_list .= "'".$keys[$i]."'";
			if (($i + 1) != count($data)) {
				$query_list .= ",";
			}
		}
		$query_list .= ")";
	
		/* loop over the tables containing parameter data */
		foreach (explode(" ", "dlf_str_param_values dlf_num_param_values") as $table) {
		
			/* execute query */
			$query_count++;
			$results = db_query("SELECT id, name, value FROM ".$table." WHERE id IN ".$query_list." AND ".str_replace("t1.", "", $period), $dbh);
			if (!$results) {
				exit;
			}

			/* append results to multi-dimensional array */
			for ($i = count($data[$row[0]]['Parameters']); $row = db_fetch_row($results); $i++) {
				if ($prev_row != $row[0]) {
					$i = count($data[$row[0]]['Parameters']);
				}
				$prev_row = $row[0];

				if (strpos($row[2], " ")) {
					$data[$row[0]]['Parameters'][$i] = $row[1]."=\"".$row[2]."\"";
				} else {
					$data[$row[0]]['Parameters'][$i] = $row[1]."=".$row[2];
				}
			}
		}
	}

	?>
</head>

<body>
	<table class="workspace" cellspacing="0" cellpadding="0">
	
	<!-- header -->
  	<tr class="header">
    	<td colspan="2">CASTOR Distributed Logging Facility</td>
		<td align="right"><?php printf("%s", strtoupper($_GET['instance'])); ?></td>
  	</tr>
  	<tr>
    	<td colspan="3"><hr size="1"/></td>
  	</tr>
	
	<!-- content -->
  	<tr class="content">
    	<td colspan="3" valign="top">
	
			<!-- results table -->
			<table class="noborder" cellspacing="0" cellpadding="0" width="100%">
			
				<!-- summary header -->
				<tr>
					<td align="left">
					<?php

					/* calculate message snapshot numbers */
					if ($_GET['page'] == 1) {
						$start_no = !count($data) ? 0 : 1;
					} else {
						$start_no = (($_GET['page'] - 1) * $_GET['limit']) + 1;
					}
				
					$start_no = $start_no > $query_total ? $query_total : $start_no;
	  				$end_no   = ($start_no == 1 ? 1 : $start_no) + ($_GET['limit'] - 1);
					$end_no   = $end_no > $query_total ? $query_total : $end_no;
				
					echo "Total number of messages found: <strong>".$query_total."</strong> ";
					if ($_GET['drilltime']) {
						$buf = explode(".", $_GET['drilltime']);
						echo "from <strong>".$buf[0]." +/-".$db_drilldown_time."</strong> hours";
					} else if ($_GET['last']) {
						$date_to   = explode(" ", date("d/m/Y H:i", time()));
						$date_from = explode(" ", date("d/m/Y H:i", time() - ($_GET['last'] * 60)));

						$_GET['from']	  = $date_from[0];
						$_GET['fromtime'] = $date_from[1];
						$_GET['to']	  = $date_to[0];
						$_GET['totime']   = $date_to[1];
		
						echo "between <strong>".$_GET['from']." ".$_GET['fromtime']."</strong> ";
						echo "and <strong>".$_GET['to']." ".$_GET['totime']."</strong> - (".$_GET['last']." minutes)";
					} else {
						echo "between <strong>".$_GET['from']." ".$_GET['fromtime']."</strong> ";
						echo "and <strong>".$_GET['to']." ".$_GET['totime']."</strong>";
					}
					echo "<br/>";
					echo "Displaying messages <strong> $start_no </strong> through <strong> $end_no </strong><br/>&nbsp;";

					?>
					</td>

					<!-- navigation -->
					<td align="right">
					<?php

					echo "<form id=\"navigation1\" name=\"navigation\" method=\"get\" action=\"results.php\">";
				
					/* duplicate original form values */
					foreach (array_keys($HTTP_GET_VARS) as $name) {
						if ($name == "Submit") {
							continue;
						}
						if (is_array($_GET[$name])) {
							for ($i = 0; $i < count($_GET[$name]); $i++) {
								echo "<input type=\"hidden\" name=\"".$name."[]\" value=\"".$_GET[$name][$i]."\"/>";
							}
						} else {
							echo "<input type=\"hidden\" name=\"".$name."\" value=\"".$_GET[$name]."\"/>";
						}
					}
					echo "<input type=\"submit\" name=\"nav\" value=\"Query\" class=\"button\" onclick=\"this.form.action='query.php';\"/>";

					if ($_GET['page'] > 1) {
						echo "&nbsp; &nbsp; &nbsp;<input type=\"submit\" name=\"nav\" value=\"Previous\" class=\"button\"/>";
					}
					if ($query_total > ($_GET['limit'] * $_GET['page'])) {
						echo "&nbsp; &nbsp;<input type=\"submit\" name=\"nav\" value=\"Next\" class=\"button\"/>";
					} else {
						echo "&nbsp;";
					}

					echo "</form>";

					?>
					</td>
				</tr>
				
				<!-- results -->
				<tr>
					<td colspan="2" height="300" valign="<?php if (!count($data)) { echo "middle"; } else { echo "top"; } ?>">
					
					<?php
					
					if (!count($data)) {
						echo "<strong>No results found!</strong>";
					} else {
		
						echo "<table border=\"1\" cellspacing=\"3\" cellpadding=\"3\" width=\"100%\">";	
						
						/* table header */
						echo "<tr>";
						
						/* generate table header
			 			*   - display the mandatory 'timestamp; field first followed by the user define columns
			 			*/
						echo "<td class=\"banner\">Timestamp</td>";

						foreach(array_keys($dlf_sql_columns) as $name) {
							if (($dlf_sql_columns[$name]['name']) && 
						 	   ($dlf_sql_columns[$name]['display'])) {
								echo "<td class=\"banner\">".$dlf_sql_columns[$name]['name']."</td>";
							}
						}
			
						if (($_GET['col_params'] == on) || ($_GET['columns'] == "default")) {
							echo "<td class=\"banner\">Parameters</td>";
						}
		 
						/* generate the common request url used when drilling down through the data */
						$get_url  = "columns=".$_GET['columns']."&amp;limit=".$_GET['limit'];
						$get_url .= "&amp;instance=".$_GET['instance']."&amp;page=1";

						foreach (array_keys($dlf_sql_columns) as $name) {
							$get_url .= "&amp;".$name."=".$_GET[$name];
						}
			
						if ($_GET['col_params']) {
							$get_url .= "&amp;col_params=".$_GET['col_params'];
						}
						
						echo "</tr>";
						
						/* table data */
						for ($i = 0, $keys = array_keys($data); $i < count($keys); $i++) {
							echo "<tr>";
	
							/* mandatory fields */
							echo "<td class=\"timestamp\">".$data[$keys[$i]]['Timestamp']."</td>";
			
							/* user defined fields */
							foreach (array_keys($dlf_sql_columns) as $name) {
								if ((!$dlf_sql_columns[$name]['name']) || 
					 		 	  (!$dlf_sql_columns[$name]['display'])) {
									continue;
								}
	
								$value = $data[$keys[$i]][$dlf_sql_columns[$name]['name']];
								$class = str_replace("col_", "", $name);
			
								/* apply correct css class for severity field */
								if ($dlf_sql_columns[$name]['name'] == "Severity") {
									echo "<td class=\"sev_".strtolower($value)."\">".$value."</td>";  
								}

								/* if no value is available we still pad the table cell otherwise the table
			 					 * borders are not rendered correctly
			 					 */
								else if (!$value) {
									echo "<td class=\"".$class."\">N/A</td>";
								}
				
								/* hyperlink required for drill down */
								else if ($dlf_sql_columns[$name]['href'] != "") {

									/* adjustments required to handle ns file id */
									if (($dlf_sql_columns[$name]['name'] == "NS File ID") && ($use_database_view)) {
										echo "<td class=\"".$class."\">";
										echo "<a href=\"dbview.php?instance=".$_GET['instance']."&amp;entry=castorfile&amp;param=".urlencode($value)."\" target=\"blank\">".$value."</a>";
										echo "</td>";
										continue;
									} 
			
									$drill = explode(".", $data[$keys[$i]]['Timestamp']);
									$drill = "drilltime=".urlencode($drill[0]);
								
									/* add an invisible word-break to request and sub request ids */
									if (($dlf_sql_columns[$name]['name'] == "Request ID") ||
										($dlf_sql_columns[$name]['name'] == "Sub Request ID")) {
											$value = str_replace("-", "-<wbr />", $value);
									}

									echo "<td class=\"".$class."\">";
									if ($value != "N/A") {
										if ($data[$keys[$i]]["m_".$dlf_sql_columns[$name]['href']]) {
											echo "<a href=\"".$PHP_SELF."?".$drill."&amp;".$get_url."&amp;".$dlf_sql_columns[$name]['href']."=".$data[$keys[$i]]["m_".$dlf_sql_columns[$name]['href']]."\">".$value."</a>";
										} else {
											echo "<a href=\"".$PHP_SELF."?".$drill."&amp;".$get_url."&amp;".$dlf_sql_columns[$name]['href']."=".urlencode(str_replace("-<wbr />","-",$value))."\">".$value."</a>";
										}	
									} else {
										echo $value;
									}				
									echo "</td>";
								} 

								/* normal data */
								else {
									echo "<td class=\"".$class."\">".$value."</td>";
								}
							}	

							/* parameters */
							if (($_GET['col_params'] == "on") || ($_GET['columns'] == "default")) {
								echo "<td class=\"parameters\">";
								if (count($data[$keys[$i]]['Parameters'])) {
									for ($j = 0; $j < count($data[$keys[$i]]['Parameters']); $j++) {
									
										/* deal with long lines */
										$value = $data[$keys[$i]]['Parameters'][$j];
										if (strlen($value) > 150) {
											$value = str_replace("/", "/<wbr />", $value);
											$value = "<br/>".str_replace(".", ".<wbr />", $value)."<br/>";
										}				
									
										echo $value."<br/>";
									}
								} else {
									echo "&nbsp;";
								}
								echo "</td>";
							}
							echo "</tr>";
						}
						echo "</table>";
					}
						
					
					echo "<br />";
					
					?>
					
					</td>		
				</tr>
				
				<!-- navigation -->
				<?php 
		
				if (count($data)) {
					echo "<tr>";
					echo "<td align=\"right\" valign=\"bottom\" colspan=\"2\">";
					echo "<form id=\"navigation2\" name=\"navigation\" method=\"get\" action=\"results.php\">";
				
					/* duplicate original form values */
					foreach (array_keys($HTTP_GET_VARS) as $name) {
						if ($name == "Submit") {
							continue;
						}
						if (is_array($_GET[$name])) {
							for ($i = 0; $i < count($_GET[$name]); $i++) {
								echo "<input type=\"hidden\" name=\"".$name."[]\" value=\"".$_GET[$name][$i]."\"/>";
							}
						} else {
							echo "<input type=\"hidden\" name=\"".$name."\" value=\"".$_GET[$name]."\"/>";
						}
					}

					if ($_GET['page'] > 1) {
						echo "<input type=\"submit\" name=\"nav\" value=\"Previous\" class=\"button\"/>&nbsp;&nbsp;";
					}
					if ($query_total > ($_GET['limit'] * $_GET['page'])) {
						echo "<input type=\"submit\" name=\"nav\" value=\"Next\" class=\"button\"/>";
					} else {
						echo "&nbsp;";
					}

					echo "</form>";
		 			echo "</td>";
					echo "</tr>";
				}

				?>
			</table>
		</td>
	</tr>

	<!-- connectivity information -->
	<tr>
		<td colspan="2">Connected to:
			<?php
			if ($show_db_version) { 
				echo $db_instances[$_GET['instance']]['username']."@".$db_instances[$_GET['instance']]['server']." (".$db_instances[$_GET['instance']]['type']." ".db_server_version($dbh).")";
			} else {
				echo $db_instances[$_GET['instance']]['username']."@".$db_instances[$_GET['instance']]['server']." (".$db_instances[$_GET['instance']]['type'].")";
			}
			?>	
		</td>
		<td align="right">
			<?php
			if ($show_partition_count) {
				echo db_partition_count($dbh);
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
