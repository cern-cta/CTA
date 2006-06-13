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

$page_start = getmicrotime();
$querycount = 0;

$dbh = db_connect($_GET['instance'], 1);

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

	<title>Distributed Logging Facility - Results</title>

	<?php

	/* form adjustments 
	 *   - needed for form navigation and message text selection
	 */
	if ($_GET['msgtext'] == -1) { 
		$_GET['msgtext'] = "All"; 
	}
	if ($_GET['nav'] == "Prev") { 
		$_GET['page']--; 
	} else if ($_GET['nav'] == "Next") {
		$_GET['page']++;
	}

	/* flag columns required for display */
	foreach (array_keys($db_sql_columns) as $name) {
		if (($_GET['columns'] == "default") || ($_GET[$name] == "on")) {
			$db_sql_columns[$name]['display']  = 1;
			$db_sql_columns[$name]['required'] = 1;
		}
	}

	/* the use may have specified a where constraint yet decided not to display the column
	 * the constraint refers too. Here we flag that a join on a particular field/column
	 * is necessary to satisfy the constraint
	 */
	foreach (array_keys($db_sql_conditions) as $condition) {

		/* ignore arrays with/or values set to "All" */
		if ((is_array($_GET[$condition])) && ($_GET[$condition][0] == "All")) {
			continue;
		}
		if (($_GET[$condition] != "") && ($_GET[$condition] != "All")) {
			$db_sql_conditions[$condition]['defined'] = 1;
		}
	}

	/* the follow fields are mandatory */
	$query_fields = "t1.id t1.timestamp t1.timeusec t1.severity t1.facility t1.hostid t1.nshostid";

	/* append option fields */
	foreach (array_keys($db_sql_columns) as $name) {
		if ($db_sql_columns[$name]['display']) {
			$query_fields .= " ".$db_sql_columns[$name]['field'];
		}
	}
	$query_fields = str_replace(" ", ",", trim($query_fields));

	/* generate the join criteria needed to satisfy the field requirements */
	foreach (array_keys($db_sql_columns) as $name) {
		if ($db_sql_columns[$name]['required']) {
			$query_joins .= " ".$db_sql_columns[$name]['join'];
		}
	}

	/* generate the where portion of the query
	 *   - note: it just so happens that all mandatory fields come from the primary dlf table
	 *		   and do not require any joins to resolve data
	 */
	foreach (array_keys($db_sql_conditions) as $column) {
		if ($db_sql_conditions[$column]['defined'] != 1) {
			continue;
		}

		/* first condition */
		$query_where .= $query_conditions ? " AND" : " WHERE";
		$query_conditions++;

		/* an array requires the use of 'OR' */
		if (is_array($_GET[$column])) {
			for ($i = 0, $query_where .= " ("; $i < count($_GET[$column]); $i++) {
				$query_where .= "(".trim($db_sql_conditions[$column]['field'])." = '".$_GET[$column][$i]."')";
				if (($i + 1) != count($_GET[$column])) {
					$query_where .= " OR";
				}
			}
			$query_where .= ")";
		}

		/* wildcards allowed in this field ? */
		else if (($db_sql_conditions[$column]['wildcard']) && (strpos($_GET[$column], "%"))) {
			$query_where .= " ".$db_sql_conditions[$column]['field']." LIKE '".$_GET[$column]."' ";
		} else {
			$query_where .= " ".$db_sql_conditions[$column]['field']." = '".$_GET[$column]."' ";
		}
	}

	/*
	 * parameters are special as they require sql statements across two tables
	 */
	if (($_GET['paramvalue'] || $_GET['paramname'])) {
		$query_joins .= " LEFT JOIN dlf_str_param_values t9 on (t1.id = t9.id) LEFT JOIN dlf_num_param_values t10 on (t1.id = t10.id)";
	
		/* loop over tables */
		foreach (explode(" ", "paramname paramvalue") as $name) {
			if (!$_GET[$name]) {
				continue;
			}
			$field = ($name == "paramname") ? "name" : "value";
	
			/* first condition ? */
			$query_where .= $query_conditions ? " AND" : " WHERE";
			$query_conditions++;

			/* wildcards allowed in this field ? */
			if (strpos($_GET[$name], "%")) {
				$query_where .= " ((t9.".$field." LIKE '".$_GET[$name]."') OR (t10.".$field." LIKE '".$_GET[$name]."'))";
			} else {
				$query_where .= " ((t9.".$field." = '".$_GET[$name]."') OR (t10.".$field." = '".$_GET[$name]."'))";
			}
		}
	}

	/* append time period to query */
	$query_period .= $query_conditions ? " AND" : " WHERE";
	$query_conditions++;

	if (DB_LAYER == "mysql") {
		if ($_GET['drilltime']) {
			$query_period .= " ((t1.timestamp >= DATE_SUB(STR_TO_DATE('".$_GET['drilltime']."', '%Y-%m-%d %H:%i:%s'), interval ".$db_drilldown_time." hour)) AND
						(t1.timestamp < DATE_ADD(STR_TO_DATE('".$_GET['drilltime']."', '%Y-%m-%d %H:%i:%s'), interval ".$db_drilldown_time." hour)))";
		} else if ($_GET['last'] != 0) {
			$query_period .= " t1.timestamp > DATE_SUB(NOW(), interval ".$_GET['last']." minute)";
		} else if ($_GET['mode'] != "drilldown") {
			$query_period .= " ((t1.timestamp >= STR_TO_DATE('".$_GET['from']." ".$_GET['fromtime'].":00', '%d/%m/%Y %H:%i:%s')) AND
						(t1.timestamp <  STR_TO_DATE('". $_GET['to']. " " .$_GET['totime'].":00', '%d/%m/%Y %H:%i:%s')))";
		}  	
	} else {
		if ($_GET['drilltime']) {
			$query_period .= " ((t1.timestamp >= (TO_DATE('".$_GET['drilltime']."', 'DD/MM/YYYY HH24:MI:SS') - ".$db_drilldown_time."/24)) AND
						(t1.timestamp <  (TO_DATE('".$_GET['drilltime']."', 'DD/MM/YYYY HH24:MI:SS') + ".$db_drilldown_time."/24)))"; 
		} else if ($_GET['last'] != 0) {
			$query_period .= " t1.timestamp > (SYSDATE - ".$_GET['last']."/1440)";
		} else if ($_GET['mode'] != "drilldown") {
			$query_period .= " ((t1.timestamp >= TO_DATE('".$_GET['from'] ." " .$_GET['fromtime'].":00', 'DD/MM/YYYY HH24:MI:SS')) AND
						(t1.timestamp <  TO_DATE('".$_GET['to']." " .$_GET['totime'].":00', 'DD/MM/YYYY HH24:MI:SS')))";  
		}   	
	}

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
	echo "<!-- Query 1 -->\n";
	echo "<!-- Fields:   ".$query_fields.  " -->\n";
	echo "<!-- joins:    ".$query_joins.   " -->\n";
	echo "<!-- where:    ".$query_where.   " -->\n";
	echo "<!-- period:   ".$query_period.  " -->\n";
	echo "<!-- finalise: ".$query_finalise." -->\n";	
 	
	/* execute query */
	$querycount++;
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
		foreach (array_keys($db_sql_columns) as $name) {
			if (!$db_sql_columns[$name]['display']) {
				continue;
			}
			$data[$id][$db_sql_columns[$name]['name']] = $row[$i];
			$i++;
		}
	}

	/* count the total number of possible results in the resultset */
	$querycount++;
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
			$querycount++;
			$results = db_query("SELECT id, name, value FROM ".$table." WHERE id IN ".$query_list, $dbh);
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

	<!-- header -->
	<table class="noborder" cellspacing="0" cellpadding="0" width="100%">
		<tr class="header">
			<td>&nbsp;CASTOR Distributed Logging Facility</td>
		</tr>
	</table>

	<!-- break 1 -->
	<hr align="left" size="1" />
	
	<!-- main workspace -->
	<table class="<?php if (count($data)) { echo "workspace"; } else { echo "results"; } ?>" cellspacing="0" cellpadding="0" width="100%">
		<tr>
			<td valign="top">

			<!-- data header -->
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
				$date_to   = explode(" ", date("d/m/Y H:i", time() + ($_GET['last'] * 60)));
				$date_from = explode(" ", date("d/m/Y H:i", time()));

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

		  	<!-- table data -->
			<?php 
			
				if (!count($data)) {
					echo "<table cellspacing=\"3\" cellpadding=\"3\" align=\"center\" width=\"100%\">";
				} else {
					echo "<table class=\"backdrop\" border=\"1\" cellspacing=\"3\" cellpadding=\"3\">";	
				}
			
			?>			
				<tr>

				<?php
		
				if (count($data)) {

					/* generate table header
			 		*   - display the mandatory 'timestamp; field first followed by the user define columns
			 		*/
					echo "<td class=\"banner\">Timestamp</td>";

					foreach(array_keys($db_sql_columns) as $name) {
						if (($db_sql_columns[$name]['name']) && 
						    ($db_sql_columns[$name]['display'])) {
							echo "<td class=\"banner\">".$db_sql_columns[$name]['name']."</td>";
						}
					}
			
					if (($_GET['col_params'] == on) || ($_GET['columns'] == "default")) {
						echo "<td class=\"banner\">Parameters</td>";
					}
		 
					/* generate the common request url used when drilling down through the data */
					$get_url  = "columns=".$_GET['columns']."&amp;limit=".$_GET['limit'];
					$get_url .= "&amp;instance=".$_GET['instance']."&amp;page=1";

					foreach (array_keys($db_sql_columns) as $name) {
						$get_url .= "&amp;".$name."=".$_GET[$name];
					}
			
					if ($_GET['col_params']) {
						$get_url .= "&amp;col_params=".$_GET['col_params'];
					}
				
				} else {
					echo "<td>";
					echo "<table align=\"center\" cellspacing=\"0\" cellpadding=\"4\" width=\"200\">";
					echo	"<tr align=\"center\">";
					echo		"<td><strong>No results found!</strong></td>";
					echo	"</tr>";
					echo "</table>";
					echo "</td>";	
				}
				
				/* append rows to table */
				if (count($data)) {
					echo "</tr>";

					for ($i = 0, $keys = array_keys($data); $i < count($keys); $i++) {
						echo "<tr>";
	
						/* mandatory fields */
						echo "<td class=\"timestamp\">".$data[$keys[$i]]['Timestamp']."</td>";
			
						/* user defined fields */
						foreach (array_keys($db_sql_columns) as $name) {
							if ((!$db_sql_columns[$name]['name']) || 
					 		   (!$db_sql_columns[$name]['display'])) {
								continue;
							}
	
							$value = $data[$keys[$i]][$db_sql_columns[$name]['name']];
							$class = str_replace("col_", "", $name);
			
							/* apply correct css class for severity field */
							if ($db_sql_columns[$name]['name'] == "Severity") {
								echo "<td class=\"sev_".strtolower($value)."\">".$value."</td>";  
							}

							/* if no value is available we still pad the table cell otherwise the table
			 				 * borders are not rendered correctly
			 				 */
							else if (!$value) {
								echo "<td >&nbsp;</td>";
							}
				
							/* hyperlink required for drill down */
							else if ($db_sql_columns[$name]['href'] != "") {

								/* adjustments required to handle ns file id */
								if (($db_sql_columns[$name]['name'] == "NS File ID") && ($use_database_view)) {
									echo "<td class=\"".$class."\">";
									echo "<a href=\"dbview.php?instance=".$_GET['instance']."&nsfileid=".urlencode($value)."\" target=\"blank\">".$value."</a>";
									echo "</td>";
									continue;
								} 
			
								$drill = explode(".", $data[$keys[$i]]['Timestamp']);
								$drill = "drilltime=".urlencode($drill[0]);
	
								echo "<td class=\"".$class."\">";
								if ($data[$keys[$i]]["m_".$db_sql_columns[$name]['href']]) {
									echo "<a href=\"".$PHP_SELF."?".$drill."&amp;".$get_url."&amp;".$db_sql_columns[$name]['href']."=".$data[$keys[$i]]["m_".$db_sql_columns[$name]['href']]."\">".$value."</a>";
								} else {
									echo "<a href=\"".$PHP_SELF."?".$drill."&amp;".$get_url."&amp;".$db_sql_columns[$name]['href']."=".urlencode($value)."\">".$value."</a>";
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
							echo "<td class=\"".$class."\">";
							if (count($data[$keys[$i]]['Parameters'])) {
								for ($j = 0; $j < count($data[$keys[$i]]['Parameters']); $j++) {
									echo $data[$keys[$i]]['Parameters'][$j]."<br/>";
								}
							} else {
								echo "&nbsp;";
							}
							echo "</td>";
						}
						echo "</tr>";
					}
				}

				?>
				</table>
			</td>
		</tr>
	</table>

	<!-- navigation -->
	<table class="noborder" cellspacing="0" cellpadding="0" width="100%">
		<tr>
			<td align="left" valign="bottom">Connected to: 
			<?php 
			if ($show_db_version) { 
				echo $db_instances[$_GET['instance']]['username']."@".$db_instances[$_GET['instance']]['server']." (".$db_instances[$_GET['instance']]['type']." ".db_server_version($dbh).")";
			} else {
				echo $db_instances[$_GET['instance']]['username']."@".$db_instances[$_GET['instance']]['server']." (".$db_instances[$_GET['instance']]['type'].")";
			}
		
			if (count($data)) {
				echo "</td>";
				echo "<td align=\"right\">";
					echo "<form id=\"navigation\" name=\"navigation\" method=\"get\" action=\"results.php\">";
				
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
						echo "<input type=\"submit\" name=\"nav\" value=\"Prev\" />&nbsp;&nbsp;";
					}
					if ($query_total > $_GET['limit']) {
						echo "<input type=\"submit\" name=\"nav\" value=\"Next\" />";
					} else {
						echo "&nbsp;";
					}

				echo "</form>";
		 		echo "</td>";

			}

			?>
		</tr>
	</table>

	<!-- break 2 -->
	<hr align="left" size="1" />
	
	<!-- footer -->
	<table class="noborder" cellspacing="0" cellpadding="0" width="100%">
		<tr valign="top">
			<?php $page_end = getmicrotime(); ?>
			<td align="left" width="33%">DLF interface version: <?php echo $version ?></td>
			<td align="center" width="33%"><?php printf("Page generated in %.3f seconds with %d sql queries", $page_end - $page_start, $querycount); ?></td>
			<td align="right" width="33%">
			<a href="http://validator.w3.org/check?uri=referer"><img src="images/xhtml.png" alt="Valid XHTML 1.0 Strict" /></a>&nbsp; 
			<a href="http://jigsaw.w3.org/css-validator/check/referer"><img src="images/css.png" alt="Valid CSS 2.0" /></a>&nbsp; 
			<a href="http://www.php.net/"><img src="images/php.png" alt="Powered by PHP" /></a>&nbsp;
			</td>
		</tr>
	</table>
</body>
</html>
