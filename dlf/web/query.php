<?php

/******************************************************************************************************
 *                                                                                                    *
 * index.php                                                                                          *
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
 * $Id: query.php,v 1.15 2007/06/28 06:20:08 waldron Exp $
 */

require("utils.php");
include("config.php");
include("/var/www/conf/dlf/login.conf");

/* include the correct database interface module */
include("db/".strtolower($db_instances[$_GET['instance']]['type']).".php");

/* page variables */
$dbh            = db_connect($_GET['instance'], 1, 0);
$gen_start      = getmicrotime();
$query_count    = 0;
$schema_version = db_schema_version($dbh);

setcookie("instance", $_GET['instance']);

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
	<title><?php printf("%s - ", strtoupper($_GET['instance'])); ?>Distributed Logging Facility - Query</title>
	<link href="site.css" rel="stylesheet" type="text/css" />
	
	<?php

	/* load calendar popup ? */
	if ($use_calendar_popup) {
		print "<script language=\"javascript\" src=\"js/CalendarPopup.js\" type=\"text/javascript\"></script>\n";
	}

	/* generate message text array */
	$results = db_query("SELECT fac_no, msg_no, msg_text FROM dlf_msg_texts	ORDER by fac_no, UPPER(msg_text)", $dbh);
	if (!$results) {
		exit;
	}

	/* loop over results */
	for ($i = 0, $fac_no = -2, $all_msgtexts = ""; $row = db_fetch_row($results); $i++) {

		/* a new facility ? */
		if ($row[0] != $fac_no) {
			$all_msgtexts .= "['".$row[0]."','All','All'],\n\t\t";
		}
		$fac_no = $row[0];
	
		/* array entry */
		$all_msgtexts .= "['".$row[0]."','".$row[1]."','".str_replace("'", "\'", str_replace("\n", "", $row[2]))."']";
	}

	echo "\t<script type=\"text/javascript\"> \n\t\tvar txtOptions = [";
	echo str_replace("']['", "'],\n\t\t['", trim($all_msgtexts));
	echo "]\n;";

	?>

	// javascript function to populate msgtext box
	function fac_selected(theSel) {
		theForm = theSel.form;
		opt = theForm.msg_no.options;
		opt.length = 0;
		for (i = 0, k = 0; i < txtOptions.length; i++) {
			if (txtOptions[i][0] == theSel.value) {
				value = txtOptions[i][1];
				name  = txtOptions[i][2];
				for (j = 0; j < opt.length; j++) {
					if (opt[j].value == value) value = ""; 
				}
				if (value > "") {
					opt[opt.length] = new Option(name, value);
					k++;
				}
			}
		}
		if (k == 0) {
			opt[opt.length] = new Option('All', -1);
		}
	}

	// reset the 'from' and 'to' fields
	function reset_dates() {
		document.query.from.value     = "dd/mm/yyyy";
		document.query.fromtime.value = "00:00";
		document.query.to.value	      = "dd/mm/yyyy";
		document.query.totime.value   = "00:00";
	}

	// toggle colume checkboxes
	function check_all(check) {
		document.query.col_nsfileid.checked   = check;
		document.query.col_msgtext.checked    = check;
		document.query.col_severity.checked   = check;
		document.query.col_reqid.checked      = check;
		document.query.col_hostname.checked   = check;
		document.query.col_subreqid.checked   = check;
		document.query.col_pid.checked	      = check;
		document.query.col_tapevid.checked    = check;
		document.query.col_tid.checked	      = check;
		document.query.col_params.checked     = check;																				
		document.query.col_nshostname.checked = check;
		<?php
			if ($schema_version > 1) {
				echo "document.query.col_security.checked = check";
			}
		?>
	}
	 
	// toggle/adjust column option radio buttons   
	function custom_columns() {
		if ((document.query.col_nsfileid.checked == true) &&
		    (document.query.col_msgtext.checked  == true) &&
		    (document.query.col_severity.checked == true) &&
		    (document.query.col_reqid.checked == true)	  &&
		    (document.query.col_hostname.checked == true) &&
		    (document.query.col_subreqid.checked == true) &&
		    (document.query.col_pid.checked == true)	  &&
		    (document.query.col_tapevid.checked == true)  &&
		    (document.query.col_tid.checked == true)	  &&
		    (document.query.col_params.checked == true)	  &&
		    <?php
			    if ($schema_version > 1) {
				   echo "(document.query.col_security.checked == true) &&";
		    	}
		    ?>																		
		    (document.query.col_nshostname.checked == true)) {
			document.query.columns[0].checked = true;
			document.query.columns[1].checked = false;
		} else {
			document.query.columns[0].checked = false;
			document.query.columns[1].checked = true;
		}
	}

	// is a number ?
	function is_num(str) {
		var validChars = "0123456789.-+";
		var isNum = true;
		var char;

		for (i = 0; i < str.length && isNum == true; i++) {
			char = str.charAt(i);
			if (validChars.indexOf(char) == -1) {
				isNum = false;
			}
		}
		return isNum;
	}

	// validate form content
	function validate_form() {
		if (document.query.last.value == 0) {
			if ((document.query.from.value == "dd/mm/yyyy") && (document.query.to.value == "dd/mm/yyyy")) {
				alert("No timestamp criteria defined");
				return false;
			}
			if ((document.query.from.value != "dd/mm/yyyy") &&
			    (document.query.to.value   != "dd/mm/yyyy") &&
			    (compareDates(document.query.to.value + " " + document.query.totime.value, 'dd/MM/yyyy HH:mm',
					  document.query.from.value + " " + document.query.fromtime.value, 'dd/MM/yyyy HH:mm')) != 1) {
				alert("The from date must be smaller then the to date");
				return false;
			}
			if (document.query.to.value == "dd/mm/yyyy") {
				var now = new Date();
				document.query.to.value     = now.getDate() + "/" + (now.getMonth() + 1) + "/" + now.getYear();
				document.query.totime.value = now.getHours() + ":" + now.getMinutes();		
			}
	  	}
		if (((document.query.paramname.value != "") && (document.query.paramvalue.value == "")) ||
			((document.query.paramname.value == "") && (document.query.paramvalue.value != ""))) {
			alert("When searching by parameter both 'name' and 'value' must be specified");
			return false;
		}
		if (document.query.pid.value != "") {
			if (!is_num(document.query.pid.value)) {
				alert("Process id field contains an invalid number");
				return false;
			}
		}
		if (document.query.nsfileid.value != "") {
                        if (!is_num(document.query.nsfileid.value)) {
                                alert("NS file id field contains an invalid number");
                                return false;
                        }
                }

                <?php

		if ($schema_version > 1) {
                	echo "if (document.query.userid.value != \"\") {";
       	                echo "	if (!is_num(document.query.userid.value)) {";
               	        echo "        	alert(\"User id field contains an invalid number\");";
                        echo "	        return false;";
	                echo "  }";
	                echo "}";
	                echo "if (document.query.groupid.value != \"\") {";
        	        echo "  if (!is_num(document.query.groupid.value)) {";
			echo "		alert(\"Group id field contains an invalid number\");";
			echo "		return false;";
                        echo "	}";
                	echo "}";
		}

		?>

		return true;
	}
	
</script>
</head>
<body onload="fac_selected(document.query.facility)">
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
		<td colspan="3" valign="middle">

		<div id="datediv" style="position:absolute; visibility:hidden; background-color:white;"></div>

		<!-- start form -->
		<form action="results.php" method="get" name="query" onsubmit="return validate_form();" id="query">
		<table class="noborder" align="center" cellspacing="0" cellpadding="4">
		<tr>
			<td colspan="2" class="banner">Message Options</td>
			<td width="30"></td>
			<td colspan="4" class="banner">Parameter Options</td>
			<td width="30"></td>
			<td colspan="3" class="banner">Timestamp Options</td>
		</tr>

		<tr>
			<td width="120" rowspan="4" align="left">Severity:</td>
			<td width="160" rowspan="4">
			<select name="severity[]" size="5" multiple="multiple" class="querySelect" tabindex="1">
			<option selected="selected">All</option>
			<?php
				$results = db_query("SELECT sev_no, sev_name FROM dlf_severities ORDER by sev_no ASC", $dbh);
				if (!$results) {
					exit;
				}
				while ($row = db_fetch_row($results)) {
					echo "<option value=\"".$row[0]."\">".$row[1]."</option>";
				}
			?>
			</select>		
		</td>
		<td width="30">&nbsp;</td>
		<td width="120" align="left">Parameter Name:</td>
		<td colspan="3">
			<input type="text" name="paramname" class="queryInput" tabindex="11" />		
		</td>
		<td width="30">&nbsp;</td>
		<td width="120" align="left">Last:</td>
		<td colspan="2">
			<select name="last" size="1" class="querySelect" tabindex="13" onchange="reset_dates()">
				<option value="0" selected="selected"></option>
				<option value="10">10 minutes</option>
				<option value="20 ">20 minutes</option>
				<option value="60">1 Hour</option>
				<option value="360">6 hours</option>
				<option value="1440">1 day</option>
				<option value="-1">Everything</option>
			</select>		
		</td>
		</tr>

		<tr>
			<td width="30">&nbsp;</td>
			<td align="left">Parameter Value:</td>
			<td colspan="3">
				<input type="text" name="paramvalue" class="queryInput" tabindex="12" />			
			</td>
			<td width="30">&nbsp;</td>
			<td colspan="3" align="center">- OR -</td>
		</tr>

		<tr>
			<td width="30">&nbsp;</td>
			<td>&nbsp;</td>
			<td colspan="3">&nbsp;</td>
			<td width="30">&nbsp;</td>
			<td rowspan="2" align="left">From:</td>
			<td width="97" rowspan="2">
			<?php 
			if ($use_calendar_popup) {
				echo "<script language=\"JavaScript\" type=\"text/javascript\" id=\"js1\">";
				echo "var calendar1 = new CalendarPopup(\"datediv\");";
				echo "calendar1.setCssPrefix(\"Calendar\");"; 
				echo "</script>";
			}
			?>
			<input name="from" type="text" class="queryTime" value="dd/mm/yyyy" tabindex="14" 
			<?php if ($use_calendar_popup) { 
				echo "onclick=\"calendar1.select(document.forms[0].from, 'anchor1', 'dd/MM/yyyy'); this.form.last.options[0].selected = true; return false;\""; 
			} else {
				echo "onclick=\"this.form.last.options[0].selected = true;\"";
			} 
			?> />
			</td>
			<td width="63" rowspan="2"><input name="fromtime" type="text" value="00:00" size="4" maxlength="5" tabindex="15" />
				<a href="#" name="anchor1" class="vanish" id="anchor1"></a>
			</td>
		</tr>

		<tr>
			<td width="30">&nbsp;</td>
			<td colspan="4" class="banner">Column Options</td>
			<td width="30">&nbsp;</td>
		</tr>

		<tr>
			<td align="left">Facility:</td>
			<td>
				<select name="facility" onchange="fac_selected(this);" class="querySelect" tabindex="2">
				<option selected="selected">All</option>
				<?php
				$results = db_query("SELECT fac_no, fac_name FROM dlf_facilities ORDER by UPPER(fac_name)", $dbh);
				if (!$results) {
					exit;
				}
				while ($row = db_fetch_row($results)) {
					echo "<option value=\"".$row[0]."\">".$row[1]."</option>";
				}
				?>
				</select>
			</td>
			<td width="30">&nbsp;</td>
			<td align="left">Default columns</td>
			<td width="21">
				<input name="columns" type="radio" value="default" checked="checked" onclick="check_all(true)" />
			</td>
			<td width="100" align="left">Custom Colums</td>
			<td width="21">
				<input name="columns" type="radio" value="custom" onclick="check_all(false)" />
			</td>
			<td width="30">&nbsp;</td>
			<td width="120" align="left">To:</td>
			<td width="97">
			<?php
			if ($use_calendar_popup) {
				echo "<script language=\"JavaScript\" type=\"text/javascript\" id=\"js2\">";
				echo "var calendar2 = new CalendarPopup(\"datediv\");";
				echo "calendar2.setCssPrefix(\"Calendar\");"; 
				echo "</script>";
			}
			?>
			<input name="to" type="text" class="queryTime" tabindex="16" value="dd/mm/yyyy"
			<?php if ($use_calendar_popup) { 
				echo "onclick=\"calendar2.select(document.forms[0].to, 'anchor2', 'dd/MM/yyyy'); this.form.last.options[0].selected = true; return false;\""; 
			} else {
				echo "onclick=\"this.form.last.options[0].selected = true;\"";
			} 
			?> />
			</td>
			<td width="63"><input name="totime" type="text" value="00:00" size="4" maxlength="5" tabindex="17" />
			<a href="#" name="anchor2" class="vanish" id="anchor2"></a> </td>
		</tr>

		<tr>
			<td align="left">Hostname:</td>
			<td>
			<?php
			if ($use_strict_hosts) {
				echo "<select name=\"hostid\" class=\"querySelect\"><option selected=\"selected\">All</option>";
				$results = db_query("SELECT hostid, hostname FROM dlf_host_map ORDER by UPPER(hostname)", $dbh);
				if (!$results) {
					exit;
				}
				while ($row = db_fetch_row($results)) {
					echo "<option value=\"".$row[0]."\">".$row[1]."</option>";
				}
				echo "</select>";
			} else {
				echo "<input type=\"text\" name=\"hostid\" class=\"queryInput\" tabindex=\"3\"/>";
			}
			?>
			</td>
			<td width="30">&nbsp;</td>
			<td align="left">&nbsp;</td>
			<td>&nbsp;</td>
			<td width="100" align="left">&nbsp;</td>
			<td width="21">&nbsp;</td>
			<td width="30">&nbsp;</td>
			<td>&nbsp;</td>
			<td colspan="2">&nbsp;</td>
		</tr>

		<tr>
			<td rowspan="2" align="left">Message Text :</td>
			<td rowspan="2">
			<select name="msg_no" class="querySelect" tabindex="4">
				<option selected="selected">All</option>
			</select>
			</td>
			<td width="30" rowspan="2">&nbsp;</td>
			<td rowspan="2" align="left">Severity</td>
			<td rowspan="2">
				<input name="col_severity" type="checkbox" checked="checked" onclick="custom_columns()" />
			</td>
			<td width="100" rowspan="2" align="left">NS File ID</td>
			<td width="21" rowspan="2">
				<input name="col_nsfileid" type="checkbox" checked="checked" onclick="custom_columns()" />
			</td>
			<td width="30" rowspan="2">&nbsp;</td>
			<td colspan="3" class="padding"></td>
		</tr>

		<tr>
			<?php
			if ($schema_version > 1) {
				echo "<td colspan=\"3\" class=\"banner\">Security Options</td>";
			} else {
				echo "<td>&nbsp;</td>";
			}
			?>
		</tr>

		<tr>
			<td align="left">Process ID:</td>
			<td>
				<input type="text" name="pid" class="queryInput" tabindex="5" />
			</td>
			<td width="30">&nbsp;</td>
			<td align="left">Hostname</td>
			<td>
				<input name="col_hostname" type="checkbox" checked="checked" onclick="custom_columns()" />
			</td>
			<td width="100" align="left">Message Text</td>
			<td width="21">
				<input name="col_msgtext" type="checkbox" checked="checked" onclick="custom_columns()" />
			</td>
			<td width="30">&nbsp;</td>
			<td width="120" align="left"><?php if ($schema_version > 1) { echo "UID:"; } ?></td>
			<td align="left">
			<?php
			if ($schema_version > 1) {
				echo "<input name=\"userid\" type=\"text\" value=\"\" size=\"3\" maxlength=\"8\" tabindex=\"18\" /> &nbsp; &nbsp; &nbsp;GID:";
			}
			?>
			</td>
			<td>
			<?php
			if ($schema_version > 1) {
				echo "<input name=\"groupid\" type=\"text\" value=\"\" size=\"3\" maxlength=\"8\" tabindex=\"19\" />";
			}
			?>
			</td>
		</tr>

		<tr>
			<td align="left">Tape VID:</td>
			<td>
				<input type="text" name="tapevid" class="queryInput" tabindex="6" />
			</td>
			<td width="30">&nbsp;</td>
			<td align="left">Process ID</td>
			<td>
				<input name="col_pid" type="checkbox" checked="checked" onclick="custom_columns()" />
			</td>
			<td width="100" align="left">Request ID</td>
			<td width="21">
				<input name="col_reqid" type="checkbox" checked="checked" onclick="custom_columns()" />
			</td>
			<td width="30">&nbsp;</td>
			<td align="left"><?php if ($schema_version > 1) { echo "Type:"; } ?></td>
			<td colspan="2"><?php 
			if ($schema_version > 1) { 
				echo "<input type=\"text\" name=\"sec_type\" class=\"queryInput\" tabindex=\"20\" />";
			}
			?>
			</td>
		</tr>

		<tr>
			<td align="left">Request ID:</td>
			<td align="left"><input type="text" name="reqid" class="queryInput" tabindex="7" /></td>
			<td width="30">&nbsp;</td>
			<td align="left">Thread ID</td>
			<td>
				<input name="col_tid" type="checkbox" checked="checked" onclick="custom_columns()" />
			</td>
			<td width="100" align="left">Sub Request ID</td>
			<td width="21">
				<input name="col_subreqid" type="checkbox" checked="checked" onclick="custom_columns()" />
			</td>
			<td width="30">&nbsp;</td>
			<td align="left"><?php if ($schema_version > 1) { echo "Name:"; } ?></td>
			<td colspan="2"><?php 
			if ($schema_version > 1) { 
				echo "<input type=\"text\" name=\"sec_name\" class=\"queryInput\" tabindex=\"20\" />";
			}
			?>
			</td>
		</tr>

		<tr>
			<td align="left">Sub Request ID:</td>
			<td align="left"><input type="text" name="subreqid" class="queryInput" tabindex="8" /></td>
			<td width="30">&nbsp;</td>
			<td align="left">NS Hostname</td>
			<td>
				<input name="col_nshostname" type="checkbox" checked="checked" onclick="custom_columns()" />
			</td>
			<td width="100" align="left">Tape VID</td>
			<td width="21">
				<input name="col_tapevid" type="checkbox" checked="checked" onclick="custom_columns()" />
			</td>
			<td width="30">&nbsp;</td>
			<td align="left">&nbsp;</td>
			<td colspan="2">&nbsp;</td>
		</tr>

		<tr>
			<td align="left">NS Hostname:</td>
			<td>
			<?php
			if ($use_strict_hosts) {
				echo "<select name=\"nshostid\" class=\"querySelect\"><option selected=\"selected\">All</option>";
				$results = db_query("SELECT nshostid, nshostname FROM dlf_nshost_map ORDER by UPPER(nshostname)", $dbh);
				if (!$results) {
					exit;
				}
				while ($row = db_fetch_row($results)) {
					echo "<option value=\"".$row[0]."\">".$row[1]."</option>";
				}
				echo "</select>";
			} else {
				echo "<input type=\"text\" name=\"nshostid\" class=\"queryInput\" tabindex=\"9\"/>";
			}
			?>
			</td>
			<td align="left">&nbsp;</td>
			<td align="left">Parameters</td>
			<td>
				<input name="col_params" type="checkbox" checked="checked" onclick="custom_columns()" />
			</td>
			<td align="left"><?php if ($schema_version > 1) { echo "Security"; } ?></td>
			<td align="left">
			<?php
			if ($schema_version > 1) {
				echo "<input name=\"col_security\" type=\"checkbox\" checked=\"checked\" onclick=\"custom_columns()\" />";
			}
			?>
			</td>
			<td width="30">&nbsp;</td>
			<td align="left">Sort By:</td>
			<td colspan="2"><select name="sort" size="1" class="querySelect" tabindex="21">
                <option value="tdesc" selected="selected">Time (Descending)</option>
                <option value="tasc">Time (Ascending)</option>
              </select>
            </td>
		</tr>

		<tr>
			<td align="left">NS File ID:</td>
			<td><input type="text" name="nsfileid" class="queryInput" tabindex="10" /></td>
			<td align="left">&nbsp;</td>
			<td align="left">&nbsp;</td>
			<td width="21">&nbsp;</td>
			<td width="100">&nbsp;</td>
			<td width="21">&nbsp;</td>
			<td width="30">&nbsp;</td>
			<td align="left">Style:</td>
			<td colspan="2"><select name="style" size="1" class="querySelect" tabindex="22">
                <?php
				$i = 1;
				foreach (array('Classic', 'Multi-line', 'Single') as $value) {
					if (isset($HTTP_COOKIE_VARS['style']) && ($HTTP_COOKIE_VARS['style'] == $i)) {
						echo "<option value=\"".$i."\" selected=\"selected\" >".$value."</option>";
					} else {
						echo "<option value=\"".$i."\">".$value."</option>";
					}
					$i++;
				}
				?>
              </select>
            </td>
		</tr>

		<tr>
			<td align="left">&nbsp;</td>
			<td>&nbsp;</td>
			<td align="left">&nbsp;</td>
			<td align="left">&nbsp;</td>
			<td></td>
			<td></td>
			<td></td>
			<td width="30"></td>
			<td align="left">Lines per Page:</td>
			<td colspan="2"><select name="limit" size="1" class="querySelect" tabindex="23">
                <option value="10">10</option>
                <option value="20">20</option>
                <option value="50">50</option>
                <option value="100">100</option>
                <option value="200">200</option>
                <option value="500" selected="selected">500</option>
                <option value="1000">1000</option>
              </select>
            </td>
		</tr>

		<tr>
			<td>
				<input type="hidden" name="instance" value="<?php echo $_GET['instance']; ?>" />
				<input type="hidden" name="page" value="1" />
			</td>
			<td>&nbsp;</td>
			<td width="30">&nbsp;</td>
			<td>&nbsp;</td>
			<td>&nbsp;</td>
			<td>&nbsp;</td>
			<td>&nbsp;</td>
			<td width="30">&nbsp;</td>
			<td>&nbsp;</td>
			<td colspan="2">
				<div align="right">
				<input type="submit" name="Submit" value="Submit" class="button" />
				&nbsp;
				<input type="reset" name="Reset" value="&nbsp;Reset&nbsp;" class="button" />
				</div>
			</td>
		</tr>
		</table>
		</form>

		<!-- end form -->	
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
