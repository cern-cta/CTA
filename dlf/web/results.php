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
sanitize_input();
include("config.php");
include("/var/www/conf/dlf/login.conf");
include("db/oracle.php");

$dbh         = db_connect($_GET['instance'], 0, 0);
$gen_start   = getmicrotime();
$query_count = 0;

/* set defaults */
if (!isset($_GET['style'])) {
  $_GET['style'] = 1;
}
$_GET['col_facility'] = "on";

if (isset($_GET['msg_no']) && ($_GET['msg_no'] < 0)) {
  $_GET['msg_no'] = 'All';
}

/* determine the schema name to be used */
if (!isset($db_instances[$_GET['instance']]['schema'])) {
  trigger_error("Unknown schema name for dlf database");
  exit(0);
}
$schema = $db_instances[$_GET['instance']]['schema'];

setcookie("style", $_GET['style']);

?>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">

<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
<head>
  <meta name="author" content="Castor Dev" />
  <meta name="description" content="Distributed Logging Facility" />
  <meta name="robots" content="noindex,nofollow" />
  <meta http-equiv="Content-Type" content="text/html; charset=us-ascii" />
  <meta http-equiv="Pragma" content="no-cache" />
  <meta http-equiv="Cache-Control" content="no-cache" />
  <meta http-equiv="Default-Style" content="compact" />
  <meta http-equiv="Content-Style-Type" content="text/css" />
  <link rel="stylesheet" type="text/css" title="compact" href="site.css" />
  <title><?php printf("%s - ", strtoupper($_GET['instance'])); ?>Distributed Logging Facility - Results</title>

  <?php

  /* start creation of bind variables */
  $bindvars = array();

  /* start of main query */
  $query = "SELECT * FROM (SELECT p.*, ROWNUM RNUM FROM (
              SELECT t1.id, t1.timestamp, t1.timeusec, t1.severity, t1.facility, t1.hostid, t1.nshostid, t1.reqid,
              t2.hostname, t3.fac_name, t4.sev_name, t5.msg_text, t1.pid, t1.tid, t6.nshostname, t1.nsfileid,
              t1.tapevid, t1.subreqid, t1.userid, t1.groupid, t1.sec_type, t1.sec_name
              FROM  ".$schema.".dlf_messages t1
              INNER JOIN ".$schema.".dlf_host_map   t2 ON (t1.hostid   = t2.hostid)
              INNER JOIN ".$schema.".dlf_facilities t3 ON (t1.facility = t3.fac_no)
              INNER JOIN ".$schema.".dlf_severities t4 ON (t1.severity = t4.sev_no)
              INNER JOIN ".$schema.".dlf_msg_texts  t5 ON (t1.msg_no   = t5.msg_no AND t1.facility = t5.fac_no)
              INNER JOIN ".$schema.".dlf_nshost_map t6 ON (t1.nshostid = t6.nshostid) ";

  /* construct severity filter */
  $firstclause = 0;
  $filters = 0;
  if (isset($_GET['severity']) && is_array($_GET['severity']) && ($_GET['severity'][0] != 'All')) {
    $query .= "WHERE t1.severity IN(";
    $firstclause++;
    $filters++;
    foreach (array_keys($_GET['severity']) as $num) {
      $bindvars[':sev'.$num] = $_GET['severity'][$num];
      $query .= ":sev".$num.", ";
    }
    $query = preg_replace('/, $/', ') ', $query);
  }

  /* construct additional filters */
  foreach (array('facility', 'hostid', 'pid', 'msg_no', 'reqid', 'nshostid', 'nsfileid', 'subreqid',
                 'tapevid', 'userid', 'groupid', 'sec_type', 'sec_name') as $name) {
    if (isset($_GET[$name]) && ($_GET[$name] != 'All') && ($_GET[$name] != "")) {
      $query .= ($firstclause) ? "AND t1.".$name." = " : "WHERE t1.".$name." = ";
      $firstclause++;
      $bindvars[$name] = $_GET[$name];
      $query .= ":".$name." ";
      $filters++;
    }
  }

  /* construct the time restrictions */
  $clause = ($firstclause) ? "AND " : "WHERE ";
  $firstclause++;
  if (isset($_GET['drilltime'])) {
    $bindvars[':drilltime'] = $_GET['drilltime'];
    $query .= $clause." ((t1.timestamp >= (TO_DATE(:drilltime, 'DD/MM/YYYY HH24:MI:SS') - ".$db_drilldown_time."/24)) AND
                         (t1.timestamp <  (TO_DATE(:drilltime, 'DD/MM/YYYY HH24:MI:SS') + ".$db_drilldown_time."/24))) ";
  } else if (isset($_GET['last']) && ($_GET['last'] > 0)) {
    $bindvars[':last'] = $_GET['last'];
    $query .= $clause." t1.timestamp > (SYSDATE - :last/1440) ";
  } else if ((isset($_GET['from']) && ($_GET['from'] != "dd/mm/yy")) &&
             (isset($_GET['to']) && ($_GET['to'] != "dd/mm/yy")) &&
             (isset($_GET['fromtime']) && isset($_GET['totime'])) &&
             (isset($_GET['last']) && ($_GET['last'] == 0))) {
    $bindvars[':begin'] = $_GET['from']." ".$_GET['fromtime'].":00";
    $bindvars[':end']   = $_GET['to']." ".$_GET['totime'].":00";
    $query .= $clause." ((t1.timestamp >= TO_DATE(:begin, 'DD/MM/YYYY HH24:MI:SS')) AND
                         (t1.timestamp <  TO_DATE(:end, 'DD/MM/YYYY HH24:MI:SS'))) ";
  } else if ($filters == 0) {
    echo "Querying everything with no filters is not allowed";
    exit(0);
  }
	
  /* ordering */
  $ordering = "DESC";
  if (isset($_GET['sort']) && ($_GET['sort'] == "tasc")) {
    $ordering = "ASC";
  }

  /* check page and limit parameters */
  if (!isset($_GET['page']) || !isset($_GET['limit'])) {
    trigger_error("Missing page and/or limit parameters");
    exit(0);
  }
  if (!is_numeric($_GET['limit']) || !is_numeric($_GET['page'])) {
    trigger_error("Invalid page and/or limit parameters");
    exit(0);
  }
  if (($_GET['limit'] < 0) || ($_GET['limit'] > 1000)) {
    trigger_error("Invalid limit parameter");
    exit(0);
  }
  if ($_GET['page'] < 0) {
    triggr("Invalid page parameter");
    exit(0);
  }

  /* append ordering and output limitations */
  $bindvars[':page']  = $_GET['page'];
  $bindvars[':limit'] = $_GET['limit'];
  $query .= "ORDER BY timestamp $ordering, timeusec $ordering) p)
             WHERE (RNUM > (:page - 1) * :limit AND RNUM <= :page * :limit) ";

  /* execute query */
  $results = db_query($query, $dbh, $bindvars);
  if (!$results) {
    exit;
  }

  $data = array();
  while ($row = db_fetch_row($results)) {
    $id = $row[0];
    $data[$id]['col_timestamp']  = $row[1].".".$row[2];
    $data[$id]['col_reqid']      = $row[7];
    $data[$id]['col_hostname']   = $row[8];
    $data[$id]['col_facility']   = $row[9];
    $data[$id]['col_severity']   = $row[10];
    $data[$id]['col_msgtext']    = $row[11];
    $data[$id]['col_pid']        = $row[12];
    $data[$id]['col_tid']        = $row[13];
    $data[$id]['col_nshostname'] = $row[14];
    $data[$id]['col_nsfileid']   = $row[15];
    $data[$id]['col_tapevid']  = $row[16];
    $data[$id]['col_subreqid'] = $row[17];
    if ((($row[18] != -1) && ($row[18])) || (($row[19] != -1)  && ($row[19]))|| (($row[20] != "N/A") && ($row[20])) || (($row[21] != "N/A") && ($row[21]))) {
      $data[$id]['col_security'] = "UID=".$row[18]." GID=".$row[19]." TYPE=\"".$row[20]."\" NAME=\"".$row[21]."\"";
    }
    $data[$id]['m_severity']     = $row[3];
    $data[$id]['m_facility']     = $row[4];
    $data[$id]['m_hostname']     = $row[5];
    $data[$id]['m_nshostname']   = $row[6];
  }

  /* construct query to count total number of results */
  $query = strstr($query, "SELECT t1");
  $query = substr($query, 0, strpos($query, "ORDER BY"));
  $query = "SELECT COUNT(*) FROM (".$query.")";

  /* delete unneeded bind variables */
  unset($bindvars[':page']);
  unset($bindvars[':limit']);

  /* fetch total amount of rows */
  $results = db_query($query, $dbh, $bindvars);
  if (!$results) {
    exit;
  }

  $row = db_fetch_row($results);
  $query_total = $row[0];

  /* reset bind variables */
  $bindvars = array();

  /* extract the parameters */
  if (((isset($_GET['col_params']) && ($_GET['col_params'] == "on")) ||
       (isset($_GET['columns']) && ($_GET['columns'] == "default"))) &&
       (count($data))) {

    $query = "SELECT id, timestamp, name, value FROM ".$schema.".:table WHERE id IN (";
    foreach (array_keys($data) as $num) {
      $query .= $num.", ";
    }
    $query = preg_replace('/, $/', ') ', $query);

    if (isset($_GET['drilltime'])) {
      $bindvars[':drilltime'] = $_GET['drilltime'];
      $query .= "AND ((timestamp >= (TO_DATE(:drilltime, 'DD/MM/YYYY HH24:MI:SS') - ".$db_drilldown_time."/24)) AND
                      (timestamp <  (TO_DATE(:drilltime, 'DD/MM/YYYY HH24:MI:SS') + ".$db_drilldown_time."/24)))";
    } else if (isset($_GET['last']) && ($_GET['last'] > 0)) {
      $bindvars[':last'] = $_GET['last'] + 2;
      $query .= "AND timestamp > (SYSDATE - :last/1440)";
    } else if ((isset($_GET['from']) && ($_GET['from'] != "dd/mm/yy")) &&
               (isset($_GET['to']) && ($_GET['to'] != "dd/mm/yy")) &&
               (isset($_GET['fromtime']) && isset($_GET['totime'])) &&
              (isset($_GET['last']) && ($_GET['last'] == 0))) {
      $bindvars[':begin'] = $_GET['from']." ".$_GET['fromtime'].":00";
      $bindvars[':end']   = $_GET['to']." ".$_GET['totime'].":00";
      $query .= "AND ((timestamp >= TO_DATE(:begin, 'DD/MM/YYYY HH24:MI:SS')) AND
                      (timestamp <  TO_DATE(:end, 'DD/MM/YYYY HH24:MI:SS')))";
    }

    /* loop over parameter tables */
    foreach (array('dlf_str_param_values', 'dlf_num_param_values') as $table) {
      $exe_query = str_replace(':table', $table, $query);

      $results = db_query($exe_query, $dbh, $bindvars);
      if (!$results) {
        exit;
      }

      $i = 0;
      if (isset($data[$row[0]]['col_params'])) {
        $i = count($data[$row[0]]['col_params']);
      }

      /* append the parameter information to the data structure */
      for ($prev_row = ""; $row = db_fetch_row($results); $i++) {
        if ($prev_row != $row[0]) {
          if (isset($data[$row[0]]['col_params'])) {
            $i = count($data[$row[0]]['col_params']);
          } else {
            $i = 0;
          }
        }
        $prev_row = $row[0];
        $data[$row[0]]['col_params'][$i] = $row[2]."=".$row[3];
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
          if (isset($_GET['drilltime'])) {
            $buf = explode(".", $_GET['drilltime']);
            echo "from <strong>".$buf[0]." +/-".$db_drilldown_time."</strong> hours";
          } else if (($_GET['last'] != 0) && ($_GET['last'] != -1)) {
            $date_to   = explode(" ", date("d/m/Y H:i", time()));
            $date_from = explode(" ", date("d/m/Y H:i", time() - ($_GET['last'] * 60)));

            $_GET['from']     = $date_from[0];
            $_GET['fromtime'] = $date_from[1];
            $_GET['to']       = $date_to[0];
            $_GET['totime']   = $date_to[1];

            echo "between <strong>".$_GET['from']." ".$_GET['fromtime']."</strong> ";
            echo "and <strong>".$_GET['to']." ".$_GET['totime']."</strong> - (".$_GET['last']." minutes)";
          } else if ($_GET['last'] != -1) {
            echo "between <strong>".$_GET['from']." ".$_GET['fromtime']."</strong> ";
            echo "and <strong>".$_GET['to']." ".$_GET['totime']."</strong>";
          }
          $orderby = ($ordering == "DESC") ? "Descending" : "Ascending";
          echo "<br/>";
          echo "Displaying messages <strong> $start_no </strong> through <strong> $end_no </strong> - ordered by timestamp <strong>$orderby</strong><br />";

          $filter_string = "";
          $filters = array('severity:Severity',
                           'facility:Facility:',
                           'hostid:Hostname',
                           'msg_no:Message Text:',
                           'pid:Pid',
                           'tapevid:Tape VID',
                           'reqid:Request ID',
                           'subreqid:Sub Request ID',
                           'nshostid:NS Hostname',
                           'nsfileid:NS File ID',
                           'paramname:Parameter Name',
                           'paramvalue:Parameter Value',
                           'userid:User ID',
                           'groupid:Group ID',
                           'sec_type:Security Type',
                           'sec_name:Security Name');
          foreach ($filters as $config) {
            list ($form_name, $display_name) = split(":", $config);
            if (!isset($_GET[$form_name]) || ($_GET[$form_name] == 'All')) {
              continue;
            }
            $value = $_GET[$form_name];
            if ($value == "") {
              continue;
            }
            $bindvars = array();
	    $bindvars[':n'] = $_GET[$form_name];
            if ($form_name == 'severity') {
              if ($_GET[$form_name][0] == 'All') {
                continue;
              }
              $results = db_query("SELECT sev_no, sev_name FROM ".$schema.".dlf_severities", $dbh);
              while ($row = db_fetch_row($results)) {
                $sevlist[$row[0]] = $row[1];
              }
              $i = 0;
              foreach (array_keys($_GET[$form_name]) as $num) {
                if ($i == 0) {
                  $value = "Severity = <strong>'".$sevlist[$_GET['severity'][$num]]."'</strong> ";
                } else {
                  $value .= "OR Severity = <strong>'".$sevlist[$_GET['severity'][$num]]."'</strong> ";
                }
                $i++;
              }
              $filter_string .= $value;
              continue;
            }
            else if ($form_name == 'facility') {
              $results = db_query("SELECT fac_name FROM ".$schema.".dlf_facilities WHERE fac_no = :n", $dbh, $bindvars);
              $row     = db_fetch_row($results);
              $value   = $row[0];
            }
            else if ($form_name == 'hostid') {
              $results = db_query("SELECT hostname FROM ".$schema.".dlf_host_map WHERE hostid = :n", $dbh, $bindvars);
              $row     = db_fetch_row($results);
              $value   = $row[0] ? $row[0] : "???";
            }
            else if ($form_name == 'msg_no') {
              $bindvars[':f'] = $_GET['facility'];
              $results = db_query("SELECT msg_text FROM ".$schema.".dlf_msg_texts WHERE fac_no = :f AND msg_no = :n", $dbh, $bindvars);
              $row     = db_fetch_row($results);
              $value   = $row[0] ? $row[0] : "???";
            }
            else if ($form_name == 'nshostid') {
              $results = db_query("SELECT nshostname FROM ".$schema.".dlf_nshost_map WHERE nshostid = :n", $dbh, $bindvars);
              $row     = db_fetch_row($results);
              $value   = $row[0] ? $row[0] : "???";
            }
            $filter_string .= ($filter_string) ? " AND " : "";
            $filter_string .= $display_name ." = <strong>'".$value."'</strong>";
          }

          if ($filter_string) {
            echo "&nbsp; &nbsp; Filters: WHERE ".trim($filter_string)."<br />";
          }
          echo "<br/>";

          ?>
          </td>

          <!-- navigation -->
          <td align="right">
          <?php
            $query_string = str_replace("&", "&amp;", $_SERVER['QUERY_STRING']);
            $page_string = generate_pagination($query_total, $_GET['limit'], $_GET['page']);
            if ($page_string) {
              echo "<a href=\"query.php?".$query_string."\">Back to Query</a>&nbsp;&nbsp;-&nbsp;&nbsp;".$page_string;
            } else {
              echo "<a href=\"query.php?".$query_string."\">Back to Query</a>".$page_string;
            }
          ?>
          </td>
        </tr>

        <!-- results -->
        <tr>
          <td colspan="2" height="300" valign="<?php if (!count($data)) { echo "middle"; } else { echo "top"; } ?>">

          <?php

          $columns = array('col_severity:Severity:',
                           'col_hostname:Hostname:m_hostname',
                           'col_facility:Facility:m_facility',
                           'col_pid:PID:pid',
                           'col_tid:TID:',
                           'col_msgtext:Message Text:',
                           'col_nshostname:NS Hostname:m_nshostname',
                           'col_nsfileid:NS File ID:nsfileid',
                           'col_reqid:Request ID:reqid',
                           'col_subreqid:Sub Request ID:subreqid',
                           'col_tapevid:Tape VID:tapevid',
                           'col_params:Parameters:');

          if (!count($data)) {
            echo "<strong>No results found!</strong>";
          } else {


            echo "<table border=\"1\" cellspacing=\"3\" cellpadding=\"3\" width=\"100%\">";
            echo "<tr>";

            /* generate table header */
            echo "<td class=\"banner\">Timestamp</td>";

            $columns_displayed = 0;
            foreach ($columns as $value) {
              list ($col_name, $act_name, $href_name) = split(":", $value, 3);
              if ((isset($_GET['columns']) && ($_GET['columns'] == 'default')) ||
                  (isset($_GET[$col_name]) && ($_GET[$col_name] == "on"))) {
                if ((($_GET['style'] == 2) || ($_GET['style'] == 3)) && ($col_name == 'col_params')) {
                  continue;
                }
                $columns_displayed++;
                echo "<td class=\"banner\">".$act_name."</td>";
              }
            }

            /* generate the common request url used when drilling down through the data */
            $get_url  = "columns=".$_GET['columns']."&amp;limit=".$_GET['limit'];
            $get_url .= "&amp;instance=".$_GET['instance']."&amp;page=1";

            foreach ($columns as $value) {
              list ($col_name, $act_name, $href_name) = split(":", $value, 3);
              if (isset($_GET[$col_name])) {
                $get_url .= "&amp;".$col_name."=".$_GET[$col_name];
              }
            }

            if (isset($_GET['col_params'])) {
              $get_url .= "&amp;col_params=".$_GET['col_params'];
            }

            echo "</tr>";

            /* table data */
            for ($i = 0, $keys = array_keys($data); $i < count($keys); $i++) {
              echo "<tr>";
              echo "<td title=\"$keys[$i]\" class=\"timestamp\">".$data[$keys[$i]]['col_timestamp']."</td>";

              /* user defined fields */
              foreach ($columns as $config) {
                list ($col_name, $act_name, $href_name) = split(":", $config, 3);
                if (!isset($_GET[$col_name])) {
                  continue;
                }

               if ((isset($_GET['columns']) && ($_GET['columns'] != 'default')) &&
                  (isset($_GET[$col_name]) && ($_GET[$col_name] != "on"))) {
                  continue;
                }
                if ($col_name == "col_params") {
                  continue;
                }

                $value = $data[$keys[$i]][$col_name];
                $class = str_replace("col_", "", $col_name);

                /* apply correct css class for severity field */
                if ($col_name == "col_severity") {
                  echo "<td class=\"sev_".strtolower($value)."\">".$value."</td>";
                }

                /* if no value is available we still pad the table cell otherwise the table
                 * borders are not rendered correctly
                 */
                else if (!$value) {
                  echo "<td class=\"".$class."\">N/A</td>";
                }

                /* hyperlink required for drill down */
                else if ($href_name) {

                  $drill = explode(".", $data[$keys[$i]]['col_timestamp']);
                  $drill = "drilltime=".urlencode($drill[0]);

                  /* adjustments required to handle ns file id */
                  if ($act_name == "NS File ID") {
                    if ($use_database_view) {
                      echo "<td class=\"".$class."\">";
                      echo "<a href=\"".$_SERVER['PHP_SELF']."?".$drill."&amp;".$get_url."&amp;".str_replace("m_", "", $href_name)."=".$value."\">".$value."</a><br />";
                      echo "<a href=\"dbview.php?instance=".$_GET['instance']."&amp;entry=castorfile&amp;param=".urlencode($value)."\" target=\"blank\">Stager DB</a>";
                      echo "</td>";
                    } else {
                      echo "<td class=\"".$class."\">";
                      echo "<a href=\"".$_SERVER['PHP_SELF']."?".$drill."&amp;".$get_url."&amp;".str_replace("m_", "", $href_name)."=".$value."\">".$value."</a>";
                      echo "</td>";
                    }
                    continue;
                  }

                  /* add an invisible word-break to request and sub request ids */
                  if (($act_name == "Request ID") ||
                    ($act_name == "Sub Request ID")) {
                      $value = str_replace("-", "-<wbr />", $value);
                  }

                  if ((($col_name = "col_reqid") || ($col_name = "col_subreqid")) && ($_GET['style'] != 1) && ($value != "N/A")) {
                    $class .= "_long";
                  }

                  echo "<td class=\"".$class."\">";
                  if ($value != "N/A") {
                    if (isset($data[$keys[$i]][$href_name])) {
                      echo "<a href=\"".$_SERVER['PHP_SELF']."?".$drill."&amp;".$get_url."&amp;".str_replace("m_", "", $href_name)."=".$data[$keys[$i]][$href_name]."\">".$value."</a>";
                    } else {
                      echo "<a href=\"".$_SERVER['PHP_SELF']."?".$drill."&amp;".$get_url."&amp;".str_replace("m_", "", $href_name)."=".urlencode(str_replace("-<wbr />","-",$value))."\">".$value."</a>";
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
              if (((isset($_GET['col_params']) && ($_GET['col_params'] == "on")) || ($_GET['columns'] == "default")) && ($_GET['style'] == 1)) {
                echo "<td class=\"parameters\">";
                if (isset($data[$keys[$i]]['col_params']) && count($data[$keys[$i]]['col_params'])) {
                  for ($j = 0; $j < count($data[$keys[$i]]['col_params']); $j++) {

                    /* deal with long lines */
                    $value = $data[$keys[$i]]['col_params'][$j];
                    if (strlen($value) > 150) {
                      $value = str_replace("/", "/<wbr />", $value);
                      $value = "<br/>".str_replace(".", ".<wbr />", $value)."<br/>";
                    }

                    echo $value."<br/>";
                  }
                } else if (!isset($data[$keys[$i]]['col_security'])) {
                  echo "&nbsp;";
                }

                if (isset($data[$keys[$i]]['col_security'])) {
                  echo $data[$keys[$i]]['col_security'];
                }
                echo "</td>";
              }

              if (((isset($_GET['col_params']) && ($_GET['col_params'] == "on")) || ($_GET['columns'] == "default")) && ($_GET['style'] == 2) || ($_GET['style'] == 3)) {
                if (count($data[$keys[$i]]['col_params'])) {
                  for ($j = 0, $value = ''; $j < count($data[$keys[$i]]['col_params']); $j++) {
                    $buf = $data[$keys[$i]]['col_params'][$j]. " ";
                    $buf = preg_replace('/^(\w+)=/', '<strong>$1</strong>=', $buf);

                    $value .= $buf;
                    $value .= ($_GET['style'] == 3) ? "&nbsp; &nbsp; " : "<br />";
                  }
                  if (isset($data[$keys[$i]]['col_security'])) {
                    $buf = $data[$keys[$i]]['col_security']. " ";
                    $buf = str_replace('GID=', '&nbsp; &nbsp; <strong>GID</strong>=', $buf);
                    $buf = str_replace('UID=', '<strong>UID</strong>=', $buf);
                    $buf = str_replace('TYPE=', '&nbsp; &nbsp; <strong>TYPE</strong>=', $buf);
                    $buf = str_replace('NAME=', '&nbsp; &nbsp; <strong>NAME</strong>=', $buf);
                    $value .= $buf;
                  }

                  if ($_GET['style'] == 3) {
                    $value .= "<br/><br/>";
                  }
                  echo "<tr><td></td><td align=\"left\" colspan=\"".$columns_displayed."\">$value</td></tr>";
                }
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
          if ($query_total > 10) {
            echo "<tr>";
            echo "<td align=\"right\" valign=\"bottom\" colspan=\"2\">";
              $query_string = str_replace("&", "&amp;", $_SERVER['QUERY_STRING']);
              $page_string = generate_pagination($query_total, $_GET['limit'], $_GET['page']);
              if ($page_string) {
                echo "<a href=\"query.php?".$query_string."\">Back to Query</a>&nbsp;&nbsp;-&nbsp;&nbsp;".$page_string;
              } else {
                echo "<a href=\"query.php?".$query_string."\">Back to Query</a>".$page_string;
              }
            echo "</td>";
            echo "</tr>";
          }
        ?>
      </table>
      <br />
    </td>
  </tr>

  <!-- connectivity information -->
  <tr>
    <td colspan="2">Connected to:
      <?php
      if ($show_db_version) {
        echo $db_instances[$_GET['instance']]['username']."@".$db_instances[$_GET['instance']]['server']." (Oracle ".db_server_version($dbh).")";
      } else {
        echo $db_instances[$_GET['instance']]['username']."@".$db_instances[$_GET['instance']]['server']." (Oracle)";
      }
      ?>
    </td>
    <td align="right">
      <?php
      if ($show_partition_count) {
        echo db_partition_count($dbh, $schema);
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
