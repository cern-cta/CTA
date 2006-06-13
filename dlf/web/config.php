<?php

/******************************************************************************************************
 *                                                                                                    *
 * config.php                                                                                         *
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
 * Version of the DLF web interface
 */
$version = "1.0.0-1";

/**
 * Use the javascript/css calendar popup for date selection?
 */
$use_calendar_popup = 1;

/**
 * Allow hosts to be manually entered by the user?
 */
$use_strict_hosts = 1;

/**
 * Use the database view to expand information on ns file ids
 *   - note: not available in versions < 1.0.0-1
 */
$use_database_view = 0;

/**
 * Display the version of the database on results pages?
 */
$show_db_version = 1;

/**
 * Database connectivity information
 */
$db_instances = array(

	"castor1" => array(
		"type"	   => "MySQL",
		"username" => "castor_dlf",
		"password" => "password",
		"server"   => "dbserver2.domain.com",
		"database" => "dlf"                   /* only required for mysql! */
	),

	"castor2" => array(
		"type"	   => "Oracle",
		"username" => "castor_dlf",
		"password" => "password",
		"server"   => "dbserver1.domain.com"
	),
);

/**
 * This setting defines the time frame the results should encompass when drilling down through data.
 * For example, when selecting a Request ID link the results page will group together all messages 
 * matching that request id +/- X hours from the selected message.
 *
 * Warning: setting this value too high can cause very large table scans to be performed impacting
 *		   database performance.
 */
$db_drilldown_time = 48;

/**
 * SQL fields
 *
 * Caution: changing these settings will change the way in which sql queries are dynamically constructed			
 */
$db_sql_columns = array(

	"col_severity" => array(
		"field"	   => "t2.sev_name",
		"name"	   => "Severity",
		"join"	   => "INNER JOIN dlf_severities t2 on (t1.severity = t2.sev_no)",
		"display"  => 0,
		"required" => 0,
		"href"	   => "",
		"width"	   => 60,
	),

	"col_hostname" => array(
		"field"	   => "t3.hostname",
		"name"	   => "Hostname",
		"join"	   => "INNER JOIN dlf_host_map t3 on (t1.hostid = t3.hostid)",
		"display"  => 0,
		"required" => 0,
		"href"	   => "hostname",
		"width"	   => 100
	),

	"col_facility" => array(
		"field"	   => "t4.fac_name",
		"name"	   => "Facility",
		"join"	   => "INNER JOIN dlf_facilities t4 on (t1.facility = t4.fac_no)",
		"display"  => 0,
		"required" => 0,
		"href"	   => "facility",
		"width"    => 100
	),

	"col_pid" => array(
		"field"	   => "t1.pid",
		"name"	   => "PID",
		"join"	   => "",
		"display"  => 0,
		"required" => 0,
		"href"	   => "pid",
		"width"	   => 100
	),

	"col_tid" => array(
		"field"	   => "t1.tid",
		"name"	   => "TID",
		"join"	   => "",
		"display"  => 0,
		"required" => 0,
		"href"	   => "",
		"width"	   => 100
	),

	"col_msgtext" => array(
		"field"	   => "t5.msg_text",
		"name"	   => "Message Text",
		"join"	   => "INNER JOIN dlf_msg_texts t5 on ((t1.msg_no = t5.msg_no) AND (t1.facility = t5.fac_no))",
		"display"  => 0,
		"required" => 0,
		"href"	   => "",
		"width"    => 100
	),

	"col_nshostname" => array(
		"field"	   => "t6.nshostname",
		"name"	   => "NS Hostname",
		"join"	   => "INNER JOIN dlf_nshost_map t6 on (t1.nshostid = t6.nshostid)",
		"display"  => 0,
		"required" => 0,
		"href"	   => "nshostname",
		"width"	   => 100
	),

	"col_nsfileid" => array(
		"field"	   => "t1.nsfileid",
		"name"	   => "NS File ID",
		"join"	   => "",
		"display"  => 0,
		"required" => 0,
		"href"	   => "nsfileid",
		"width"	   => 100
	),

	"col_reqid" => array(
		"field"	   => "t1.reqid",
		"name"	   => "Request ID",
		"join"	   => "",
		"display"  => 0,
		"required" => 0,
		"href"	   => "reqid",
		"width"	   => 100
	),

	"col_subreqid" => array(
		"field"	   => "t7.subreqid",
		"name"     => "Sub Request ID",
		"join"	   => "LEFT JOIN dlf_reqid_map t7 on (t1.id = t7.id)",
		"display"  => 0,
		"required" => 0,
		"href"	   => "subreqid",
		"width"	   => 100
	),

	"col_tapevid" => array(
		"field"	   => "t8.tapevid",
		"name"	   => "Tape VID",
		"join"	   => "LEFT JOIN dlf_tape_ids t8 on (t1.id = t8.id)",
		"display"  => 0,
		"required" => 0,
		"href"	   => "tapevid",
		"width"	   => 100
	),
);

/**
 * SQL WHERE conditions
 */
$db_sql_conditions = array(

	"severity" => array(
		"field"	    => "t1.severity",
		"defined"   => 0,
		"wildcard"  => 0
	),

	"facility" => array(
		"field"	    => "t1.facility",
		"defined"   => 0,
		"wildcard"  => 0
	),

	"hostname" => array(
		"field"	    => "t1.hostid",
		"defined"   => 0,
		"wildcard"  => 1
	),

	"msgtext" => array(
		"field"	    => "t5.msg_no",
		"defined"   => 0,
		"wildcard"  => 0
	),

	"pid" => array(
		"field"	    => "t1.pid",
		"defined"   => 0,
		"wildcard"  => 0
	),

	"tapevid" => array(
		"field"	    => "t8.tapevid",
		"defined"   => 0,
		"wildcard"  => 0
	),

	"reqid" => array(
		"field"	    => "t1.reqid",
		"defined"   => 0,
		"wildcard"  => 0
	),

	"subreqid" => array(
		"field"	    => "t7.subreqid",
		"defined"   => 0,
		"wildcard"  => 0
	),

	"nshostname" => array(
		"field"     => "t1.nshostid",
		"defined"   => 0,
		"wildcard"  => 1
	),

	"nsfileid" => array(
		"field"	    => "t1.nsfileid",
		"defined"   => 0,
		"wildcard"  => 0
	)
);


?>
