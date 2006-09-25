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
 */
$use_database_view = 1;

/**
 * Display the version of the database on results pages?
 */
$show_db_version = 1;

/**
 * Display the number of partition online in the database?
 */
$show_partition_count  = 1;
$max_partitions_online = 30;


/**
 * Database connectivity information
 */
$db_instances = array(

	"castor1" => array(
		"type"     => "MySQL",
		"username" => "castor_dlf",
		"password" => "password",
		"server"   => "dbserver2.domain.com",
		"database" => "dlf"                   /* only required for mysql! */
		
		/* stager database */
		"stagerdb" => array(
			"type"	   => "MySQL",
			"username" => "stager_read",
			"password" => "password",
			"server"   => "dbserver2.domain.com",
			"database" => "dlf",
		),
	),
	
	"castor2" => array(
		"type"     => "Oracle",
		"username" => "castor_dlf",
		"password" => "password",
		"server"   => "dbserver1.domain.com"
		
		/* stager database */
		"stagerdb" => array(
			"type"	   => "Oracle",
			"username" => "stager_read",
			"password" => "password",
			"server"   => "dbserver2.domain.com",
		),
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
 * DLF SQL fields
 *
 * Caution: changing these settings will change the way in which sql queries are dynamically constructed
 */
$dlf_sql_columns = array(

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
 * DLF SQL WHERE conditions
 */
$dlf_sql_conditions = array(

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

/**
 * Stager SQL Tables
 *
 * The configuration information below describes the internal database schema of the stager database and 
 * how tables relate to each. The format is:
 *
 * <tablename> =  array(
 *	"table"		=> "name of the database table"
 *	"lookup"	=> "which column name to lookup the parameter value on"
 *	"fields"	=> array(
 *		This is an array of fields to display to the user and information on how the data should  
 *      	be presented. The format is <column name>:[display option] where display option is one of 
 *      	[size|link|time|status]
 *
 *		When utilising options link and status a corresponding links array and status arrays must 
 *		be defined!
 *	)
 *	"links"		=> array(
 *		This section resolves the link option of the fields above. Every column_name with a link 
 *		shoudl be listed here as a separate array where the options in the array take the format:
 *		<table_name>:<uselinklookup [yes|no]>
 *	)
 *
 *	This option is special and only needs to be defined if one of the link resolvers in the link array
 *	above has the uselinkloop option enabled. We have this because some tables while navigating between
 *	lookup different column names as a foreign key. To resolve this when uselinklookup=yes the 
 *	linklookup value overrides the lookup value defined above
 *
 *	"linklookup"	=> "column_name" 
 * )
 *
 */
$stager_sql_tables = array(
	
	"castorfile" => array(
		"table"		=> "castorfile",
		"lookup" 	=> "fileid",
		"fields" 	=> array(
			"id:link", "nshost", "fileid", "filesize:size", "creationtime:time", "lastaccesstime:time", "svcclass:link", "nbaccesses", "fileclass"
		),
		"links"		=> array(
			"id"		=> array("tapecopy:no", "diskcopy:no", "subrequest:no"),
			"svcclass"	=> array("svcclass:no")
		),
		
		/* */
		"linklookup" => "id",
	),	
	
	"tapecopy"	=> array(
		"table"		=> "tapecopy",
		"lookup"	=> "castorfile",
		"fields"	=> array(
			"id:link", "copynb", "status:status"
		),
		"links"		=> array(
			"id"	=> array("stream2tapecopy:no")
		),
		"status"	=> array(
			"created", "to be migrated", "waiting streams", "selected", "to be recalled", "staged", "failed"
		),
	),
	
	"diskcopy"	=> array(
		"table"		=> "diskcopy",
		"lookup"	=> "castorfile",
		"fields"	=> array(
			"castorfile:link", "filesystem:link", "path", "creationtime:time", "status:status", "gcweight"
		),
		"links"		=> array(
			"castorfile" => array("castorfile:yes"),
			"filesystem" => array("filesystem:no")
		),	
		"status"	=> array(
			"staged", "wait disk2disk copy", "wait tape recall", "deleted", "failed", "waitfs", "stageout", "invalid", "gc candidate", "being deleted", "can be migrated", "wait fs scheduling"
		),
		
		/* */
		"linklookup" => "id",
	),
	
	"filesystem"	=> array(
		"table"		=> "filesystem",
		"lookup"	=> "id",
		"fields"	=> array(
			"diskpool:expand", "diskserver:expand", "status:status", "spacetobefreed:size", "free:size", "mountpoint", "minfreespace:size", "maxfreespace:size"
		),
		"status"	=> array(
			"production", "draining", "disabled"
		),	
		"expands"	=> array(
			"diskpool"	=> array("name"),
			"diskserver"=> array("name", "status:status")
		),
	),
	
	"diskpool"	=> array(
		"table"		=> "diskpool",
		"lookup"	=> "id",
		"fields"	=> array(
			"name"
		),
	),
	
	"diskserver"	=> array(
		"table"		=> "diskserver",
		"lookup"	=> "id",
		"fields"	=> array(
			"name", "status:status"
		),	
		"status"	=> array(
			"production", "draining", "disabled"
		),
	),
	
	"subrequest"	=> array(
		"table"		=> "subrequest",
		"lookup"	=> "castorfile",
		"fields"	=> array(
			"diskcopy:link", "castorfile:link", "retrycounter", "filename", "protocol", "xsize", "priority", "subreqid", "status:status", "parent", "request", "creationtime:time", "lastmodificationtime:time"
		),
		"links"		=> array(
			"diskcopy" 	 => array("diskcopy:yes"),
			"castorfile" => array("castorfile:yes")
		),	
		"status"	=> array(
			"start", "restart", "retry", "wait sched", "wait tape recall", "wait subreq", "ready", "failed", "finished", "failed finished", "failed answering", "archived", "repack"
		),
	),	
	
	"tape"		=> array(
		"table"		=> "tape",
		"lookup"	=> "id",
		"fields"	=> array(
			"vid", "side", "tpmode", "errmsgtext", "errorcode", "xsize", "severity", "stream", "status:status"
		),
		"links"		=> array(
			"diskcopy" 	 => array("diskcopy:no"),
			"castorfile" => array("castorfile:no")
		),	
		"status"	=> array(
			"unused", "pending", "wait drive", "wait monunt", "mounted", "finished", "failed", "unknown"
		),
	),	
	
	"stream"	=> array(
		"table"		=> "stream",
		"lookup"	=> "tape",
		"fields"	=> array(
			"initialsizetotransfer:size", "tape", "tapepool:expand"
		),
		"expands"	=> array(
			"tapepool"	=> array("name"),
		),
	),	

	"tapepool"	=> array(
		"table"		=> "tapepool",
		"lookup"	=> "id",
		"fields"	=> array(
			"name"
		),	
	),
	
	"segment"	=> array(
		"table"		=> "segment",
		"lookup"	=> "id",
		"fields"	=> array(
			"fseq", "offset", "bytes_in:size", "status:status", "bytes_out:size", "host_bytes:size", "segmcksumalgorithm", "segmcksum", "errmsgtxt", "errorcode", "severity", "tape", "copy", "status:status"
		),
		"status"	=> array(
			"unprocesses", "deprecated", "deprecated", "deprecated", "deprecated", "file copied", "failed", "selected", "retired"
		),
	),		

	"svcclass"	=> array(
		"table"		=> "svcclass",
		"lookup"	=> "id",
		"fields"	=> array(
			"nbdrives", "name", "defaultfilesize:size", "maxperlicanb", "replicationpolicy", "gcpolicy", "migratorpolicy", "recallerpolicy"
		),
	),

	"stream2tapecopy"	=> array(
		"table"		=> "stream2tapecopy",
		"lookup"	=> "child",
		"fields"	=> array(
			"parent", "child"
		),	
	),
);


?>
