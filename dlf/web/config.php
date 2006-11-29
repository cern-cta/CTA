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

/*
 * Version of the DLF web interface
 */
$version = "1.0.2-1";

/*
 * Use the javascript/css calendar popup for date selection?
 */
$use_calendar_popup = 1;

/*
 * Allow hosts to be manually entered by the user?
 */
$use_strict_hosts = 1;

/*
 * Use the database view to expand information on ns file ids
 */
$use_database_view = 1;

/*
 * Display the version of the database on results pages?
 */
$show_db_version = 1;

/*
 * Display the number of partition online in the database?
 */
$show_partition_count  = 1;


/*
 * This setting defines the time frame the results should encompass when drilling down through data.
 * For example, when selecting a Request ID link the results page will group together all messages 
 * matching that request id +/- X hours from the selected message.
 *
 * Warning: setting this value too high can cause very large table scans to be performed impacting
 *		   database performance.
 */
$db_drilldown_time = 48;


/*
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
