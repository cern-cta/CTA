<?php

/******************************************************************************************************
 *                                                                                                    *
 * mysql.php                                                                                          *
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
 * $Id: mysql.php,v 1.3 2006/11/29 13:39:15 waldron Exp $
 */

/* definitions */
if (!defined("DB_LAYER")) {
	define("DB_LAYER", "mysql");
}


/*
 * Open connection
 */
function db_connect($instance, $persistency, $stager) {
	
	include("config.php");
	include("login.php");

	/* variables */
	$server   = "";
	$username = "";
	$password = "";
	$database = "";

	/* lookup the instance name for connection details */
	if (!$db_instances[$instance]) {
		trigger_error("Faled to resolve instance name '$instance' to castor instance", E_USER_ERROR);
		exit;
	} else if ($stager == 0) { 
		$server   = $db_instances[$instance]['server'];
		$user	  = $db_instances[$instance]['username'];
		$pass	  = $db_instances[$instance]['password'];
		$database = $db_instances[$instance]['database'];
	} else {
	
		/* stagerdb exists for this instance of dlf ? */
		if (!$db_instances[$instance]['stagerdb']) {
			trigger_error("Failed to resolves instance name '$instance' to castor stager instance", E_USER_ERROR);
			exit;
		}
		
		$server   = $db_instances[$instance]['stagerdb']['server'];
		$user	  = $db_instances[$instance]['stagerdb']['username'];
		$pass	  = $db_instances[$instance]['stagerdb']['password'];		
		$database = $db_instances[$instance]['stagerdb']['database'];
	}
	
	/* open connection to a mysql database */
	if ($persistency == true) {
		$conn = mysql_pconnect($server, $user, $pass);
	} else {
		$conn = mysql_connect($server, $user, $pass);
	}
	
	/* connection successful ? */
	if (!$conn) {
		trigger_error("mysql_connect() - ".mysql_error(), E_USER_ERROR);
		exit;
	}
	
	/* switch to database */
	if (!mysql_select_db($database, $conn)) {
		trigger_error("mysql_select_db() - ".mysql_error(), E_USER_ERROR);
		exit;
	}
	
	return $conn;
}

/*
 * Query
 */
function db_query($query, $conn) {

	/* increment the number of queries executed */
	global $query_count;
	$query_count++;

	/* execute query */
	$results = mysql_query($query, $conn);
	if (!$results) {
		trigger_error("mysql_query() - ".mysql_error($conn), E_USER_ERROR);
		exit;
	}

	return $results;
}

/*
 * Fetch row
 */
function db_fetch_row($results) {
	return mysql_fetch_row($results);
}

/*
 * Server version
 */
function db_server_version($conn) {
	return "- version: ".mysql_get_server_info($conn);
}

/*
 * Extract a value by name
 */
function db_result($results, $field) {
	
}

/*
 * Partition count
 */
function db_partition_count($conn) {
	return;
}

/*
 * Database schema version
 */
function db_schema_version($conn) {
	$stmt = db_query("SHOW TABLES LIKE 'dlf_tape_ids'", $conn);
	$row  = db_fetch_row($stmt);
	
	return $row[0] ? 1 : 2;	
}

?>
