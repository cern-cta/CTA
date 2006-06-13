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

/* definitions */
if (!defined("DB_LAYER")) {
	define("DB_LAYER", "mysql");
}


/** 
 * Open connection
 */
function db_connect($instance, $persistency) {
	
	include("config.php");

	/* variables */
	$server   = "";
	$username = "";
	$password = "";
	$database = "";

	/* lookup the instance name for connection details */
	if (!$db_instances[$instance]) {
		trigger_error("Faled to resolve instance name '$instance' to castor instance", E_USER_ERROR);
		exit;
	} else { 
		$server   = $db_instances[$instance]['server'];
		$user	  = $db_instances[$instance]['username'];
		$pass	  = $db_instances[$instance]['password'];
		$database = $db_instances[$instance]['database'];
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

/**
 * Query
 */
function db_query($query, $conn) {

	/* execute query */
	$results = mysql_query($query, $conn);
	if (!$results) {
		trigger_error("mysql_query() - ".mysql_error($conn), E_USER_ERROR);
		exit;
	}

	return $results;
}

/**
 *
 */
function db_fetch_row($results) {
	return mysql_fetch_row($results);
}

/**
 * Row count
 */
function db_row_count($results) {
	return mysql_num_rows($results);
}

/**
 * Server version
 */
function db_server_version($conn) {
	return "- version: ".mysql_get_server_info($conn);
}


?>
