<?php

/******************************************************************************************************
 *                                                                                                    *
 * oracle.php                                                                                         *
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
	define("DB_LAYER", "oracle");
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
	}
	
	/* open connection to a mysql database */
	if ($persistency == true) {
		$conn = ocilogon($user, $pass, $server);
	} else {
		$conn = ociplogon($user, $pass, $server);
	}
	
	/* connection successful ? */
	if (!$conn) {
		trigger_error("ocilogin() - ".ocierror(), E_USER_ERROR);
		exit;
	}
	
	/* set the time format */
	$stmt = ociparse($conn, "ALTER SESSION SET NLS_DATE_FORMAT = 'dd-mm-yyyy hh24:mi:ss'");
	if (!$stmt) {
		trigger_error("ociparse() - ".ocierror(), E_USER_ERROR);
		exit;
	}
	$rtn = ociexecute($stmt, OCI_DEFULT);
	if (!$rtn) {
		trigger_error("ociexecute() - ".ocierror(), E_USER_ERROR);
		exit;
	}

	return $conn;
}

/**
 * Query
 */
function db_query($query, $conn) {

	/* execute query */
	$stmt = ociparse($conn, $query);
	if (!$stmt) {
		trigger_error("ociparse() - ".ocierror($conn), E_USER_ERROR);
		exit;
	}
	$rtn = ociexecute($stmt, OCI_DEFAULT);
	if (!$rtn) {
		trigger_error("ociexecute() - ".ocierror($stmt), E_USER_ERROR);
		exit;
	}
	return $stmt;
}

/**
 * Fetch row
 */
function db_fetch_row($results) {

	/* convert the result in to an array */
	$rtn = ocifetch($results);
	if($rtn) {
		for ($i = 1, $j = 0; $i <= ocinumcols($results); $i++, $j++) {
			$row[$j] = ociresult($results, ocicolumnname($results, $i));
		}
	}
	return $row;
}

/**
 * Server version
 */
function db_server_version($conn) {
	$db_version = split("-", ociserverversion($conn));
	return trim("- version: ". $db_version[0]);;
}


?>
