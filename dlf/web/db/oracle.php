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

/**
 * $Id: oracle.php,v 1.3 2006/09/25 11:43:53 waldron Exp $
 */

/* definitions */
if (!defined("DB_LAYER")) {
	define("DB_LAYER", "oracle");
}


/**
 * Open connection
 */
function db_connect($instance, $persistency, $stager) {
	
	include("config.php");

	/* variables */
	$server   = "";
	$username = "";
	$password = "";

	/* lookup the instance name for connection details */
	if (!$db_instances[$instance]) {
		trigger_error("Faled to resolve instance name '$instance' to castor instance", E_USER_ERROR);
		exit;
	} else if ($stager == 0) { 
		$server = $db_instances[$instance]['server'];
		$user	= $db_instances[$instance]['username'];
		$pass	= $db_instances[$instance]['password'];
	} else {
	
		/* stagerdb exists for this instance of dlf ? */
		if (!$db_instances[$instance]['stagerdb']) {
			trigger_error("Failed to resolves instance name '$instance' to castor stager instance", E_USER_ERROR);
			exit;
		}
		
		$server = $db_instances[$instance]['stagerdb']['server'];
		$user	= $db_instances[$instance]['stagerdb']['username'];
		$pass	= $db_instances[$instance]['stagerdb']['password'];		
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


/*
 *
 */
function db_result($results, $field) {
	return $value = ociresult($results, strtoupper($field));
}


/**
 * Partition count
 */
function db_partition_count($conn) {
	
	/* database supports partitioning ? */
	$version = db_server_version($conn);
	if (!stristr($version, "Enterprise")) {
		return;
	}

	/* increment the number of queries executed */
	global $query_count;
	$query_count++;

	/* execute query */
	$stmt = ociparse($conn, "SELECT COUNT(*) FROM USER_TAB_PARTITIONS WHERE PARTITION_NAME != 'MAX_VALUE' AND TABLE_NAME = 'DLF_MESSAGES' AND PARTITION_NAME <= CONCAT('P_', TO_CHAR(SYSDATE, 'YYYYMMDD'))");
	if (!$stmt) {
		trigger_error("ociparse() - ".ocierror($conn), E_USER_ERROR);
		exit;
	}
	$rtn = ociexecute($stmt, OCI_DEFAULT);
	if (!$rtn) {
		trigger_error("ociexecute() - ".ocierror($stmt), E_USER_ERROR);
		exit;
	}
	$row = db_fetch_row($stmt);

	/* no partitions online ? 
	 *   - this is mostly likely caused by a unpartitioned database!
	 */
	if ($row[0] > 0) {
		if ($row[30] < $max_partitions_online) {
			return "Partitions online: 30";
		} else {
			return "Partitions online: ".$row[0];
		}
	} else {
		return;
	}
}

?>
