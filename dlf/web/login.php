<?php

/*
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

?>