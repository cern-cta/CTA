<?php 

//checks if included dynamic library needed for the Oracle db
if (!function_exists('ociplogon')) {
	if (strtoupper(substr(PHP_OS, 0, 3) == 'WIN')) {
		dl('php_oci8.dll');
	}
}

include ("user.php");
error_reporting(E_ALL ^ E_NOTICE);  
//connection
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['password'],$db_instances[$service]['server']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
$servuce = "stagerdb";
$query1 = "select name,to_char(euid) usern,sum(nbbytes) bsize
           from (
             select a.euid,a.nbbytes,c.child svcclass
             from ".$db_instances[$service]['schema']."accounting a, ".$db_instances[$service]['schema']."filesystem b, ".$db_instances[$service]['schema']."diskpool2svcclass c 
             where a.filesystem = b.id
               and b.diskpool = c.parent) temp,  ".$db_instances[$service]['schema']."svcclass
           where temp.svcclass =  ".$db_instances[$service]['schema']."svcclass.id
           group by euid,name
           UNION
           select name , 'free_space' usern,free bsize
           from (
           select child svcclass,diskpool,sum(free) free
           from  ".$db_instances[$service]['schema']."filesystem a, ".$db_instances[$service]['schema']."diskpool2svcclass b
           where a.diskpool = b.parent
           group by child,diskpool ) c ,  ".$db_instances[$service]['schema']."svcclass
           where c.svcclass =  ".$db_instances[$service]['schema']."svcclass.id
           order by name";

	   
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$svcclass = "foo";
$j=0;
echo "<svcclass><username><userid><volume in MBs>\n";
while (OCIFetch($parsed1)) {
	$svcclass = OCIResult($parsed1,1);
	$userid = OCIResult($parsed1,2);
	$size = OCIResult($parsed1,3);
        $uname = $userid;
        if ($userid != 'free_space') {
	        $username = posix_getpwuid((int)$userid);
		if(!empty($username))
	  		$uname = $username['name'];
        }
        echo "$svcclass ";
	echo "$uname ";
        echo "$userid ";
	$size = round(($size / (1024*1024)),3);
	echo "$size";
	echo "\n";
};
?> 


	
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	
