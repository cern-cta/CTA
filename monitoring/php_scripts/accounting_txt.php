<?php 

//checks if included dynamic library needed for the Oracle db
if (!function_exists('ociplogon')) {
	if (strtoupper(substr(PHP_OS, 0, 3) == 'WIN')) {
		dl('php_oci8.dll');
	}
}
//see user.php.example
include ("user.php");
error_reporting(E_ALL ^ E_NOTICE);  
//connection
//please initialize $sercvice (e.g. castor_atlas_stager (you need a correnct entry in user.php))
//either by posting a value 
//$service = $_GET['service'];
//or by hardcoding one
//$service = "castor_atlas_stager"
$conn = ocilogon($db_instances[$service]['username'],$db_instances[$service]['pass'],$db_instances[$service]['serv']);
if(!$conn) {
	$e = oci_error();
	print htmlentities($e['message']);
	exit;
}
$query1 = "select name,to_char(euid) usern,sum(nbbytes) bsize
           from (
             select a.euid,a.nbbytes,c.child svcclass
             from castor_stager.accounting a, castor_stager.filesystem b, castor_stager.diskpool2svcclass c 
             where a.filesystem = b.id
               and b.diskpool = c.parent) temp,  castor_stager.svcclass
           where temp.svcclass =  castor_stager.svcclass.id
           group by euid,name
           UNION
           select name , 'free_space' usern,free bsize
           from (
           select child svcclass,diskpool,sum(free) free
           from  castor_stager.filesystem a, castor_stager.diskpool2svcclass b
           where a.diskpool = b.parent
           group by child,diskpool ) c ,  castor_stager.svcclass
           where c.svcclass =  castor_stager.svcclass.id
           order by name";

	   
if (!($parsed1 = OCIParse($conn, $query1))) 
	{ echo "Error Parsing Query";exit();}
if (!OCIExecute($parsed1))
	{ echo "Error Executing Query";exit();}
$svcclass = "foo";
$j=0;
echo "<svcclass>\n<username> <volume in MBs>\n";
while (OCIFetch($parsed1)) {
	$svcclass_new = OCIResult($parsed1,1);
	if ($svcclass_new != $svcclass) {
		echo "\n$svcclass_new \n";
		$svcclass = $svcclass_new;
	}
	$userid = OCIResult($parsed1,2);
	$size = OCIResult($parsed1,3);
        $uname = $userid;
        if ($userid != 'free_space') {
	        $username = posix_getpwuid((int)$userid);
		if(!empty($username))
	  		$uname = $username['name'];
        }
	echo "$uname ";
	$size = round(($size / (1024*1024)),3);
	echo "$size";
	echo "\n";
};
?> 


	
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	   
	
