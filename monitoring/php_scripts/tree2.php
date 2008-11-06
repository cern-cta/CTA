/******************************************************************************
 *              tree2.php
 *
 * This file is part of the Castor Monitoring project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2008  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * Author Theodoros Rekatsinas, 
 *****************************************************************************/
<?php
include ("lib.php");
error_reporting(E_ALL ^ E_NOTICE);
include ("user.php");
$conn = $conn = ocilogon(username,pass,serv);
  if (!$conn) {
   $e = oci_error();
   print htmlentities($e['message']);
   exit;
  }
 
 $query1 = "SELECT name, LEVEL l,  CONNECT_BY_ISLEAF leaf,num_accesses,node_id
	FROM filesystem
	where CONNECT_BY_ISLEAF = 0
	START WITH parent_id is null
	CONNECT BY  prior  node_id = parent_id
	order siblings by name";
	
$parsed1 = OCIParse($conn, $query1);
 
 OCIExecute($parsed1);
 $i = 0;
 $plevel = 1;
 $pleaf = 1;
 $pnode_id = 1;
 echo "
<html>
  <head>
  <script src='mktree.js' language='JavaScript'></script>
  <link rel='stylesheet' href='mktree.css'>
  </head>
  <body>
  <A href=\"#\" onClick=\"expandTree('tree1'); return false;\">Expand All</A>&nbsp;&nbsp;&nbsp;
  <A href=\"#\" onClick=\"collapseTree('tree1'); return false;\">Collapse All</A>&nbsp;&nbsp;&nbsp;
  <p><b>  Folder    -     Number of Accesses </b></p>
  <ul class='mktree' id='tree1'>";
  echo "\n";
 while ( OCIFetch($parsed1) ) {
   $name = OCIResult($parsed1, 1);//name
   $level = OCIResult($parsed1, 2);//level
   $leaf = OCIResult($parsed1, 3);//leaf
   $accesses = OCIResult($parsed1, 4);//number of accesses
   $node_id = OCIResult($parsed1, 5);//node id
   if ($plevel < $level) {
		if  ($leaf == 1) 
			echo "<li>$name <b> $accesses</b></li>";
			
		else if  ($leaf == 0)
			echo "<li class='liClosed'><a href='pie_test.php?pid=$node_id' target=\"right\">$name</a> <b> $accesses</b><ul>";
	}
	echo "\n";
	if ($plevel > $level) {
		if($pleaf == 1){
			echo "<a href='contents.php?pid=$pnode_id' target='_blank'>contents</a>";
			for($j = 0;$j < $plevel - $level;$j++){
				echo "</ul></li>";
				echo "\n";
			}
		}
		else if($pleaf == 0) {
			echo "<a href='contents.php?pid=$pnode_id' target='_blank'>contents</a>";
			for($j = 0;$j <= $plevel - $level;$j++){
				echo "</ul></li>";
				echo "\n";
			}
		}
		if ($leaf ==1)
			echo "<li>$name <b> $accesses</b></li>";
		else if ($leaf== 0 )
			echo "<li class='liClosed'><a href='pie_test.php?pid=$node_id' target=\"right\">$name</a> <b> $accesses</b><ul>";
	}
	echo "\n";
	if ($plevel == $level) {
		if ($pleaf == 0)  {
			echo "<a href='contents.php?pid=$pnode_id ' target='_blank''>contents</a>";
			echo"</ul></li>";
		}
		if ($leaf ==1)
			echo "<li>$name <b> $accesses</b></li>";
		else if ($leaf == 0 )
			echo "<li class='liClosed'><a href='pie_test.php?pid=$node_id' target=\"right\">$name</a> <b> $accesses</b><ul>";
	}
	echo "\n";
	$plevel = $level;
	$pleaf = $leaf;   
	$pnode_id = $node_id;
 }
 if ($pleaf == 1)
	 while ($plevel > 1) {
	 	echo "</ul></li>";
		echo "\n";
		$plevel--;
	 }
 else if ($pleaf == 0)
 	 while ($plevel >= 1) {
	 	echo "</ul></li>";
		echo "\n";
		$plevel--;
	 } 
echo "</ul>
  </body>
</html>";
echo "\n";
?>
