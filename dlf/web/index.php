<?php

/******************************************************************************************************
 *                                                                                                    *
 * index.php                                                                                          *
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
 * $Id: index.php,v 1.2 2006/09/06 12:53:43 waldron Exp $
 */

require("utils.php");
include("config.php");

$gen_start = getmicrotime();

?>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
	<meta name="author" content="Dennis Waldron" />
	<meta name="description" content="Distributed Logging Facility" />
	<meta http-equiv="Content-Type" content="text/html; charset=us-ascii" />
	<meta http-equiv="Pragma" content="no-cache" />
	<meta http-equiv="Cache-Control" content="no-cache" />
	<meta http-equiv="Default-Style" content="compact" />
	<meta http-equiv="Content-Style-Type" content="text/css" />
	<title>Distributed Logging Facility - Home</title>
	<link href="site.css" rel="stylesheet" type="text/css" />
</head>
<body>
	<table class="workspace" cellspacing="0" cellpadding="0">
	
	<!-- header -->
  	<tr class="header">
    	<td colspan="3">CASTOR Distributed Logging Facility</td>
  	</tr>
  	<tr>
    	<td colspan="3"><hr size="1"/></td>
  	</tr>
		
	<!-- content -->
  	<tr class="content">
    	<td colspan="3" align="center">
		
		<!-- selection box -->
		<form action="query.php" method="get" name="instance" id="instance">
		
		<table class="border" cellspacing="0" cellpadding="4">
			<tr class="banner">
				<td>Select Instance:</td>
			</tr>
			<tr>
				<td valign="middle">&nbsp;&nbsp;Use Database:&nbsp;&nbsp;
					<select name="instance">
					<?php
						sort(array_keys($db_instances));
						foreach (array_keys($db_instances) as $name) {
							echo "<option value=\"".$name."\"";
							echo ">".$db_instances[$name]['type']." (".$db_instances[$name]['username']."@".$db_instances[$name]['server'].")</option>";
						}					
					?>
					</select>&nbsp;&nbsp;
					<input type="submit" name="Submit" value="Connect" class="button"/>&nbsp;
				</td>
			</tr>
		</table>
		
		</form>
		
		</td>
  	</tr>
	
	<tr>
		<td colspan="3">&nbsp;</td>
	</tr>
	
	<!-- footer -->
  	<tr>
    	<td colspan="3"><hr size="1"/></td>
	</tr>
  	<tr class="footer">
    	<td width="33%" align="left"  >DLF interface version: <?php echo $version ?></td>
    	<td width="33%" align="center"><?php printf("Page generated in %.3f seconds.", getmicrotime() - $gen_start); ?></td>
    	<td width="33%" align="right" >
			<a href="http://validator.w3.org/check?uri=referer"><img src="images/xhtml.png" alt="Valid XHTML 1.0 Strict" /></a>&nbsp;
			<a href="http://jigsaw.w3.org/css-validator/check/referer"><img src="images/css.png" alt="Valid CSS 2.0" /></a>&nbsp;
			<a href="http://www.php.net/"><img src="images/php.png" alt="Powered by PHP" /></a>		
		</td>
  	</tr>
	</table>
</body>
</html>
