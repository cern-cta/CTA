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

require("utils.php");
include("config.php");

$page_start = getmicrotime();

?>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">

<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
<head>
	<meta name="author" content="Dennis Waldron" />
	<meta name="description" content="Distributed Logging Facility" />
	<meta http-equiv="Content-Type" content="text/html; charset=us-ascii" />
	<meta http-equiv="Pragma" content="no-cache" />
	<meta http-equiv="Cache-Control" content="no-cache" />
	<meta http-equiv="Default-Style" content="compact" />
	<meta http-equiv="Content-Style-Type" content="text/css" />
	<link rel="stylesheet" type="text/css" title="compact" href="site.css" />

	<title>Distributed Logging Facility - Home</title>
</head>

<body>

	<!-- header -->
	<table class="noborder" cellspacing="0" cellpadding="0" width="100%">
		<tr class="header">
			<td>&nbsp;CASTOR Distributed Logging Facility</td>
		</tr>
	</table>

	<!-- break 1 -->
	<hr align="left" size="1" />
	
	<!-- main workspace -->
	<table class="workspace, noborder, backdrop" cellspacing="0" cellpadding="0" width="100%">
		<tr>
			<td>

				<!-- start form -->
				<form action="query.php" method="post" name="instance" id="instance">
				 
					<!-- central selection box -->
					<table class="border" align="center" cellspacing="0" cellpadding="4">
						<tr class="banner">
							<td>Select Instance:</td>
						</tr>

						<tr>
							<td valign="middle">&nbsp;&nbsp;Use Database:&nbsp;&nbsp; <select name="instance">
							<?php

							foreach (array_keys($db_instances) as $instance) {
								echo "<option value=\"".$instance."\"";
								echo ">".$db_instances[$instance]['type']." (".$db_instances[$instance]['username']."@".$db_instances[$instance]['server'].")</option>";
							}
	
							?>
							</select> &nbsp;&nbsp; <input type="submit" name="Submit" value="Connect" /> &nbsp;&nbsp;</td>
						</tr>
					</table>
					<!-- end central selection box -->

				</form>
				<!-- end form -->
	   
			</td>
		</tr>
	</table>
	
	<!-- connectivity information -->
	<table class="noborder" cellspacing="0" cellpadding="0" width="100%">
		<tr>
			<td align="left">&nbsp;</td>
		</tr>
	</table>

	<!-- break 2 -->
	<hr align="left" size="1" />
	
	<!-- footer -->
	<table class="noborder" cellspacing="0" cellpadding="0" width="100%">
		<tr valign="top">
			<?php $page_end = getmicrotime(); ?>
			<td align="left" width="33%">DLF interface version: <?php echo $version ?></td>
			<td align="center" width="33%"><?php printf("Page generated in %.3f seconds.", $page_end - $page_start); ?></td>
			<td align="right" width="33%">
			<a href="http://validator.w3.org/check?uri=referer"><img src="images/xhtml.png" alt="Valid XHTML 1.0 Strict" /></a>&nbsp; 
			<a href="http://jigsaw.w3.org/css-validator/check/referer"><img src="images/css.png" alt="Valid CSS 2.0" /></a>&nbsp; 
			<a href="http://www.php.net/"><img src="images/php.png" alt="Powered by PHP" /></a>&nbsp;
			</td>
		</tr>
	</table>
</body>
</html>