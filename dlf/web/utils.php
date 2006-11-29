<?php

/******************************************************************************************************
 *                                                                                                    *
 * utils.php                                                                                          *
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
 * $Id: utils.php,v 1.3 2006/11/29 13:39:15 waldron Exp $
 */

include("config.php");

/*
 * get micro time
 */
function getmicrotime() {  
	$temparray  = split(" ", microtime());  
	$returntime = $temparray[0] + $temparray[1];  
	return $returntime;  
}

/*
 * page navigation (adapted from phpBB)
 */
function generate_pagination($num_items, $per_page, $start_item) {

	$total_pages = ceil($num_items / $per_page);
	if ( $total_pages == 1 ) {
		return '';
	}

	$base_url = $_SERVER['PHP_SELF']."?".str_replace("&", "&amp;", $_SERVER['QUERY_STRING']);
	$base_url = preg_replace('/&page=(\d)/', '', $base_url);
	$on_page  = $start_item;

	$page_string = '';
	if ( $total_pages > 10 ) {
		$init_page_max = ( $total_pages > 3 ) ? 3 : $total_pages;

		for($i = 1; $i < $init_page_max + 1; $i++) {
	
			$page_string .= ( $i == $on_page ) ? '<b>'.$i.'</b>' : '<a href="'.$base_url. "&amp;page=".$i .'">'.$i.'</a>';
			if ( $i <  $init_page_max ) {
				$page_string .= ", ";
			}
		}

		if ( $total_pages > 3 ) {
			if ( $on_page > 1  && $on_page < $total_pages ) {
				$page_string .= ( $on_page > 5 ) ? ' ... ' : ', ';

				$init_page_min = ( $on_page > 4 ) ? $on_page : 5;
				$init_page_max = ( $on_page < $total_pages - 4 ) ? $on_page : $total_pages - 4;

				for($i = $init_page_min - 1; $i < $init_page_max + 2; $i++) {
					$page_string .= ($i == $on_page) ? '<b>'.$i.'</b>' : '<a href="'.$base_url."&amp;page=".$i.'">'.$i.'</a>';
					if ( $i <  $init_page_max + 1 ) {
						$page_string .= ', ';
					}
				}

				$page_string .= ( $on_page < $total_pages - 4 ) ? ' ... ' : ', ';
			}
			else {
				$page_string .= ' ... ';
			}

			for($i = $total_pages - 2; $i < $total_pages + 1; $i++) {
				$page_string .= ( $i == $on_page ) ? '<b>'.$i.'</b>' : '<a href="'.$base_url."&amp;page=".$i.'">'.$i.'</a>';
				if( $i <  $total_pages ) {
					$page_string .= ", ";
				}
			}
		}
	}
	else {
		for($i = 1; $i < $total_pages + 1; $i++) {
			$page_string .= ( $i == $on_page ) ? '<b>'.$i.'</b>' : '<a href="'.$base_url."&amp;page=".$i.'">'.$i.'</a>';
			if ( $i <  $total_pages ) {
				$page_string .= ', ';
			}
		}
	}

	if ( $on_page > 1 ) {
		$page_string = ' <a href="' . $base_url . "&amp;page=" . ($on_page - 1)  . '">Previous</a>&nbsp;&nbsp;' . $page_string;
	}

	if ( $on_page < $total_pages ) {
		$page_string .= '&nbsp;&nbsp;<a href="' . $base_url . "&amp;page=" . ($on_page + 1) . '">Next</a>';
	}
	
	if ($page_string) {
		$page_string = 'Goto page ' . $page_string;
	}
	return $page_string;
}


/**********************************************************************************************/

?>