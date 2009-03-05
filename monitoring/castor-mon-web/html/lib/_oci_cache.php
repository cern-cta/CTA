<?php 
//activate image caching
 define("USE_CACHE",true);
 define("READ_CACHE",true);
 define("CACHE_DIR","../../tmp/jpgraph_cache/");
//checks if included dynamic library needed for the graphs
if (strtoupper(substr(PHP_OS, 0, 3) == 'WIN')) {
    if (!function_exists('imagepng')) {
        dl('php_gd2.dll');
    }
} 
//checks if included dynamic library needed for the Oracle db
if (!function_exists('ociplogon')) {
	if (strtoupper(substr(PHP_OS, 0, 3) == 'WIN')) {
		dl('php_oci8.dll');
	}
}
?>