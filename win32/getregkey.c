/*
 * Copyright (C) 1990-1999 by CERN IT-PDP/IP
 * All rights reserved
 */

#ifndef lint
static char sccsid[] =  "@(#)$RCSfile: getregkey.c,v $ $Revision: 1.1 $ $Date: 2000/03/10 17:01:20 $ CERN IT-PDP/IP Aneta Baran";
#endif /* not lint */

/* getregkey.c - get registry key value (equivalent of getconfent())              */
#if defined(_WIN32)
#include <windows.h>
#include "syslog.h"
#include "log.h"

#undef DEBUG

/* It is assumed that SHIFT configuration resides at HKEY_LOCAL_MACHINE\Software\Shift.
   The subkeys have names identical to the categories in shift.conf. Parameters category
   and name are the same as those used in getconfent() call. */ 

#define REG_KEY_NAME "Software\\Shift"

__declspec(thread) static char value[80];

char *getregkey(category, name)
char *category;
char *name;
{
   int	rcode;
   HKEY	key_handle;
   char value_name[80];
   int	name_size, value_size;
   int 	n;
   char key_name[80];
   int 	found = 0;

   openlog("rfiod", LOG_DEBUG, LOG_DAEMON);

   sprintf(key_name, "%s\\%s", REG_KEY_NAME, category);
#ifdef DEBUG 
   log(LOG_DEBUG, "getregkey(%s, %s)\n", category, name );
   log(LOG_DEBUG, "getregkey: opening key %s\n", key_name);
#endif /* DEBUG */ 

/* open the key to enumerate */
   rcode =  RegOpenKeyEx(HKEY_LOCAL_MACHINE, key_name, 0, KEY_QUERY_VALUE, &key_handle);
   if( rcode != ERROR_SUCCESS ) {
      log(LOG_ERR, "Unable to open registry key %s", key_name);
      return NULL;
   }
#ifdef DEBUG 
   log(LOG_DEBUG, "Searching for value name %s\n", name);
#endif
   n = 0;
   do {
      name_size = value_size = 80;
      rcode = RegEnumValue(key_handle, n, value_name, &name_size, NULL, NULL, value, &value_size);
      if( rcode == ERROR_SUCCESS )  {
	 if( strcmp(name, value_name) == 0 ) {
#ifdef DEBUG 
	    log(LOG_DEBUG, "Value %s = %s found in registry", value_name, value);
#endif
	    found = 1;
	 }
	 n++; 
      }
   }  while( rcode == ERROR_SUCCESS && !found );

   rcode = RegCloseKey(key_handle);
   if( rcode != ERROR_SUCCESS )  {
      log(LOG_ERR, "Unable to close registry key %s", key_name );
   }
   if( found ) return value;
   else return NULL;
}
#endif










