/*
 * Copyright (C) 1990-1998 by CERN/IT/PDP/IP
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)ws_errmsg.c	1.1 12/11/98 CERN IT-PDP/IP Aneta Baran";
#endif /* not lint */

#if defined(_WIN32)
#include "ws_errmsg.h"


/*
 *  ws_strerr -  equivalent of strerror() for WinSock errors
 */ 

char *ws_strerr(err_no)
int err_no;
{
   if( err_no > WSAEREFUSED ) {
      switch(err_no) {
       case 11001: return "WSAHOST_NOT_FOUND (Authoritative Answer: Host not found)";
       case 11002: return "WSATRY_AGAIN (Non-Authoritative: Host not found, or SERVERFAIL)";
       case 11003: return "WSANO_RECOVERY (Non-recoverable errors, FORMERR, REFUSED, NOTIMP)";
       case 11004: return "WSANO_DATA, WSANO_ADDRESS(Valid name, no data record of requested type or no address, look for MX record";
       default: {
	  return "Unknown WSA error code.";
       }
      }
   }  else
      if( strcmp("", ws_errmsg[err_no-WSABASEERR]) )  {
	 return ws_errmsg[err_no-WSABASEERR]; 
      }  else  {
	  return "Unknown WSA error code.";
      }
}


/*
 *	geterr() - returns description of the last WinSock error (for logging purpose)
 */

char *geterr() {
   return( ws_strerr( WSAGetLastError() ) );
}


/*
 *	ws_perror - equivalent of perror() for WinSock errors
 */

int ws_perror ( char *s ) {
   int error;

   error =  WSAGetLastError(); 
   fprintf( stderr, "%s: %s\n", s, ws_strerr( error) );
   WSASetLastError( 0 );
   return error;
}

#endif
