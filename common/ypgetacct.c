/*
 * Copyright (C) 1990,1991 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)ypgetacct.c	1.4 06/01/92 CERN CN-SW/DC Antoine Trannoy";
#endif /* not lint */

/* ypgetacct()              Getting account id in YP      */

#if defined(NIS)
#include <stdio.h>
#if !defined(apollo)
#include <unistd.h>
#endif
#include <string.h>
#include <sys/types.h>
#include <pwd.h>
#include <rpcsvc/ypclnt.h>

static int ypcall(instatus,inkey,inkeylen,inval,invallen,indata)
	int instatus ;
	char * inkey ;
	int inkeylen ;
	char * inval ;
	int invallen ;
	char *indata ;
{
	static char   name[10] ;
	static char account[9] ;
	static int   first = 1 ; 
	char 		  * cp ; 

	/*
	 * There is an error.
	 */
	if ( instatus != 1 ) {
		*indata= '\0' ;
		return 1 ;
	}

	/*
	 * It is the first time.
	 */
	if ( first ) {
		(void) strcpy(name,strtok(indata,":")) ;
		(void) strcpy(account,strtok((char *)NULL,":")) ;
		first= 0 ; 
	}
	
	cp= strtok(inval,":") ;
	if ( strcmp(cp,name) ) {
		/*
		 * Username does not match.
		 */
		return 0 ;
	}
	else	{

		cp= strtok((char *)NULL,":") ; 
		if ( *account == '*' ) {
			(void) strcpy(indata,cp) ;
			return 1 ;
		} 
		else 	{
			if ( strcmp(cp,account) )
				return 0 ;
			(void) strcpy(indata,cp) ; 
			return 1 ;
		}
	}	
}

char * ypgetacct(pwd,account,buffer)
	struct passwd * pwd ; 
	char      * account ;
	char 	  *  buffer ;
{
	struct ypall_callback call ;
	char              * domain ; 
	
	/*
	 * Consulting YP.
	 */
	if ( yp_get_default_domain(&domain) ) 
		return NULL ;

	(void) sprintf(buffer,"%s:%s:",pwd->pw_name,(account==NULL)?"*":account) ;
	call.foreach= ypcall ;
	call.data= buffer ;
	if ( yp_all(domain,"account",&call) ) 
		return NULL ;
	return ( * (char *) call.data == '\0' ) ? NULL : buffer ;
}
#endif /* NIS */
