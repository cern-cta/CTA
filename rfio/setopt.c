/*
 * $Id: setopt.c,v 1.2 1999/07/20 12:48:27 jdurand Exp $
 *
 * $Log: setopt.c,v $
 * Revision 1.2  1999/07/20 12:48:27  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Timeouted version of RFIO. Using netread_timeout() and netwrite_timeout
 *   on all control and data sockets.
 *
 */

/*
 * Copyright (C) 1990,1991 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)setopt.c	1.3 9/1/95  CERN CN-SW/DC  F. Hassine";
#endif /* not lint */
#define RFIO_KERNEL 1
#include "rfio.h"               /* remote file I/O definitions          */

/*
 * By default RFIO are buffered. 
 */
static int rfio_opt= RFIO_READBUF ;
/*
 * By default rfio_connect()  reads NET entries 
 */
static int rfio_net= RFIO_NET ;
static int rfio_connretry = RFIO_RETRYIT ;
/*
 * Force local I/O access. Required in some cases.
 */ 
static int rfio_forcelocal = RFIO_NOLOCAL ;

/*
 * User can set option through this function.
 */
int	rfiosetopt(opt,pval,len) 
	int	opt ; 
	int  * pval ;		
	int	len ;
{
	switch(opt) {
		case RFIO_READOPT:
			rfio_opt= *pval ;
			return 0 ; 
		case RFIO_NETOPT:
			rfio_net= *pval ;
			return 0 ;
		case RFIO_NETRETRYOPT:
			rfio_connretry= *pval ;
			return 0 ;
		case RFIO_CONNECTOPT:
			rfio_forcelocal= *pval ;
			return 0 ;
		default:
			errno= EINVAL ;
			return -1 ;
	}
}
/*
 * User can read an option through this function
 */

int	rfioreadopt(opt)
int 	opt ;
{
	switch(opt) {
		case RFIO_READOPT:
			return ( rfio_opt ) ;
		case RFIO_NETOPT:
			return ( rfio_net ) ;
		case RFIO_NETRETRYOPT:
			return ( rfio_connretry ) ;
		case RFIO_CONNECTOPT:
			return ( rfio_forcelocal ) ;
		default:
			errno= EINVAL ;
			return -1 ;
	}
}
