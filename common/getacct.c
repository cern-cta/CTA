/*
 * Copyright (C) 1990-1997 by CERN CN-PDP/CS
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)getacct.c	1.10 07/04/97 CERN CN-PDP/CS F. Hemmer";
#endif /* not lint */

/*	getacct() - Getting the current account id	*/

/*
 * If the environment variable ACCOUNT is set
 *      Check if it is a valid account id for the user
 * Else
 *      Get the primary account id of the user
 * Endif
 *
 * The look-up policy is the same as for passwd.
 */

#if !defined(vms) && !defined(CRAY)

#include <stdio.h>
#include <string.h>
#include <pwd.h>
#include <sys/types.h>
#if !defined(_WIN32)
#include <unistd.h>
#endif
#include <stdlib.h>

#include "getacct.h"

static char	resbuf[BUFSIZ];

extern char	*getacctent();


char	*getacct() 

	{ char		*account = NULL;  	/* Pointer to the account env variable	*/
	  struct passwd	*pwd = NULL;		/* Pointer to the password entry	*/
	  char		buf[BUFSIZ];
	  char		*cprv = NULL,
			*rv = NULL;

	  /*
	   * Get environment variable.
	   */

	  account = getenv(ACCOUNT_VAR);

	  if (account != NULL)
	    { if (strcmp(account, EMPTY_STR) == 0)
	        account = NULL;
	    }

	  /*
	   * Get password entry.
	   */

	  if ((pwd = getpwuid(getuid())) == NULL)
	    return(NULL);

	  /*
	   * Get account file entry
	   */

	  cprv = getacctent(pwd, account, buf, (int)sizeof(buf));

	  /*
	   * Extract account id
	   */

	  if (cprv != NULL)
	    { if ((rv = strtok(cprv, COLON_STR)) != NULL)
		if ((rv = strtok((char *)NULL, COLON_STR)) != NULL)
		  { strcpy(resbuf, rv);

		    return(resbuf);
		  }
	    }

	  return(NULL);
	}

#endif	/* !vms && !CRAY */

#if defined(vms)
#include <ssdef.h>      /* System services definitions                  */
#include <uaidef.h>     /* User authorization definitions               */
#include <rmsdef.h>     /* Record Management definitions                */
#include <stdio.h>      /* Standard Input/Output definitions            */
#include <errno.h>      /* Standard error numbers                       */


extern int	sys$getuai();	/* VMS get user account information 	*/

static  char    account[9+1];   /* account string buffer                */
static  int     accountl;       /* account string length                */

typedef struct {                /* VMS Item desciptor                   */
        short   bufl;
        short   code;
        char    *buf;
        int     *retl;
} Item;

typedef struct  {               /* VMS generic descriptor               */
        int     len;
        char    *buf;
} descrip;

static struct   {               /* VMS Item list for $GETUAI            */
        Item    items[57];
        int     eol;
} itemlist = {
        {
        9,      UAI$_ACCOUNT,           account,                &accountl
        },
        0
};


char * getacct() 
{
	descrip name_dsc;               /* username's descriptor        */
	char    name[L_cuserid];        /* username buffer              */
	char    *cp;                    /* character pointer            */
	int     rc;                     /* syscall return code          */

	cuserid(name);                  /* C env. must be initialized   */
        name_dsc.len = strlen(name);
        name_dsc.buf = name;
        if ((rc = sys$getuai(0, 0, &name_dsc, &itemlist, 0, 0, 0))
                        != SS$_NORMAL)       {
                if (rc == RMS$_RNF)     {
                        vaxc$errno = rc;
                        errno = EVMSERR;
			cp = NULL;
                }
                if (!(rc & 0x01))       {
                        vaxc$errno = rc;
                        errno = EVMSERR;
			cp = NULL;
                }
                else    {
                        account[accountl]='\0';
			cp = account;
                }
        }
        else    {
                account[accountl]='\0';
		cp = account;
        }
	return(cp);
}
#endif /* vms */

#if defined(CRAY)
#include <stdio.h>      /* Standard Input/Output definitions            */

char    *getacct()
{
	extern int	acctid();
	extern char	*acid2nam();
	int		aid;                    /* account id                   */
	char		*cp;                    /* character pointer            */

        aid = acctid(0, -1);

        if ((aid = acctid(0, -1)) == -1)	{
		cp = NULL;
        }
        else    {
		if ((cp = acid2nam(aid)) == NULL)     {
			cp = NULL;
                }
        }

	return(cp);
}
#endif /* CRAY */

/*
 * Fortran wrapper
 */

/*FH*   to be done      RC = XYACCT()           */
