/*
 * $Id: getacct.c,v 1.3 1999/07/21 20:07:46 jdurand Exp $
 * $Log: getacct.c,v $
 * Revision 1.3  1999/07/21 20:07:46  jdurand
 * *** empty log message ***
 *
 * Revision 1.2  1999/07/21 16:25:41  obarring
 * Make MT safe
 *
 */

/*
 * Copyright (C) 1990-1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char cvsId[] = "$Id: getacct.c,v 1.3 1999/07/21 20:07:46 jdurand Exp $";
#endif /* not lint */

/*  getacct() - Getting the current account id  */

/*
 * If the environment variable ACCOUNT is set
 *      Check if it is a valid account id for the user
 * Else
 *      Get the primary account id of the user
 * Endif
 *
 * The look-up policy is the same as for passwd.
 */

#include <stdio.h>
#include <string.h>
#include <pwd.h>
#include <sys/types.h>
#if !defined(_WIN32)
#include <unistd.h>
/*
 * _WIN32 strtok() is already MT safe where as others wait
 * for next POXIS release
 */
#if defined(_REENTRANT)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif
#else
extern uid_t getuid();
#endif
#include <stdlib.h>

#include <Cglobals.h>
#include "getacct.h"

extern char *getacctent_r();


char    *getacct_r(resbuf,resbufsiz) 
char *resbuf;
size_t resbufsiz;
{ 
    char      *account = NULL;    /* Pointer to the account env variable  */
    struct passwd *pwd = NULL;        /* Pointer to the password entry    */
    char      buf[BUFSIZ];
    char      *cprv, *rv, *last;

    cprv = rv = last = NULL;

    /*
     * Get environment variable.
     */

    account = getenv(ACCOUNT_VAR);

    if (account != NULL) {
        if (strcmp(account, EMPTY_STR) == 0) account = NULL;
    }

    /*
     * Get password entry.
     */

    if ((pwd = getpwuid(getuid())) == NULL) return(NULL);

    /*
     * Get account file entry
     */

    cprv = getacctent_r(pwd, account, buf, (int)sizeof(buf));

    /*
     * Extract account id
     */

    if (cprv != NULL) {
        if ((rv = strtok(cprv, COLON_STR)) != NULL) {
            if ((rv = strtok((char *)NULL, COLON_STR)) != NULL) {
                strcpy(resbuf, rv);
                return(resbuf);
            }
        }
    }
    return(NULL);    
}

static int getacct_key = -1;

char *getacct()
{
    char *resbuf = NULL;

    Cglobals_get(&getacct_key,(void **) &resbuf,BUFSIZ+1);
    if ( resbuf == NULL ) return(NULL);

    return(getacct_r(resbuf,BUFSIZ+1));
}


/*
 * Fortran wrapper
 */

/*FH*   to be done      RC = XYACCT()           */
