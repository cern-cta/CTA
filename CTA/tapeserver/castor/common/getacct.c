
/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

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
#include <unistd.h>
/*
 * _WIN32 strtok() is already MT safe where as others wait
 * for next POXIS release
 */
#define strtok(X,Y) strtok_r(X,Y,&last)
#include <stdlib.h>

#include <Cglobals.h>
#include <Cpwd.h>
#include "getacct.h"

EXTERN_C char *getacctent();


char *getacct_r(char *resbuf)
{ 
    char      *account = NULL;    /* Pointer to the account env variable  */
    struct passwd *pwd = NULL;        /* Pointer to the password entry    */
    char      buf[BUFSIZ];
    char      *cprv, *rv;
    char      *last = NULL;

    cprv = rv = NULL;

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

    if ((pwd = Cgetpwuid(getuid())) == NULL) return(NULL);

    /*
     * Get account file entry
     */

    cprv = getacctent(pwd, account, buf, (int)sizeof(buf));

    /*
     * Extract account id
     */

    if (cprv != NULL) {
        if (strtok(cprv, COLON_STR) != NULL) {
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
    void *resbuf = NULL;

    Cglobals_get(&getacct_key, &resbuf,BUFSIZ+1);
    if ( resbuf == NULL ) return(NULL);

    return(getacct_r(resbuf));
}


/*
 * Fortran wrapper
 */

/*FH*   to be done      RC = XYACCT()           */
