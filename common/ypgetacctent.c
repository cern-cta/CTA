/*
 * $Id: ypgetacctent.c,v 1.4 1999/12/09 13:39:51 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char cvsId[] = "@(#)$RCSfile: ypgetacctent.c,v $ $Revision: 1.4 $ $Date: 1999/12/09 13:39:51 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*  ypgetacctent() - Get account id in YP   */

#if defined(NIS)
#include <stdio.h>

#if !defined(apollo)
#include <unistd.h>
#endif

#include <string.h>
#include <sys/types.h>
#include <pwd.h>
#include <rpcsvc/ypclnt.h>

#include "ypgetacctent.h"

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */

typedef struct Cyp_spec {
        char _name[NAME_LEN];
        char _account[ACCT_LEN];
        int _firsttime;
} Cyp_spec_t;
static int Cypent_key = -1;
#define name Cyp->_name
#define account Cyp->_account
#define firsttime Cyp->_firsttime

static int ypcall(instatus, inkey, inkeylen, inval, invallen, indata)
    int instatus;
    char    *inkey;
    int inkeylen;
    char    *inval;
    int invallen;
    char    *indata;
{

    char      *cp = NULL; 
    char      buf[BUFSIZ];
    Cyp_spec_t *Cyp;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
    char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */

    /*
     * There is an error
     */

    if (instatus != 1) { 
        *indata = ENDSTR_CHR;
        return(1);
    }

    Cglobals_get(&Cypent_key,&Cyp,sizeof(Cyp_spec_t));

    /*
     * It is the first time
     */

    if (firsttime == 0) {
        (void)strcpy(name, strtok(indata, COLON_STR));
        (void)strcpy(account, strtok((char *)NULL, COLON_STR));
        firsttime = 1; 
    }
    
    (void)strncpy(buf, inval, (size_t)invallen);

    buf[invallen] = '\0';     /* see man strncpy */

    cp = strtok(inval, COLON_STR);

    if (strcmp(cp, name) != 0)
      /*
       * Username does not match.
       */
      return(0);
    else {
        cp = strtok((char *)NULL, COLON_STR);
        if (*account == STAR_CHR) {
            (void)strcpy(indata, buf);
            return(1);
        } else { 
            if (strcmp(cp, account) != 0) return(0);
            (void)strcpy(indata, buf); 
            return(1);
        }
    }
}

#undef name
#undef account
#undef first

char    *ypgetacctent(pwd, account, buffer, bufferlen)
    struct passwd   *pwd; 
    char        *account;
    char        *buffer;
    int     bufferlen;
{
    struct ypall_callback call;
    char          *domain = NULL; 
    char          buf[BUFSIZ];
    char          *outval = NULL;
    int           outvallen = 0;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
    char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */


    /*
     * Consulting YP.
     */

    if (yp_get_default_domain(&domain) != 0) return(NULL);

    if (account == NULL) {
        (void)sprintf(buf, "%s:%s", pwd->pw_name, DFLT_SEQ_STR);
        if (yp_match(domain, ACCT_MAP_NAME, buf, (int)strlen(buf), &outval, &outvallen) == 0) {
            (void)strncpy(buffer, outval, (size_t)bufferlen);
            return(buffer);
        }
    } else {
        int   seq = 0;

        for (seq = 0; seq < MAX_SEQ_NUM; seq++) {
            (void)sprintf(buf, "%s:%d", pwd->pw_name, seq);
            if (yp_match(domain, ACCT_MAP_NAME, buf, (int)strlen(buf), &outval, &outvallen) == 0) {
                char  fub[BUFSIZ], *acctfld;       
                acctfld = NULL;

                (void)strcpy(fub, buf);
                acctfld = strtok(fub, COLON_STR);
                acctfld = strtok((char *)NULL, COLON_STR);

                if (strcmp(account, acctfld) == 0) {
                    (void)strncpy(buffer, outval, (size_t)bufferlen);
                    return(buffer);
                }
            }
        }

        (void)sprintf(buf, "%s:%s", pwd->pw_name, account);

        call.foreach  = ypcall;
        call.data     = buf;

        if (yp_all(domain, ACCT_MAP_NAME, &call) != 0) return(NULL);

        if (*(char *)(call.data) == ENDSTR_CHR) return(NULL);
        else {
            (void)strncpy(buffer, call.data, (size_t)bufferlen);
            return(buffer);
        }
    }
    return(NULL);
}

#endif /* NIS */
