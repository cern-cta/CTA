/*
 * $Id: getacctent.c,v 1.2 1999/07/21 16:26:06 obarring Exp $
 * $Log: getacctent.c,v $
 * Revision 1.2  1999/07/21 16:26:06  obarring
 * Make MT safe. Static buffers were not needed anyway...?
 *
 */

/*
 * Copyright (C) 1990-1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "$Id: getacctent.c,v 1.2 1999/07/21 16:26:06 obarring Exp $";
#endif /* not lint */


#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <pwd.h>
#include <stdlib.h>

#if !defined(_WIN32)
/*
 * _WIN32 strtok() is already MT safe where as others wait
 * for next POXIS release
 */
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* !_WIN32 */


#include "getacctent.h"

extern char *ypgetacctent();

char    *getacctent_r(pwd, acct, buf, buflen)
    struct passwd   *pwd;       /* Pointer to the password entry */
    char        *acct;      /* optional non-default acct     */
    char        *buf;
    int     buflen;
    { 
        char   tmpbuf[BUFSIZ], 
            savbuf[BUFSIZ]; /* Line buffer           */

        FILE      *fp = NULL; /* Pointer to /etc/account file  */
        char acct_file[256];
        char *p;
        char *last = NULL;

        if (pwd == (struct passwd *)NULL) return(NULL);

        /*
         * If possible /etc/account is parsed until the record
         * we are looking for is found, '+' is encountered as a user login
         * name (indicating that we have to look in YP) or the end of file
         * is reached.
         */
#if defined(_WIN32)
        if (strncmp (ACCT_FILE, "%SystemRoot%\\", 13) == 0 &&
            (p = getenv ("SystemRoot"))) 
            sprintf (acct_file, "%s\\%s", p, strchr (ACCT_FILE, '\\'));
        else
#endif
            strcpy (acct_file, ACCT_FILE);

        if ((fp = fopen(acct_file, "r")) == NULL) {
#if defined(NIS)
            if (ypgetacctent(pwd, acct, buf, buflen) != buf) return(NULL);
            else return(buf);
#else
            return(NULL);
#endif  /* NIS */
        }

        while (!feof(fp)) { 
            char  *cp = NULL;

            if ((cp = fgets(tmpbuf, BUFSIZ, fp)) != tmpbuf) {
                (void)fclose(fp);
                return(NULL);
            }

            (void)strcpy(savbuf, tmpbuf);

            if (savbuf[strlen(savbuf)-1] == NEWLINE)
                savbuf[strlen(savbuf)-1] = ENDSTR;

            if ((cp = strtok(tmpbuf, COLON_STR)) == NULL) {
                (void)fclose(fp);
                return(NULL);
            }

#if defined(NIS)
            if (strcmp(cp, PLUS_STR) == 0) {
                (void)fclose(fp);
                if (ypgetacctent(pwd, acct, buf, buflen) != buf)
                    return(NULL);
                else
                    return(buf);
            }
#endif  /* NIS */

            if (strcmp(pwd->pw_name, cp) == 0) {
                /* Login name matches */
                if ((cp = strtok((char *)NULL, COLON_STR)) == NULL) {
                    (void)fclose(fp);
                    return(NULL);
                }

                if (acct != NULL) {
                    if (strcmp(acct, cp) == 0) {
                        /* Account id matches */
                        (void)fclose(fp);
                        (void)strncpy(buf, savbuf, (size_t)buflen);
                        return(buf);
                    }
                } else {
                    char  *seq = NULL;
                    if ((seq = strtok((char *)NULL, COLON_STR)) == NULL) {
                        (void)fclose(fp);
                        return(NULL);
                    }
                    if (atoi(seq) == DFLT_SEQ_NUM) {
                        (void)fclose(fp);
                        (void)strncpy(buf, savbuf, (size_t)buflen);
                        return(buf);
                    }
                }
            }
        }

        (void)fclose(fp);
        return(NULL);
    }
