/*
 * Copyright (C) 1990-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: getacctent.c,v $ $Revision: 1.8 $ $Date: 2007/02/13 07:52:24 $ CERN/IT/PDP/DM Olof Barring";
#endif /* not lint */


#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <pwd.h>
#include <stdlib.h>

#if !defined(_WIN32)
/*
 * _WIN32 strtok() is already MT safe where as others wait
 * for next POSIX release
 */
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */
#else
#include <windows.h>
#endif /* !_WIN32 */


#include <osdep.h>
#include <getacctent.h>
#include <ypgetacctent.h>

char DLL_DECL *getacctent(pwd, acct, buf, buflen)
    struct passwd   *pwd;       /* Pointer to the password entry */
    char        *acct;      /* optional non-default acct     */
    char        *buf;
    int     buflen;
    { 
        char   tmpbuf[BUFSIZ], 
            savbuf[BUFSIZ]; /* Line buffer           */

        FILE      *fp = NULL; /* Pointer to /etc/account file  */
        char acct_file[256];
#if !defined(_WIN32) && (defined(_REENTRANT) || defined(_THREAD_SAFE))
        char *last = NULL;
#endif /* !_WIN32 && (_REENTRANT || _THREAD_SAFE) */ 
#if defined(_WIN32)
        OSVERSIONINFO osvi;
	char *p;
#endif

        if (pwd == (struct passwd *)NULL) return(NULL);

        /*
         * If possible /etc/account is parsed until the record
         * we are looking for is found, '+' is encountered as a user login
         * name (indicating that we have to look in YP) or the end of file
         * is reached.
         */
#if defined(_WIN32)
        memset (&osvi, 0, sizeof(osvi));
        osvi.dwOSVersionInfoSize = sizeof(osvi);
        GetVersionEx (&osvi);
        if (osvi.dwMajorVersion > 5) {
                if (strncmp (W2000MAPDIR, "%SystemRoot%\\", 13) == 0 &&
                  (p = getenv ("SystemRoot")))
                    sprintf (acct_file, "%s%s\\account", p, strchr (W2000MAPDIR, '\\'));
                else
                    sprintf (acct_file, "%s\\account", W2000MAPDIR);
        } else {
                if (strncmp (WNTMAPDIR, "%SystemRoot%\\", 13) == 0 &&
                  (p = getenv ("SystemRoot")))
                    sprintf (acct_file, "%s%s\\account", p, strchr (WNTMAPDIR, '\\'));
                else
                    sprintf (acct_file, "%s\\account", WNTMAPDIR);
        }
#else
        strcpy (acct_file, ACCT_FILE);
#endif

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
