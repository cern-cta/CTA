/*
 * Copyright (C) 1990-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <pwd.h>
#include <stdlib.h>

/*
 * _WIN32 strtok() is already MT safe where as others wait
 * for next POSIX release
 */
#define strtok(X,Y) strtok_r(X,Y,&last)


#include <osdep.h>
#include <getacctent.h>
#include <ypgetacctent.h>

char *getacctent(struct passwd   *pwd,       /* Pointer to the password entry */
		 char        *acct,      /* optional non-default acct     */
		 char        *buf,
		 int     buflen)
    { 
        char   tmpbuf[BUFSIZ], 
            savbuf[BUFSIZ]; /* Line buffer           */

        FILE      *fp = NULL; /* Pointer to /etc/account file  */
        char acct_file[256];
        char *last = NULL;

        if (pwd == (struct passwd *)NULL) return(NULL);

        /*
         * If possible /etc/account is parsed until the record
         * we are looking for is found, '+' is encountered as a user login
         * name (indicating that we have to look in YP) or the end of file
         * is reached.
         */
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

            if (fgets(tmpbuf, BUFSIZ, fp) != tmpbuf) {
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
