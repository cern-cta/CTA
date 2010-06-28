
/*
 * Copyright (C) 1999-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * Cpwd.c - CASTOR MT-safe wrappers on some pwd routines.
 */ 

#include <stdio.h>
#include <sys/types.h>
#include <pwd.h>

#include <Cglobals.h>
#include <errno.h>
#include <serrno.h>
#include <Cpwd.h>
#include <osdep.h>

struct passwd DLL_DECL *Cgetpwnam(name)
CONST char *name;
{
#if (!defined(_REENTRANT) && !defined(_THREAD_SAFE))
    /*
     * If single-threaded compilation we don't do anything.
     */
    return(getpwnam(name));
#else
    /*
     * linux or __APPLE__
     * The final POSIX.1c standard: the return value is int and
     * buffer pointer is returned as last argument
     */
    static int pwd_key = -1;
    static int pwdbuf_key = -1;
    struct passwd *pwd = NULL;
    struct passwd *result = NULL;
    char *pwdbuf = NULL;
    size_t pwdbuflen = BUFSIZ;
    int rc;

    Cglobals_get(&pwd_key,(void **)&pwd,sizeof(struct passwd));
    Cglobals_get(&pwdbuf_key,(void **)&pwdbuf,pwdbuflen);

    if ( pwd == NULL || pwdbuf == NULL ) {
        serrno = SEINTERNAL;
        return(NULL);
    }
    errno = 0;
    rc = getpwnam_r(name,pwd,pwdbuf,pwdbuflen,&result);
    serrno = ENOENT==errno?SEUSERUNKN:SEINTERNAL;
    return(result);
#endif
}

struct passwd DLL_DECL *Cgetpwuid(uid)
uid_t uid;
{
#if (!defined(_REENTRANT) && !defined(_THREAD_SAFE))
    /*
     * If single-threaded compilation we don't do anything.
     */
    return(getpwuid(uid));
#else
    /*
     * linux or __APPLE__
     * The final POSIX.1c standard: the return value is int and
     * buffer pointer is returned as last argument
     */
    static int pwd_key = -1;
    static int pwdbuf_key = -1;
    struct passwd *pwd = NULL;
    struct passwd *result = NULL;
    char *pwdbuf = NULL;
    size_t pwdbuflen = BUFSIZ;
    int rc;

    Cglobals_get(&pwd_key,(void **)&pwd,sizeof(struct passwd));
    Cglobals_get(&pwdbuf_key,(void **)&pwdbuf,pwdbuflen);

    if ( pwd == NULL || pwdbuf == NULL ) {
        serrno = SEINTERNAL;
        return(NULL);
    }
    errno = 0;
    rc = getpwuid_r(uid,pwd,pwdbuf,pwdbuflen,&result);
    serrno = ENOENT==errno?SEUSERUNKN:SEINTERNAL;
    return(result);
#endif
}

