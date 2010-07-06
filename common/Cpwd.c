
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

struct passwd *Cgetpwnam(name)
const char *name;
{
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
}

struct passwd *Cgetpwuid(uid)
uid_t uid;
{
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
}

