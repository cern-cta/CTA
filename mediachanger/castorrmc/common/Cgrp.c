
/*
 * Copyright (C) 2000-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */


/*
 * Cgrp.c - CASTOR MT-safe wrappers on some grp routines.
 */ 

#include <stdio.h>
#include <sys/types.h>
#include <grp.h>

#include <Cglobals.h>
#include <errno.h>
#include <serrno.h>
#include <Cgrp.h>
#include <osdep.h>

struct group DLL_DECL *Cgetgrnam(name)
CONST char *name;
{
#if (!defined(_REENTRANT) && !defined(_THREAD_SAFE))
    /*
     * If single-threaded compilation we don't do anything.
     */
    return(getgrnam(name));
#elif (defined(_WIN32))
    return(getgrnam((char *) name));
#else
    /*
     * linux or APPLE
     * The final POSIX.1c standard: the return value is int and
     * buffer pointer is returned as last argument
     */
    static int grp_key = -1;
    static int grpbuf_key = -1;
    struct group *grp = NULL;
    struct group *result = NULL;
    char *grpbuf = NULL;
    size_t grpbuflen = BUFSIZ;
    int rc;

    Cglobals_get(&grp_key,(void **)&grp,sizeof(struct group));
    Cglobals_get(&grpbuf_key,(void **)&grpbuf,grpbuflen);

    if ( grp == NULL || grpbuf == NULL ) {
        serrno = SEINTERNAL;
        return(NULL);
    }
    errno = 0;
    rc = getgrnam_r(name,grp,grpbuf,grpbuflen,&result);
    serrno = ENOENT==errno?SEGROUPUNKN:SEINTERNAL;
    return(result);
#endif
}

struct group DLL_DECL *Cgetgrgid(gid)
gid_t gid;
{
#if (!defined(_REENTRANT) && !defined(_THREAD_SAFE))
    /*
     * If single-threaded compilation we don't do anything.
     */
    return(getgrgid(gid));
#elif (defined(_WIN32))
    return(getgrgid(gid));
#else
    /*
     * linux or APPLE
     * The final POSIX.1c standard: the return value is int and
     * buffer pointer is returned as last argument
     */
    static int grp_key = -1;
    static int grpbuf_key = -1;
    struct group *grp = NULL;
    struct group *result = NULL;
    char *grpbuf = NULL;
    size_t grpbuflen = BUFSIZ;
    int rc;

    Cglobals_get(&grp_key,(void **)&grp,sizeof(struct group));
    Cglobals_get(&grpbuf_key,(void **)&grpbuf,grpbuflen);

    if ( grp == NULL || grpbuf == NULL ) {
        serrno = SEINTERNAL;
        return(NULL);
    }
    errno = 0;
    rc = getgrgid_r(gid,grp,grpbuf,grpbuflen,&result);
    serrno = ENOENT==errno?SEGROUPUNKN:SEINTERNAL;
    return(result);
#endif
}

