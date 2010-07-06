
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

struct group *Cgetgrnam(name)
const char *name;
{
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
}

struct group *Cgetgrgid(gid)
gid_t gid;
{
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
}

