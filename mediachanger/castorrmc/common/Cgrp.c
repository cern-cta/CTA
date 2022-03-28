/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2000-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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
    void *grp = NULL;
    struct group *result = NULL;
    void *grpbuf = NULL;
    size_t grpbuflen = BUFSIZ;
    int rc;

    Cglobals_get(&grp_key,&grp,sizeof(struct group));
    Cglobals_get(&grpbuf_key,&grpbuf,grpbuflen);

    if ( grp == NULL || grpbuf == NULL ) {
        serrno = SEINTERNAL;
        return(NULL);
    }
    errno = 0;
    rc = getgrnam_r(name,grp,grpbuf,grpbuflen,&result);
    if (rc != 0) {
      serrno = ENOENT==errno?SEGROUPUNKN:SEINTERNAL;
    }
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
    void *grp = NULL;
    struct group *result = NULL;
    void *grpbuf = NULL;
    size_t grpbuflen = BUFSIZ;
    int rc;

    Cglobals_get(&grp_key,&grp,sizeof(struct group));
    Cglobals_get(&grpbuf_key,&grpbuf,grpbuflen);

    if ( grp == NULL || grpbuf == NULL ) {
        serrno = SEINTERNAL;
        return(NULL);
    }
    errno = 0;
    rc = getgrgid_r(gid,grp,grpbuf,grpbuflen,&result);
    if (rc != 0) {
      serrno = ENOENT==errno?SEGROUPUNKN:SEINTERNAL;
    }
    return(result);
}
