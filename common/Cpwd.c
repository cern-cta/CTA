
/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */


#ifndef lint
static char cvsId[] = "$RCSfile: Cpwd.c,v $ $Revision: 1.1 $ $Date: 1999/11/22 17:51:58 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * Cpwd.c - CASTOR MT-safe wrappers on some pwd routines.
 */ 

#include <sys/types.h>
#include <pwd.h>

#include <Cglobals.h>
#include <serrno.h>
#include <Cpwd.h>


struct passwd *Cgetpwnam(name)
const char *name;
{
#if (!defined(_REENTRANT) && !defined(_THREAD_SAFE))
    /*
     * If single-threaded compilation we don't do anything.
     */
    return(getpwnam(name));
#elif defined(linux) || (defined(__osf__) && defined(__alpha)) || \
      (defined(SOLARIS) && defined(_POSIX_PTHREAD_SEMANTICS)) || \
      defined(_AIX)
    /*
     * The final POSIX.1c standard: the return value is int and
     * buffer pointer is returned as last argument
     */
    static int pwd_key = -1;
    static int pwdbuf_key = -1;
    struct passwd *pwd = NULL;
    struct passwd *result = NULL;
    char *pwdbuf = NULL;
    size_t pwdbuflen = 1024;
    int rc;

    Cglobals_get(&pwd_key,(void **)&pwd,sizeof(struct passwd));
    Cglobals_get(&pwdbuf_key,(void **)&pwdbuf,pwdbuflen);

    if ( pwd == NULL || pwdbuf == NULL ) {
        serrno = SEINTERNAL;
        return(NULL);
    }
    rc = getpwnam_r(name,pwd,pwdbuf,pwdbuflen,&result);
    return(result);
#elif defined(hpux) || defined(HPUX10)
    static int pwd_key = -1;
    static int pwdbuf_key = -1;
    struct passwd *pwd = NULL;
    struct passwd *result = NULL;
    char *pwdbuf = NULL;
    size_t pwdbuflen = 1024;
    int rc;

    Cglobals_get(&pwd_key,(void **)&pwd,sizeof(struct passwd));
    Cglobals_get(&pwdbuf_key,(void **)&pwdbuf,pwdbuflen);

    if ( pwd == NULL || pwdbuf == NULL ) {
        serrno = SEINTERNAL;
        return(NULL);
    }
    rc = getpwnam_r(name,pwd,pwdbuf,pwdbuflen);
    if ( rc == 0 ) result = pwd;
    return(result);
#else
    /*
     * The POSIX.1c draft 6 standard: the return value is the result pointer.
     */
    static int pwd_key = -1;
    static int pwdbuf_key = -1;
    struct passwd *pwd = NULL;
    struct passwd *result = NULL;
    char *pwdbuf = NULL;
    size_t pwdbuflen = 1024;
    int rc;

    Cglobals_get(&pwd_key,(void **)&pwd,sizeof(struct passwd));
    Cglobals_get(&pwdbuf_key,(void **)&pwdbuf,pwdbuflen);

    if ( pwd == NULL || pwdbuf == NULL ) {
        serrno = SEINTERNAL;
        return(NULL);
    }
    result = getpwnam_r(name,pwd,pwdbuf,pwdbuflen);
    return(result);
#endif
}

struct passwd *Cgetpwuid(uid)
uid_t uid;
{
#if (!defined(_REENTRANT) && !defined(_THREAD_SAFE))
    /*
     * If single-threaded compilation we don't do anything.
     */
    return(getpwuid(uid));
#elif defined(linux) || (defined(__osf__) && defined(__alpha)) || \
      (defined(SOLARIS) && defined(_POSIX_PTHREAD_SEMANTICS)) || \
      defined(_AIX)
    /*
     * The final POSIX.1c standard: the return value is int and
     * buffer pointer is returned as last argument
     */
    static int pwd_key = -1;
    static int pwdbuf_key = -1;
    struct passwd *pwd = NULL;
    struct passwd *result = NULL;
    char *pwdbuf = NULL;
    size_t pwdbuflen = 1024;
    int rc;

    Cglobals_get(&pwd_key,(void **)&pwd,sizeof(struct passwd));
    Cglobals_get(&pwdbuf_key,(void **)&pwdbuf,pwdbuflen);

    if ( pwd == NULL || pwdbuf == NULL ) {
        serrno = SEINTERNAL;
        return(NULL);
    }
    rc = getpwuid_r(uid,pwd,pwdbuf,pwdbuflen,&result);
    return(result);
#elif defined(hpux) || defined(HPUX10)
    static int pwd_key = -1;
    static int pwdbuf_key = -1;
    struct passwd *pwd = NULL;
    struct passwd *result = NULL;
    char *pwdbuf = NULL;
    size_t pwdbuflen = 1024;
    int rc;

    Cglobals_get(&pwd_key,(void **)&pwd,sizeof(struct passwd));
    Cglobals_get(&pwdbuf_key,(void **)&pwdbuf,pwdbuflen);

    if ( pwd == NULL || pwdbuf == NULL ) {
        serrno = SEINTERNAL;
        return(NULL);
    }
    rc = getpwuid_r(uid,pwd,pwdbuf,pwdbuflen);
    if ( rc == 0 ) result = pwd;
    return(result);
#else
    /*
     * The POSIX.1c draft 6 standard: the return value is the result pointer.
     */
    static int pwd_key = -1;
    static int pwdbuf_key = -1;
    struct passwd *pwd = NULL;
    struct passwd *result = NULL;
    char *pwdbuf = NULL;
    size_t pwdbuflen = 1024;
    int rc;

    Cglobals_get(&pwd_key,(void **)&pwd,sizeof(struct passwd));
    Cglobals_get(&pwdbuf_key,(void **)&pwdbuf,pwdbuflen);

    if ( pwd == NULL || pwdbuf == NULL ) {
        serrno = SEINTERNAL;
        return(NULL);
    }
    result = getpwuid_r(uid,pwd,pwdbuf,pwdbuflen);
    return(result);
#endif
}

