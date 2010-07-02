
/*
 * Copyright (C) 1999-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */


/*
 * Cnetdb.c - CASTOR MT-safe wrappers on netdb routines.
 */ 

#include <sys/types.h>
#include <stddef.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#include <Cglobals.h>
#include <Cnetdb.h>

struct hostent *Cgethostbyname(name)
CONST char *name;
{
#if (!defined(_REENTRANT) && !defined(_THREAD_SAFE)) || defined(__APPLE__)
    /*
     * If single-thread compilation we don't do anything.
     * Also: Windows, MACOSX and Digital UNIX v4 and higher already
     *       provide thread safe version.
     */
    return(gethostbyname(name));
#elif defined(linux)
    static int hostent_key = -1;
    static int hostdata_key = -1;
    int rc;
    struct hostent *hp;
    struct hostent *result = (struct hostent *)NULL;
    char *buffer = (char *)NULL;
    int bufsize = 1024;
    int h_errnoop = 0;

    Cglobals_get(&hostent_key,(void **)&result,sizeof(struct hostent));
    Cglobals_get(&hostdata_key,(void **)&buffer,bufsize);

    if ( result == (struct hostent *)NULL || buffer == (char *)NULL ) {
        h_errno = NO_RECOVERY;
        return(NULL);
    }
    rc = gethostbyname_r(name,result,buffer,bufsize,&hp,&h_errnoop);
    h_errno = h_errnoop;
    if ( rc == -1 ) hp = NULL;
    return(hp);
#else
    /*
     * Not supported
     */
    return(NULL);
#endif
}

struct hostent *Cgethostbyaddr(addr,len,type)
CONST void *addr;
size_t len;
int type;
{
#if (!defined(_REENTRANT) && !defined(_THREAD_SAFE)) || defined(__APPLE__)
    /*
     * If single-thread compilation we don't do anything.
     * Also: Windows, MACOSX and Digital UNIX v4 and higher already
     *       provide thread safe version.
     */
    return(gethostbyaddr(addr,len,type));
#elif defined(linux)
    static int hostent_key = -1;
    static int hostdata_key = -1;
    int rc;
    struct hostent *hp;
    struct hostent *result = (struct hostent *)NULL;
    char *buffer = (char *)NULL;
    int bufsize = 1024;
    int h_errnoop = 0;

    Cglobals_get(&hostent_key,(void **)&result,sizeof(struct hostent));
    Cglobals_get(&hostdata_key,(void **)&buffer,bufsize);

    if ( result == (struct hostent *)NULL || buffer == (char *)NULL ) {
        h_errno = NO_RECOVERY;
        return(NULL);
    }
    rc = gethostbyaddr_r(addr,len,type,result,buffer,bufsize,&hp,&h_errnoop);
    h_errno = h_errnoop;
    if ( rc == -1 ) hp = NULL;
    return(hp);
#else
    /*
     * Not supported
     */
    return(NULL);
#endif
}

struct servent *Cgetservbyname(name,proto)
CONST char *name;
CONST char *proto;
{
#if (!defined(_REENTRANT) && !defined(_THREAD_SAFE)) || defined(__APPLE__)
    /*
     * If single-thread compilation we don't do anything.
     * Also: Windows, MACOSX and Digital UNIX v4 and higher already
     *       provide thread safe version.
     */
    return(getservbyname(name,proto));
#elif defined(linux)
    static int servent_key = -1;
    static int servdata_key = -1;
    int rc;
    struct servent *sp;
    struct servent *result = (struct servent *)NULL;
    char *buffer = (char *)NULL;
    int bufsize = 1024;

    Cglobals_get(&servent_key,(void **)&result,sizeof(struct servent));
    Cglobals_get(&servdata_key,(void **)&buffer,bufsize);

    if ( result == (struct servent *)NULL || buffer == (char *)NULL ) {
        return(NULL);
    }
    rc = getservbyname_r(name,proto,result,buffer,bufsize,&sp);
    if ( rc == -1 ) sp = NULL;
    return(sp);
#else
    /*
     * Not supported
     */
    return(NULL);
#endif
}

