
/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */


#ifndef lint
static char cvsId[] = "$RCSfile: Cnetdb.c,v $ $Revision: 1.6 $ $Date: 1999/12/15 06:14:29 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * Cnetdb.c - CASTOR MT-safe wrappers on netdb routines.
 */ 

#include <stddef.h>
#if defined(_WIN32)
#include <winsock2.h>
#else /* _WIN32 */
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#endif /* _WIN32 */

#include <Cglobals.h>
#include <Cnetdb.h>

#if (defined(IRIX5) || defined(IRIX6) || defined(IRIX64))
extern struct hostent *gethostbyname_r(const char *, struct hostent *, char *, int, int *);
extern struct hostent *gethostbyaddr_r(const void *, size_t, int, struct hostent *, char *, int, int *);
extern struct servent   *getservbyname_r(const char *, const char *,
                                         struct servent *, char *, int);
#endif

struct hostent *Cgethostbyname(name)
const char *name;
{
#if (!defined(_REENTRANT) && !defined(_THREAD_SAFE)) || defined(__osf__) && defined(__alpha) || defined(_WIN32)
    /*
     * If single-thread compilation we don't do anything.
     * Also: Windows and Digital UNIX v4 and higher already
     *       provide thread safe version.
     */
    return(gethostbyname(name));
#elif defined(SOLARIS) || defined(IRIX6)
    static int hostent_key = -1;
    static int hostdata_key = -1;
    struct hostent *hp;
    struct hostent *result = (struct hostent *)NULL;
    char *buffer = (char *)NULL;
    int bufsize = 1024;
    int h_errnoop;
    
    Cglobals_get(&hostent_key,(void **)&result,sizeof(struct hostent));
    Cglobals_get(&hostdata_key,(void **)&buffer,bufsize);

    if ( result == (struct hostent *)NULL || buffer == (char *)NULL ) {
        h_errno = NO_RECOVERY;
        return(NULL);
    }
    hp = gethostbyname_r(name,result,buffer,bufsize,&h_errnoop);
    h_errno = h_errnoop;
    return(hp);
#elif defined(AIX42) || defined(hpux) || defined(HPUX10)
    static int hostent_key = -1;
    static int hostdata_key = -1;
    int rc;
    struct hostent *result = (struct hostent *)NULL;
    struct hostent_data *ht_data = (struct hostent_data *)NULL;

    Cglobals_get(&hostent_key,(void **)&result,sizeof(struct hostent));
    Cglobals_get(&hostdata_key,(void **)&ht_data,sizeof(struct hostent_data));

    if ( result == (struct hostent *)NULL ||
        ht_data == (struct hostent_data *)NULL ) {
         h_errno = NO_RECOVERY;
         return(NULL);
    }

    rc = gethostbyname_r(name,result,ht_data);
    if (rc == -1) return(NULL);
    else return(result);
#elif defined(linux)
    static int hostent_key = -1;
    static int hostdata_key = -1;
    int rc;
    struct hostent *hp;
    struct hostent *result = (struct hostent *)NULL;
    char *buffer = (char *)NULL;
    int bufsize = 1024;
    int h_errnoop;

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
const void *addr;
size_t len;
int type;
{
#if (!defined(_REENTRANT) && !defined(_THREAD_SAFE)) || defined(__osf__) && defined(__alpha) || defined(_WIN32)
    /*
     * If single-thread compilation we don't do anything.
     * Also: Windows and Digital UNIX v4 and higher already
     *       provide thread safe version.
     */
    return(gethostbyaddr(addr,len,type));
#elif defined(SOLARIS) || defined(IRIX6)
    static int hostent_key = -1;
    static int hostdata_key = -1;
    struct hostent *hp;
    struct hostent *result = (struct hostent *)NULL;
    char *buffer = (char *)NULL;
    int bufsize = 1024;
    int h_errnoop;
    
    Cglobals_get(&hostent_key,(void **)&result,sizeof(struct hostent));
    Cglobals_get(&hostdata_key,(void **)&buffer,bufsize);

    if ( result == (struct hostent *)NULL || buffer == (char *)NULL ) {
        h_errno = NO_RECOVERY;
        return(NULL);
    }
    hp = gethostbyaddr_r(addr,len,type,result,buffer,bufsize,&h_errnoop);
    h_errno = h_errnoop;
    return(hp);
#elif defined(AIX42) || defined(hpux) || defined(HPUX10)
    static int hostent_key = -1;
    static int hostdata_key = -1;
    int rc;
    struct hostent *result = (struct hostent *)NULL;
    struct hostent_data *ht_data = (struct hostent_data *)NULL;

    Cglobals_get(&hostent_key,(void **)&result,sizeof(struct hostent));
    Cglobals_get(&hostdata_key,(void **)&ht_data,sizeof(struct hostent_data));

    if ( result == (struct hostent *)NULL ||
        ht_data == (struct hostent_data *)NULL ) {
         h_errno = NO_RECOVERY;
         return(NULL);
    }

    rc = gethostbyaddr_r(addr,len,type,result,ht_data);
    if (rc == -1) return(NULL);
    else return(result);
#elif defined(linux)
    static int hostent_key = -1;
    static int hostdata_key = -1;
    int rc;
    struct hostent *hp;
    struct hostent *result = (struct hostent *)NULL;
    char *buffer = (char *)NULL;
    int bufsize = 1024;
    int h_errnoop;

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
const char *name;
const char *proto;
{
#if (!defined(_REENTRANT) && !defined(_THREAD_SAFE)) || defined(__osf__) && defined(__alpha) || defined(_WIN32)
    /*
     * If single-thread compilation we don't do anything.
     * Also: Windows and Digital UNIX v4 and higher already
     *       provide thread safe version.
     */
    return(getservbyname(name,proto));
#elif defined(SOLARIS) || defined(IRIX6)
    static int servent_key = -1;
    static int servdata_key = -1;
    struct servent *sp;
    struct servent *result = (struct servent *)NULL;
    char *buffer = (char *)NULL;
    int bufsize = 1024;
   
    Cglobals_get(&servent_key,(void **)&result,sizeof(struct servent));
    Cglobals_get(&servdata_key,(void **)&buffer,bufsize);

    if ( result == (struct servent *)NULL || buffer == (char *)NULL ) {
        return(NULL);
    }
    sp = getservbyname_r(name,proto,result,buffer,bufsize);
    return(sp);
#elif defined(AIX42) || defined(hpux) || defined(HPUX10)
    static int servent_key = -1;
    static int servdata_key = -1;
    int rc;
    struct servent *result = (struct servent *)NULL;
    struct servent_data *st_data = (struct servent_data *)NULL;

    Cglobals_get(&servent_key,(void **)&result,sizeof(struct servent));
    Cglobals_get(&servdata_key,(void **)&st_data,sizeof(struct servent_data));

    if ( result == (struct servent *)NULL ||
        st_data == (struct servent_data *)NULL ) {
         return(NULL);
    }

    rc = getservbyname_r(name,proto,result,st_data);
    if (rc == -1) return(NULL);
    else return(result);
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

