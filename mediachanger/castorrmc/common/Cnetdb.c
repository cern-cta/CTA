/*
 * $Id: Cnetdb.c,v 1.2 1999/07/30 16:00:41 obarring Exp $
 * $Log: Cnetdb.c,v $
 * Revision 1.2  1999/07/30 16:00:41  obarring
 * Add copyright notice
 *
 * Revision 1.1  1999/07/30 15:03:55  obarring
 * First version
 *
 */

/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */


#ifndef lint
static char cvsId[] = "$Id: Cnetdb.c,v 1.2 1999/07/30 16:00:41 obarring Exp $";
#endif /* not lint */

#if defined(_WIN32)
#include <winsock2.h>
#else /* _WIN32 */
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#endif /* _WIN32 */

#include <Cglobals.h>
#include <Cnetdb.h>

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
