/*
 * Copyright (C) 1991-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: getifnam.c,v $ $Revision: 1.10 $ $Date: 2005/07/11 10:09:32 $ CERN/IT/PDP/DM Frederic Hemmer";
#endif /* not lint */

/* getifnam.c   Get connected socket interface name                     */

#include <stdio.h>                      /* Standard input/output        */
#include <sys/types.h>                  /* Standard data types          */
#if !defined(_WIN32)
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#else
#include <winsock2.h>
#endif
#if defined(SOLARIS)
#include <sys/sockio.h>
#endif
#include <errno.h>                      /* Error numbers                */
#include <serrno.h>                     /* Special error numbers        */
#include <log.h>                        /* Generalized error logger     */
#if defined(_AIX)
#include <sys/time.h>                   /* needed for if.h              */
#endif /* AIX */
#if !defined(_WIN32)
#include <net/if.h>                     /* Network interfaces           */
#include <sys/ioctl.h>                  /* ioctl() definitions          */
#endif
#include <trace.h>                      /* tracing definitions          */
#include <string.h>                     /* For strlen                   */
#include <Cglobals.h>                   /* Cglobals prototypes          */

#include <net.h>                        /* Networking specifics         */

static int ifname_key = -1;             /* Key to interface name global */

/*
 * Note: originally, we did not create a socket to get the interfaces
 *       but (at least on SunOS 4.0.3), the first call on the connected
 *       socket returned a ifc.ifc_len == 0. Even with consecutive calls
 *       it failed. With a new socket creation it works however.
 */

char DLL_DECL  *getifnam_r(s,ifname,ifnamelen)
SOCKET     s;
char    *ifname;
size_t  ifnamelen;
{
#if defined(_WIN32)
    static char dummy_ifce[4]="???";    /* We don't know yet how to get it */
    return (dummy_ifce);
#else /* _WIN32 */
    struct  ifconf  ifc;            /* Interface configuration      */
    struct  ifreq   *ifr;           /* Interface request            */
    char    buf[BUFSIZ];            /* A buffer                     */
    struct  sockaddr_in addr;       /* Internet address             */
    socklen_t addrlen;              /* Internet address length      */
    long    binaddr;                /* Store address                */
    int     n;                      /* # of interfaces              */
    int     found=0;                /* Found the interface ?        */
    SOCKET  s_s;                    /* A socket to get interfaces   */
#if defined(_AIX) || defined(__Lynx__)
    struct  sockaddr_in *sp;    /* Ptr to sockaddr in ifreq buf */  
    char    *endp;          /* End of ifreq buffer      */  
#endif /* _AIX || __Lynx__ */

    if ( ifname == NULL || ifnamelen <= 0) return(NULL);
    INIT_TRACE("COMMON_TRACE");
    TRACE(1,"getifnam_r", "getifnam_r(%d) entered",s);
    addrlen = (socklen_t) sizeof(struct sockaddr_in);
    if (getsockname(s, (struct  sockaddr *)&addr, &addrlen) == SOCKET_ERROR ) {
        TRACE(2,"getifnam_r", "getsockname returned %d", errno);
        log(LOG_ERR, "getsockname: %s\n",strerror(errno));
        END_TRACE();
        return(NULL);
    } else {
        binaddr = addr.sin_addr.s_addr;
    }
    if ((s_s = socket(AF_INET, SOCK_DGRAM, 0)) == INVALID_SOCKET ) {
        log(LOG_ERR, "socket: %s\n",strerror(errno));
        return(NULL);
    }

    ifc.ifc_len = sizeof(buf);
    ifc.ifc_buf = buf;
    ifr = ifc.ifc_req;
    if ((n = ioctlsocket(s_s, SIOCGIFCONF, (char *)&ifc)) < 0) {
        TRACE(2,"getifnam_r", "netioctl returned %d", errno);
        log(LOG_ERR, "ioctl(SIOCGIFCONF): %s\n",strerror(errno));
        (void) netclose(s_s);
        END_TRACE();
        return(NULL);
    }
    else    {
#if defined(_AIX) || defined(__Lynx__)
        endp = (char *) ifr + ifc.ifc_len;
        sp = (struct sockaddr_in *) &ifr->ifr_addr;
        while ((char *) sp < endp) {
            if ((sp->sin_family == AF_INET) &&
                (binaddr == sp->sin_addr.s_addr)) {
                if ( ifnamelen > strlen(ifr->ifr_name) )
                    strcpy(ifname, ifr->ifr_name);
                else
                    strncpy(ifname,ifr->ifr_name,ifnamelen);
                found ++;
                break;
            }
            ifr = (struct ifreq *)((char *) sp + sp->sin_len);
            sp = (struct sockaddr_in *) &ifr->ifr_addr;
        }
#else /* _AIX || __Lynx__ */
        for (n = ifc.ifc_len/sizeof(struct ifreq); --n >= 0; ifr++){
            memcpy (&addr, &ifr->ifr_addr, sizeof(struct sockaddr_in));
            TRACE(2, "getifnam_r", "interface # %d, comparing 0X%X to 0X%X", n, binaddr, addr.sin_addr.s_addr);
            if (binaddr == addr.sin_addr.s_addr) {
                if ( ifnamelen > strlen(ifr->ifr_name) )
                    strcpy(ifname, ifr->ifr_name);
                else
                    strncpy(ifname,ifr->ifr_name,ifnamelen);
                found ++;
                break;
            }
        }
#endif /* _AIX || __Lynx__ */
    }
    (void) netclose(s_s);
    if (found) {
        TRACE(2,"getifnam_r", "returning %s", ifname);
        END_TRACE();
        ifname[ifnamelen-1] = '\0';
        return(ifname);
    } else {
        serrno = SEBADIFNAM;
        TRACE(2,"getifnam_r", "returning NULL");
        END_TRACE();
        return((char *) NULL);
    }
#endif /* _WIN32 */
}

char DLL_DECL *getifnam(s)
SOCKET s;
{
    char *ifname = NULL;
    size_t ifnamelen = 16;

    Cglobals_get(&ifname_key,(void **)&ifname,ifnamelen);

    return(getifnam_r(s,ifname,ifnamelen));
}
