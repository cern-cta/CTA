/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char cvsId[] = "@(#)$RCSfile: remote.c,v $ $Revision: 1.11 $ $Date: 2001/01/30 13:31:18 $ CERN/IT/PDP/DM Olof Barring";
#endif /* not lint */

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#include <ws2tcpip.h>
#include <ws_errmsg.h>
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/param.h>
#endif  /* WIN32 */
#include <string.h>
#if defined(SOLARIS)
#include <sys/sockio.h>
#endif
#if defined(_AIX)
#include <sys/time.h>                   /* needed for if.h              */
#endif /* AIX */
#if !defined(_WIN32)
#include <net/if.h>                     /* Network interfaces           */
#include <sys/ioctl.h>                  /* ioctl() definitions          */
#endif
#include <Castor_limits.h>
#include <log.h> 
#include <net.h>
#include <serrno.h>
#include <Cnetdb.h>

#ifndef _WIN32
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */
#endif /* _WIN32 */

#ifndef LOCALHOSTSFILE
#if defined(_WIN32)
#define LOCALHOSTSFILE "%SystemRoot%\\system32\\drivers\\etc\\shift.localhosts"
#else
#define LOCALHOSTSFILE "/etc/shift.localhosts"
#endif  /* WIN32 */
#endif
#ifndef RTHOSTSFILE
#if defined(_WIN32)
#define RTHOSTSFILE "%SystemRoot%\\system32\\drivers\\etc\\shift.rthosts"
#else
#define RTHOSTSFILE "/etc/shift.rthosts"
#endif  /* WIN32 */
#endif

#if !defined(linux)
extern char *sys_errlist[];
#endif

/*
 * isremote(): returns 0 if requestor is in site
 *                   1 if requestor is out of site
 *          -1 in case of an error
 */
int DLL_DECL isremote(from_host, host_name)
struct in_addr from_host;
char *host_name ;
{
    char *p ;
    char local[CA_MAXHOSTNAMELEN+1];
    char buf[BUFSIZ];            /* A buffer                     */
    char ent[25] ;
    struct hostent  *h;
    SOCKET   s_s;
#if defined(_WIN32)
    INTERFACE_INFO *iinfo;
    char ibuf[BUFSIZ];
    int buflen;
    int rcode;
#else   
    struct  ifconf  ifc;     /* ifconf structure      */
    struct  ifreq   *ifr;    /* Pointer on ifreq structure */
    int n ;          
    char    *endp;
    struct  sockaddr_in     *sp;
    long binaddr ;
#endif  /* WIN32 */
    unsigned int netw ;
    union adr {
        u_long adr_i;
        unsigned char adr_c[4];
    } adr,*ladd;
    struct in_addr in ;
    struct  sockaddr_in addr;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
    char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */

#if defined(_WIN32) || defined(__STDC__)
    char lhfile[CA_MAXPATHLEN+1] =  LOCALHOSTSFILE;
    char rthfile[CA_MAXPATHLEN+1] = RTHOSTSFILE;
#else
    char *lhfile = LOCALHOSTSFILE;
    char *rthfile = RTHOSTSFILE;
#endif
   
    extern char *getconfent();

    if ( (p=getconfent("SIMULATION","REMOTE",1))!=NULL &&
        (p=(char *)strtok(p," \t"))!=NULL && !strcmp(p,"YES")) {
        log(LOG_DEBUG,"isremote(): Client simulates remote behaviour\n");
        return 1 ;
    }
    if ( (p=getconfent("ISREMOTE","CALLS",1))!=NULL &&
        (p=(char *)strtok(p," \t") )!=NULL && !strcmp(p,"NO") ) {
        log(LOG_DEBUG,"isremote(): Any connection assumed from local site\n");
        return 0 ;
    }

    /*
     * getting local IP number
     */
    gethostname(local, CA_MAXHOSTNAMELEN+1);
     
    if ( (h=(struct hostent *)Cgethostbyname(local))==NULL) {
        log(LOG_ERR,"isremote(): gethostbyname() error\n");
        return -1 ;
    }
    ladd=(union adr *)h->h_addr_list[0];
    in.s_addr=ladd->adr_i ;
    log(LOG_DEBUG, "isremote(): Local host is %s\n",inet_ntoa( in ));

    if ( host_name != NULL ) {
        FILE *fs;
        char *cp ;
        char s[CA_MAXHOSTNAMELEN+1];
#if defined(_WIN32)
        char path[256];
#endif   
        /*
         * Is the hostname declared as a "remote" site host ?
         */
#if defined(_WIN32)
        strcpy(path, rthfile);
        if( (strncmp(path, "%SystemRoot%\\", 13) == 0) && ((p = getenv ("SystemRoot")) != NULL) ) {
            sprintf (rthfile, "%s\\%s", p, strchr (path, '\\'));
        }
#endif
        log(LOG_DEBUG,"isremote(): searching <%s> in %s\n",host_name, rthfile);
        if ( (fs = fopen( rthfile, "r")) != NULL ) {
            while ( fgets(s,CA_MAXHOSTNAMELEN+1,fs) != NULL ) {
                if ( (cp= strtok(s," \n\t"))!=NULL ) {
                    if ( !isdigit(cp[0]) && (cp[0]!='#') &&  !strcmp(cp,host_name) ) {
                        log(LOG_DEBUG,"isremote(): %s is in list of external hosts\n",cp);
                        fclose(fs);
                        return 1;
                    }
                    if ( isdigit(cp[0]) ) {
                        strcpy(ent,cp) ;
                        if ( strtok(cp,".") ==  NULL ||
                            strtok(NULL,".") == NULL )
                            log(LOG_DEBUG,"%s ignored: IP specification too short\n", ent);
                        else {
                            if ( !strncmp( ent, inet_ntoa( from_host ), strlen(ent))) {
                                log(LOG_DEBUG,"Entry %s matches to %s\n",ent,inet_ntoa(from_host));
                                log(LOG_INFO,"isremote(): %s is classified as remote\n",host_name);
                                fclose(fs) ;
                                return 1 ;
                            }
                        }
                    }
                }
            }
            fclose(fs);
        }


        /*
         * Is the hostname declared local ?
         */
#if defined(_WIN32)
        strcpy(path, lhfile);
        if( (strncmp(path, "%SystemRoot%\\", 13) == 0) && ((p = getenv ("SystemRoot")) != NULL) ) {
            sprintf (lhfile, "%s\\%s", p, strchr (path, '\\'));
        }
#endif
        log(LOG_DEBUG,"isremote(): searching <%s> in %s\n",host_name, lhfile);
        if ( (fs = fopen( lhfile, "r")) != NULL ) {
            while ( fgets(s,CA_MAXHOSTNAMELEN+1,fs) != NULL ) {
                if ( (cp= strtok(s," \n\t")) != NULL ) {
                    if ( !isdigit(cp[0]) && (cp[0]!='#') &&  !strcmp(cp,host_name) ) {
                        log(LOG_DEBUG,"isremote(): %s is in list of local hosts\n",cp);
                        fclose(fs);
                        return 0;
                    }
                    if ( isdigit(cp[0]) ) {
                        strcpy(ent,cp) ;
                        if ( strtok(cp,".") ==  NULL || 
                            strtok(NULL,".") == NULL )
                            log(LOG_DEBUG,"%s ignored: IP specification too short \n", ent);
                        else {
                            if ( !strncmp( ent, inet_ntoa( from_host ), strlen(ent) )) {
                                log(LOG_DEBUG,"Entry %s matches to %s\n",ent,inet_ntoa(from_host));
                                log(LOG_DEBUG,"isremote(): %s is classified as local\n",host_name);
                                fclose (fs);
                                return 0 ;
                            }
                        }
                    }
                }
            }
            fclose(fs);
        }
    } /* if ( host_name != NULL ) */

    netw = inet_netof(from_host);
#if defined(_WIN32)
    log(LOG_DEBUG, "inet_netof() returned %u\n", netw);
#endif   
    adr.adr_i=from_host.s_addr;
    log(LOG_DEBUG, "isremote(): Client host is %s\n",inet_ntoa( from_host )) ;

    if( (s_s = socket(AF_INET, SOCK_DGRAM, 0)) == SOCKET_ERROR )  {
#if defined(_WIN32)
        log(LOG_ERR, "socket: %s", geterr());
#else      
        log(LOG_ERR, "socket: %s\n",sys_errlist[errno]);
#endif  /* WIN32 */
        return -1;
    }
   
#if defined(_WIN32)
    rcode = WSAIoctl(s_s, SIO_GET_INTERFACE_LIST, ibuf, BUFSIZ, buf, BUFSIZ, &buflen, NULL, NULL);
    if( rcode == SOCKET_ERROR )  {
        log(LOG_ERR, "WSAIoctl(SIO_GET_INTERFACE_LIST): %s\n", geterr());
        closesocket(s_s);
        return -1;
    } else {
        for( iinfo = (INTERFACE_INFO*)buf; iinfo < (INTERFACE_INFO*)(buf + buflen); iinfo++ ) {
            memcpy (&addr, &(iinfo->iiAddress), sizeof(struct sockaddr_in));
            log(LOG_DEBUG , "Comparing %d and %d\n",  inet_netof(addr.sin_addr), inet_netof(from_host));
            if ( inet_netof(addr.sin_addr) == inet_netof(from_host) ) {
                closesocket(s_s);
                log(LOG_DEBUG ,"isremote(): client is in same site\n");
                return 0;
            }
        }
    }
#else
    ifc.ifc_len = sizeof(buf);
    ifc.ifc_buf = buf;
    ifr = ifc.ifc_req;

    if ((n = ioctl(s_s, SIOCGIFCONF, (char *)&ifc)) < 0) {
        log(LOG_ERR, "ioctl(SIOCGIFCONF): %s\n",sys_errlist[errno]);
        close(s_s);
        return -1;
    } 
    else 
#if !defined(_AIX)
    {
        for (n = ifc.ifc_len/sizeof(struct ifreq); --n >= 0; ifr++)  {
            memcpy (&addr, &ifr->ifr_addr, sizeof(struct sockaddr_in));
            log(LOG_DEBUG , "Comparing %d and %d \n",  inet_netof(addr.sin_addr), inet_netof(from_host));
            if ( inet_netof(addr.sin_addr) == inet_netof(from_host) ) {
                close(s_s);
                log(LOG_DEBUG ,"isremote(): client is in same site\n");
                return 0;
            }
        }
    }
#else
    {
        endp = (char *) ifr + ifc.ifc_len;
        sp = (struct sockaddr_in *) &ifr->ifr_addr;
        while ((char *) sp < endp) {
            log(LOG_DEBUG , " Comparing %d and %d \n",netw, inet_netof(sp->sin_addr)) ;
            if ((sp->sin_family == AF_INET) &&
                ( netw == inet_netof (sp->sin_addr) )){
                close(s_s);
                log(LOG_DEBUG ,"isremote(): client is in same site\n");
                return 0;
            }
            ifr = (struct ifreq *)((char *) sp + sp->sin_len);
            sp = (struct sockaddr_in *) &ifr->ifr_addr;
        }
    }
#endif  /* AIX */
#endif  /* WIN32 */
    closesocket(s_s);
    log(LOG_INFO ,"isremote(): client is in another site\n");
    return 1;
}

int DLL_DECL CDoubleDnsLookup(SOCKET s, char *host) {
    char tmphost[CA_MAXHOSTNAMELEN+1], *p;
    struct sockaddr_in from;
    struct hostent *hp;
    int i;
    int fromlen = sizeof(from);

    if ( s == INVALID_SOCKET ) {
        serrno = EBADF;
        return(-1);
    }
    if ( getpeername(s,(struct sockaddr *)&from,&fromlen) == SOCKET_ERROR ) {
        log(LOG_ERR, "CDoubleDnsLookup() getpeername(): %s\n",neterror());
        serrno = SECOMERR;
        return(-1);
    }

    if ( (hp = Cgethostbyaddr((void *)&from.sin_addr,sizeof(struct in_addr),from.sin_family)) == NULL ) {
        log(LOG_ERR,"CDoubleDnsLookup() Cgethostbyaddr(): h_errno=%d, %s\n",
            h_errno,neterror());
        serrno = SECOMERR;
        return(-1);
    }
    strcpy(tmphost,hp->h_name);

    /*
     * Only allow for on-site authorised hosts
     */
    if ( (i = isremote(from.sin_addr,tmphost)) != 0 ) {
        if ( i == -1 ) return(-1);
        if ( i == 1 ) return(0);
    } 

    if ( (p = strchr(tmphost,'.')) != NULL ) *p = '\0';
    if ( host != NULL ) strcpy(host,tmphost);
    if ( (hp = Cgethostbyname(tmphost)) == NULL ) {
        log(LOG_ERR,"CDoubleDnsLookup() Cgethostbyname(): h_errno=%d, %s\n",
            h_errno,neterror());
        serrno = SECOMERR;
        return(-1);
    }
    i = 0;
    while (hp->h_addr_list[i] != NULL) {
        log(LOG_DEBUG,"CDoubleDnsLookup() comparing %s with %s\n",
            inet_ntoa(*(struct in_addr *)(hp->h_addr_list[i])),inet_ntoa(from.sin_addr));
        if ( ((struct in_addr *)(hp->h_addr_list[i++]))->s_addr == from.sin_addr.s_addr ) return(1);
    }
    return(0);
}

int DLL_DECL isadminhost(SOCKET s, char *peerhost) {
    int i, rc;
#if defined(ADMIN_HOSTS)
    char *defined_admin_hosts = ADMIN_HOSTS;
#endif /* ADMIN_HOSTS */
    char *admin_hosts, *admin_host;

    rc = CDoubleDnsLookup(s,peerhost);
    if ( rc == -1 ) return(-1);

    admin_host = admin_hosts = NULL;
    if ( admin_hosts == NULL ) admin_hosts = getenv("ADMIN_HOSTS");
    if ( admin_hosts == NULL ) admin_hosts = getconfent("ADMIN","HOSTS",1);
#if defined(ADMIN_HOSTS)
    if ( admin_hosts == NULL ) admin_hosts = defined_admin_hosts;
#endif /* ADMIN_HOSTS */

    if ( (admin_hosts != NULL) && 
         ((admin_host = strstr(admin_hosts,peerhost)) != NULL) ) {
        i = strlen(peerhost);
        if ( admin_host[i] == '\0' ||
             admin_host[i] == ' ' ||
             admin_host[i] == '\t' ||
             admin_host[i] == ',' ) return(1);
    }
    return(0);
}
