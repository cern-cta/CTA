/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/param.h>
#include <ctype.h>
#include <string.h>
#include <net/if.h>                     /* Network interfaces           */
#include <sys/ioctl.h>                  /* ioctl() definitions          */
#include <Castor_limits.h>
#include <log.h> 
#include <net.h>
#include <serrno.h>
#include <Cnetdb.h>

extern char *getconfent();

/*
 * isremote(): returns 0 if requestor is in site
 *                     1 if requestor is out of site
 *                    -1 in case of an error
 * *DEPRECATED*: this concept is obsolete, isremote
 * always returns 0.
 */
int isremote(struct in_addr from_host,
             char* host_name)
{
    (void)from_host;
    (void)host_name;
    return 0;
}

int CDoubleDnsLookup(int s, char *host) {
    char tmphost[CA_MAXHOSTNAMELEN+1], *p;
    struct sockaddr_in from;
    struct hostent *hp;
    int i, save_errno;
    socklen_t fromlen = (socklen_t) sizeof(from);

    if ( s == -1 ) {
        serrno = EBADF;
        return(-1);
    }
    if ( getpeername(s,(struct sockaddr *)&from,&fromlen) == -1 ) {
        save_errno = errno;
        log(LOG_ERR, "CDoubleDnsLookup() getpeername(): %s\n",neterror());
        errno = save_errno;
        return(-1);
    }

    if ( (hp = Cgethostbyaddr((void *)&from.sin_addr,sizeof(struct in_addr),from.sin_family)) == NULL ) {
        save_errno = (serrno > 0 ? serrno : errno);
        log(LOG_ERR,"CDoubleDnsLookup() Cgethostbyaddr(): h_errno=%d, %s\n",
            h_errno,neterror());
        serrno = save_errno;
        return(-1);
    }
    strcpy(tmphost,hp->h_name);

    /*
     * Remove domain from on-site hosts
     */
    if ( (i = isremote(from.sin_addr,tmphost)) == -1 ) {
        save_errno = errno;
        log(LOG_ERR,"CDoubleDnsLookup() isremote(): %s\n",neterror());
        errno = save_errno;
        return(-1);
    } 

    if ( (i==0) && (p = strchr(tmphost,'.')) != NULL ) *p = '\0';
    if ( host != NULL ) strcpy(host,tmphost);

    if ( (hp = Cgethostbyname(tmphost)) == NULL ) {
        save_errno = (serrno > 0 ? serrno : errno);
        log(LOG_ERR,"CDoubleDnsLookup() Cgethostbyname(): h_errno=%d, %s\n",
            h_errno,neterror());
        if ( h_errno == HOST_NOT_FOUND ) serrno = SENOSHOST;
        else serrno = save_errno;
        return(-1);
    }
    i = 0;
    while (hp->h_addr_list[i] != NULL) {
        log(LOG_DEBUG,"CDoubleDnsLookup() comparing %s with %s\n",
            inet_ntoa(*(struct in_addr *)(hp->h_addr_list[i])),inet_ntoa(from.sin_addr));
        if ( ((struct in_addr *)(hp->h_addr_list[i++]))->s_addr == from.sin_addr.s_addr ) return(0);
    }
    serrno = SENOSHOST;
    return(-1);
}

int isadminhost(int s, char *peerhost) {
    int i, rc;
    char *admin_hosts, *admin_host;

    rc = CDoubleDnsLookup(s,peerhost);
    if ( rc == -1 ) return(rc);

    admin_host = admin_hosts = NULL;
    if ( admin_hosts == NULL ) admin_hosts = getenv("ADMIN_HOSTS");
    if ( admin_hosts == NULL ) admin_hosts = getconfent("ADMIN","HOSTS",1);

    if ( (admin_hosts != NULL) && 
         ((admin_host = strstr(admin_hosts,peerhost)) != NULL) ) {
        i = strlen(peerhost);
        if ( (admin_host[i] == '\0' ||
              admin_host[i] == ' ' ||
              admin_host[i] == '\t' ||
              admin_host[i] == ',' ) &&
             (admin_host == admin_hosts ||
              admin_host[-1] == ' ' ||
              admin_host[-1] == '\t' ||
              admin_host[-1] == ',') ) return(0);
    }
    serrno = SENOTADMIN;
    return(-1);
}
