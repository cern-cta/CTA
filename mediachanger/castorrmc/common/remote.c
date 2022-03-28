/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 1990-2022 CERN
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
 *                   1 if requestor is out of site
 *          -1 in case of an error
 */
int isremote(struct in_addr from_host,
             char *host_name)
{
    char *p ;
    char local[CA_MAXHOSTNAMELEN+1];
    char buf[BUFSIZ];            /* A buffer                     */
    char ent[25] ;
    struct hostent  *h;
    int   s_s;
    struct  ifconf  ifc;     /* ifconf structure      */
    struct  ifreq   *ifr;    /* Pointer on ifreq structure */
    int n ;
    union adr {
        u_long adr_i;
        unsigned char adr_c[4];
    } *ladd;
    struct in_addr in ;
    struct  sockaddr_in addr;
    char *last = NULL;

    char lhfile[CA_MAXPATHLEN+1] = "/etc/castor/castor.localhosts";
    char rthfile[CA_MAXPATHLEN+1] = "/etc/castor/castor.remhosts";

    if ( (p=getconfent("SIMULATION","REMOTE",1))!=NULL &&
         (p=(char *)strtok_r(p," \t",&last))!=NULL && !strcmp(p,"YES")) {
        (*logfunc)(LOG_DEBUG,"isremote(): Client simulates remote behaviour\n");
        return 1 ;
    }
    if ( (p=getconfent("ISREMOTE","CALLS",1))!=NULL &&
         (p=(char *)strtok_r(p," \t",&last) )!=NULL && !strcmp(p,"NO") ) {
        (*logfunc)(LOG_DEBUG,"isremote(): Any connection assumed from local site\n");
        return 0 ;
    }

    /*
     * getting local IP number
     */
    gethostname(local, CA_MAXHOSTNAMELEN+1);

    if ( (h=(struct hostent *)Cgethostbyname(local))==NULL) {
        (*logfunc)(LOG_ERR,"isremote(): gethostbyname() error\n");
        return -1 ;
    }
    ladd=(union adr *)h->h_addr_list[0];
    in.s_addr=ladd->adr_i ;
    (*logfunc)(LOG_DEBUG, "isremote(): Local host is %s\n",inet_ntoa( in ));

    if ( host_name != NULL ) {
        FILE *fs;
        char *cp ;
        char s[CA_MAXHOSTNAMELEN+1];
        /*
         * Is the hostname declared as a "remote" site host ?
         */
        (*logfunc)(LOG_DEBUG,"isremote(): searching <%s> in %s\n",host_name, rthfile);
        if ( (fs = fopen( rthfile, "r")) != NULL ) {
            while ( fgets(s,CA_MAXHOSTNAMELEN+1,fs) != NULL ) {
              if ( (cp= strtok_r(s," \n\t",&last))!=NULL ) {
                    if ( !isdigit(cp[0]) && (cp[0]!='#') &&  !strcmp(cp,host_name) ) {
                        (*logfunc)(LOG_DEBUG,"isremote(): %s is in list of external hosts\n",cp);
                        fclose(fs);
                        return 1;
                    }
                    if ( isdigit(cp[0]) ) {
                        strcpy(ent,cp) ;
                        if ( strtok_r(cp,".",&last) ==  NULL ||
                             strtok_r(NULL,".",&last) == NULL )
                            (*logfunc)(LOG_DEBUG,"%s ignored: IP specification too short\n", ent);
                        else {
                            if ( !strncmp( ent, inet_ntoa( from_host ), strlen(ent))) {
                                (*logfunc)(LOG_DEBUG,"Entry %s matches to %s\n",ent,inet_ntoa(from_host));
                                (*logfunc)(LOG_INFO,"isremote(): %s is classified as remote\n",host_name);
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
        (*logfunc)(LOG_DEBUG,"isremote(): searching <%s> in %s\n",host_name, lhfile);
        if ( (fs = fopen( lhfile, "r")) != NULL ) {
            while ( fgets(s,CA_MAXHOSTNAMELEN+1,fs) != NULL ) {
              if ( (cp= strtok_r(s," \n\t",&last)) != NULL ) {
                    if ( !isdigit(cp[0]) && (cp[0]!='#') &&  !strcmp(cp,host_name) ) {
                        (*logfunc)(LOG_DEBUG,"isremote(): %s is in list of local hosts\n",cp);
                        fclose(fs);
                        return 0;
                    }
                    if ( isdigit(cp[0]) ) {
                        strcpy(ent,cp) ;
                        if ( strtok_r(cp,".",&last) ==  NULL ||
                             strtok_r(NULL,".",&last) == NULL )
                            (*logfunc)(LOG_DEBUG,"%s ignored: IP specification too short \n", ent);
                        else {
                            if ( !strncmp( ent, inet_ntoa( from_host ), strlen(ent) )) {
                                (*logfunc)(LOG_DEBUG,"Entry %s matches to %s\n",ent,inet_ntoa(from_host));
                                (*logfunc)(LOG_DEBUG,"isremote(): %s is classified as local\n",host_name);
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

    (*logfunc)(LOG_DEBUG, "isremote(): Client host is %s\n",inet_ntoa( from_host )) ;

    if( (s_s = socket(AF_INET, SOCK_DGRAM, 0)) == -1 )  {
        (*logfunc)(LOG_ERR, "socket: %s\n",strerror(errno));
        return -1;
    }

    ifc.ifc_len = sizeof(buf);
    ifc.ifc_buf = buf;
    ifr = ifc.ifc_req;

    if ((n = ioctl(s_s, SIOCGIFCONF, (char *)&ifc)) < 0) {
        (*logfunc)(LOG_ERR, "ioctl(SIOCGIFCONF): %s\n",strerror(errno));
        close(s_s);
        return -1;
    }
    else
    {
        for (n = ifc.ifc_len/sizeof(struct ifreq); --n >= 0; ifr++)  {
            memcpy (&addr, &ifr->ifr_addr, sizeof(struct sockaddr_in));
            (*logfunc)(LOG_DEBUG , "Comparing %d and %d \n",  inet_netof(addr.sin_addr), inet_netof(from_host));
            if ( inet_netof(addr.sin_addr) == inet_netof(from_host) ) {
                close(s_s);
                (*logfunc)(LOG_DEBUG ,"isremote(): client is in same site\n");
                return 0;
            }
        }
    }
    close(s_s);
    (*logfunc)(LOG_INFO ,"isremote(): client is in another site\n");
    return 1;
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
        (*logfunc)(LOG_ERR, "CDoubleDnsLookup() getpeername(): %s\n",neterror());
        errno = save_errno;
        return(-1);
    }

    if ( (hp = Cgethostbyaddr((void *)&from.sin_addr,sizeof(struct in_addr),from.sin_family)) == NULL ) {
        save_errno = (serrno > 0 ? serrno : errno);
        (*logfunc)(LOG_ERR,"CDoubleDnsLookup() Cgethostbyaddr(): h_errno=%d, %s\n",
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
        (*logfunc)(LOG_ERR,"CDoubleDnsLookup() isremote(): %s\n",neterror());
        errno = save_errno;
        return(-1);
    }

    if ( (i==0) && (p = strchr(tmphost,'.')) != NULL ) *p = '\0';
    if ( host != NULL ) strcpy(host,tmphost);

    if ( (hp = Cgethostbyname(tmphost)) == NULL ) {
        save_errno = (serrno > 0 ? serrno : errno);
        (*logfunc)(LOG_ERR,"CDoubleDnsLookup() Cgethostbyname(): h_errno=%d, %s\n",
            h_errno,neterror());
        if ( h_errno == HOST_NOT_FOUND ) serrno = SENOSHOST;
        else serrno = save_errno;
        return(-1);
    }
    i = 0;
    while (hp->h_addr_list[i] != NULL) {
        (*logfunc)(LOG_DEBUG,"CDoubleDnsLookup() comparing %s with %s\n",
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
