/*
 * Copyright (C) 1994-2003 by CERN/IT/ADC/CA
 * All rights reserved
 */

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <pwd.h>
#include <Castor_limits.h>
#include <Cglobals.h>
#include <Cpwd.h>
#include <serrno.h>
#include <log.h>

static char *infile = "/etc/castor/ext.users";

/*
 * function finds the corresponding entry in the
 * mapping table;
 * returns 0 if entry was found
 *         1 on error (errno is set)
 *         -1 if entry was not found
 */

int get_user(char *from_node,
             char *from_user,
             int from_uid,
             int from_gid,
             char *to_user,
             int *to_uid,
             int *to_gid)
{
    char *p ;
    struct passwd *pw ;
    FILE *fsin ;
    int uid,gid ;
    char buf[BUFSIZ+1] ;
    char maphostnam[CA_MAXHOSTNAMELEN+1];
    char mapuser[CA_MAXUSRNAMELEN+1] ;
    char mapuid[6] ;
    char mapgid[6] ;
    char *last = NULL;

    if  ( (fsin=fopen(infile,"r"))==NULL ) {
       log(LOG_ERR, "Could not open file %s, errno %d\n", infile, errno);
       serrno = ENOENT;
       return -ENOENT ;
    }

    while( fgets(buf,BUFSIZ,fsin)!=NULL ) {
        if ( buf[0] == '#') continue ;
        if ( ( p = strrchr( buf,'\n') ) != NULL ) *p='\0' ;

        log(LOG_DEBUG,"Entry >%s< in %s\n",buf,infile) ;

        p = strtok_r( buf, " \t",&last);
        if ( p != NULL ) strcpy (maphostnam, p) ;
        else continue ;

        p = strtok_r( NULL, " \t",&last);
        if ( p != NULL ) strcpy (mapuser, p) ;
        else continue ;

        p = strtok_r( NULL, " \t",&last);
        if ( p != NULL ) strcpy (mapuid, p);
        else continue ;

        p = strtok_r( NULL, " \t",&last);
        if ( p != NULL ) strcpy (mapgid, p);
        else continue ;

        p = strtok_r( NULL, " \t",&last);
        if ( p != NULL ) strcpy (to_user,p);
        else continue ;

        /* Checking maphostnam */
        if ( strcmp( maphostnam, from_node) && 
            !( (p=strchr(maphostnam,'*'))!= NULL && strstr(from_node,p+1) != NULL ) )
            continue ;

        if ( strcmp( mapuser , from_user ) && strcmp( mapuser ,"*" ) )
            continue ;

        if ( strcmp( mapuid, "*" ) && ( (uid=atoi(mapuid)) <= 0 || uid != from_uid ) ) 
            continue ;

        if ( strcmp( mapgid, "*" ) && ( (gid=atoi(mapgid)) <= 0 || gid != from_gid ) )
            continue ;

        log( LOG_DEBUG,"Found a possible entry: %s\n",to_user);

        if ( (pw = Cgetpwnam(to_user))==NULL) {
            log(LOG_INFO,"Cgetpwnam(): %s\n",sstrerror(serrno));
            continue ;
        } else {
            *to_uid = pw->pw_uid;
            *to_gid = pw->pw_gid;
        }
        fclose( fsin ) ;
        return 0 ;
    }
    fclose( fsin ) ;
    serrno = ENOENT;
    return -1 ;
}
