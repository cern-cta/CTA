/*
 * Copyright (C) 1994-2003 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: getuser.c,v $ $Revision: 1.10 $ $Date: 2003/03/11 12:37:48 $ CERN IT/ADC/CA Jean-Damien Durand";
#endif /* not lint */

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <pwd.h>
#include <Castor_limits.h>
#include <Cglobals.h>
#include <Cpwd.h>
#include <serrno.h>
#include <log.h>

#if defined(_WIN32)
char *getenv();
#endif

#ifndef MAPPING_FILE
#if defined(_WIN32)
#define MAPPING_FILE "%SystemRoot%\\system32\\drivers\\etc\\users.ext"
#else
#define MAPPING_FILE "/etc/ext.users"
#endif /* _WIN32 */
#endif  

#if defined(_WIN32)
/*
 * infile will be modified at runtime when %SystemRoot% is
 * resolved. Must reserv enough space to hold new pathname.
 */
static char infile[CA_MAXPATHLEN+1] = MAPPING_FILE;
#else
static char *infile = MAPPING_FILE;
#endif  /* WIN32 */


#ifndef _WIN32
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */
#endif

/*
 * function finds the corresponding entry in the
 * mapping table;
 * returns 0 if entry was found
 *         1 on error (errno is set)
 *         -1 if entry was not found
 */

int DLL_DECL get_user(from_node,from_user,from_uid,from_gid,to_user,to_uid,to_gid)
char *from_node;
char *from_user;
int from_uid;
int from_gid;
char *to_user;
int *to_uid ;
int *to_gid ;
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
    int counter ;
#if defined(_WIN32)
    char path[CA_MAXPATHLEN+1];
#endif
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
    char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */

#if defined(_WIN32)
    strcpy(path, infile);
    if( (strncmp(path, "%SystemRoot%\\", 13) == 0) && ((p = getenv ("SystemRoot")) != NULL) ) {
        sprintf(infile, "%s\\%s", p, strchr (path, '\\'));
    }
#endif
   
    if  ( (fsin=fopen(infile,"r"))==NULL ) {
       log(LOG_ERR, "Could not open file %s, errno %d\n", infile, errno);
       serrno = ENOENT;
       return -ENOENT ;
    }

    while( fgets(buf,BUFSIZ,fsin)!=NULL ) {
        if ( buf[0] == '#') continue ;
        if ( ( p = strrchr( buf,'\n') ) != NULL ) *p='\0' ;

        log(LOG_DEBUG,"Entry >%s< in %s\n",buf,infile) ;

        p = strtok( buf, " \t");
        if ( p != NULL ) strcpy (maphostnam, p) ;
        else continue ;

        p = strtok( NULL, " \t");
        if ( p != NULL ) strcpy (mapuser, p) ;
        else continue ;

        p = strtok( NULL, " \t");
        if ( p != NULL ) strcpy (mapuid, p);
        else continue ;

        p = strtok( NULL, " \t");
        if ( p != NULL ) strcpy (mapgid, p);
        else continue ;

        p = strtok( NULL, " \t");
        if ( p != NULL ) strcpy (to_user,p);
        else continue ;

        /* Checking maphostnam */
        counter = 0 ;
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
