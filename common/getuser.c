/*
 * Copyright (C) 1994-1998 by CERN CN-PDP/CS
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)getuser.c	1.4 03/26/99 CERN CN-SW/DC Felix Hassine";
#endif /* not lint */

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <pwd.h>
#include <log.h>

#if !defined(linux)
extern char     *sys_errlist[] ;        /* System error list  */
#endif
extern int      errno ;                 /* Current system error index */

#ifndef MAPPING_FILE
#if defined(_WIN32)
#define MAPPING_FILE "%SystemRoot%\\system32\\drivers\\etc\\users.ext"
#else
#define MAPPING_FILE "/etc/ext.users"
#endif	/* WIN32 */
#endif 	

static char *infile = MAPPING_FILE;

/*
 * function finds the corresponding entry in the
 * mapping table;
 * returns 0 if entry was found
 *         1 on error (errno is set)
 *         -1 if entry was not found
 */

int get_user(from_node,from_user,from_uid,from_gid,to_user,to_uid,to_gid)
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
   char buf[256] ;
   char maphostnam[50];
   char mapuser[10] ;
   char mapuid[6] ;
   char mapgid[6] ;
   int counter ;
#if defined(_WIN32)
   char path[256];
#endif   

#if defined(_WIN32)
   strcpy(path, infile);
   if( (strncmp(path, "%SystemRoot%\\", 13) == 0) && ((p = getenv ("SystemRoot")) != NULL) ) {
      sprintf(infile, "%s\\%s", p, strchr (path, '\\'));
   }
#endif
   
   if  ( (fsin=fopen(infile,"r"))==NULL ) {
      log(LOG_ERR, "Could not open file %s, errno %d", infile, errno);
      /* fprintf(stderr,"Could not open file %s\n",infile); */
      return -ENOENT ;
   }
   while( fgets(buf,256,fsin)!=NULL ) {
      if ( buf[0] == '#')
	 continue ;
      if ( ( p = strrchr( buf,'\n') ) != NULL )
	 *p='\0' ;
      log(LOG_DEBUG,"Entry >%s< in %s\n",buf,infile) ;

      p = strtok( buf, " \t");
      if ( p != NULL ) 	
	 strcpy (maphostnam, p) ;
      else
	 continue ;
      p = strtok( NULL, " \t");
      if ( p != NULL ) 
	 strcpy (mapuser, p) ;
      else
	 continue ;
      p = strtok( NULL, " \t");
      if ( p != NULL ) 
	 strcpy (mapuid, p);
      else
	 continue ;
      p = strtok( NULL, " \t");
      if ( p != NULL ) 
	 strcpy (mapgid, p);
      else
	 continue ;
      p = strtok( NULL, " \t");
      if ( p != NULL ) 
	 strcpy (to_user,p);
      else
	 continue ;

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
      if ( (pw = getpwnam(to_user))==NULL) {
	 log(LOG_INFO,"getpwnam(): %s\n",sys_errlist[errno]);
	 continue ;
      }
      else {
	 *to_uid = pw->pw_uid;
	 *to_gid = pw->pw_gid;
      }
      fclose( fsin ) ;
      return 0 ;

   }
   fclose( fsin ) ;
   return -1 ;
}
