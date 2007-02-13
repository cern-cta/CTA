/*
 * Copyright (C) 1993-2000 by CERN/ID/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: solveln.c,v $ $Revision: 1.5 $ $Date: 2007/02/13 07:52:24 $ CERN/IT/PDP/DM Felix Hassine";
#endif /* not lint */


#include <errno.h>
#include <stdio.h>
#if defined(_WIN32)
#define MAXHOSTNAMELEN 64
#else
#if defined(__Lynx__)
#include <socket.h>
#else
#include <sys/param.h>
#endif
#endif
#include <string.h>
#if defined(_WIN32)
#include <direct.h>
#else
#include <unistd.h>
#endif
#if defined(SOLARIS)
#include <netdb.h>
#endif /* SOLARIS */
#include <osdep.h>
#define MAXFILENAMSIZE 1024     /* Maximum length of a file path name   */

/*
 * path is assumed to be a file name or a directory path.
 * It does not modify the content of buffer path.
 * return -1 if path has not been modified, 
 * a positive number otherwise.
 */
int DLL_DECL seelink ( path, buff, size) 
char * path ;
char * buff ;
int size    ;
{
#if defined(_WIN32)
	strcpy(buff, path);
	return (strlen(path));
#else
	char *cp ;
	char filename[MAXFILENAMSIZE] ;
	char storpath[MAXFILENAMSIZE] ;
	char stordir[MAXFILENAMSIZE] ;
	int n ;
	
	strcpy(storpath, path );
	if ( (cp = strrchr(storpath,'/')) != NULL ) {
		strcpy(filename,cp+1) ;
		cp[0] = '\0' ;
	}
	else
		return -1 ;
	
	while ((cp = getcwd(stordir, MAXFILENAMSIZE-1)) == NULL && errno == ETIMEDOUT)
		sleep (60);
	if ( chdir(storpath)  < 0 ) {
		errno = ENOENT ;
		return -1 ;
	}
	else {
		cp = getcwd( buff, size );
		chdir(stordir);
		if ( cp == NULL )
			return -1 ;
		else {
			/* 
			 * Putting back file name
			 */
			strcat (buff,"/") ;
			strcat (buff ,filename) ;
			strcpy(filename, buff ) ;
			if ( (n=readlink( filename, buff, size)) < 0 ) 
				return (strlen(filename) ) ;
			else {
				buff[n] = '\0' ;
				return ( strlen(buff) ) ;
			}
		}
	}
#endif
}


/*
 * Solves links on path names given at command line.
 * Adds hostname when file path is local.
 * Returns -errno on failure, 0 otherwise.
 * Input     :  path to be modified
 *              buffer that contains the path transformed
 *              size is the buffer size.
 * if size if not sufficient, nothing is changed and -ENAMETOOLONG is
 * returned.
 * It is assumed that path begins by '/' or that it contains ":/" .
 */
#if defined(SOLARIS)
extern char * getcwd() ;
#endif

extern char *getconfent() ;
int DLL_DECL solveln(path, buffer, size)
char *path ;
char * buffer ;
int size ;
{
        char *nfsroot ;
        int n ;
        char hostname[MAXHOSTNAMELEN] ;
        char * p ;
 
        nfsroot = getconfent("RFIO","NFS_ROOT",0) ;
#ifdef NFSROOT
        if (nfsroot == NULL) nfsroot = NFSROOT;
#endif
        if ( (nfsroot == NULL  && strstr(path,":/") != NULL )  ||
             (nfsroot != NULL && !strncmp(path,nfsroot,strlen(nfsroot)) )) {
                        if ((int)strlen(path)>size) {
				(void) strncpy(buffer,path,size) ;
				return -ENAMETOOLONG;
			}
			else {
				(void) strcpy(buffer,path) ;
                        	return 0 ;
			}
        }
 
        if ( nfsroot == NULL  && strstr(path,":/") == NULL ) {
                        if ( (n=seelink(path, buffer, size)) > 0 )
                                buffer[n]='\0' ;
                        else {
                                if ((int)strlen(path)>size) {
					(void) strncpy(buffer,path,size) ;
					return -ENAMETOOLONG;
				}
				else {
					(void) strcpy(buffer,path) ;
					return 0 ;
				}
			}
                        return 0 ;
        }
 
        /*
         * Now we know we have nfs root != NULL & path does not have nfsroot
         * in the beginning
         */
 
        if ( (p=strstr(path,":/")) != NULL && !strncmp( p+1 , nfsroot
 ,strlen(nfsroot)) ){
                /*
                 * Then erase machine name
                 */
                if ((int)strlen(p+1)>size) { 
			(void) strncpy(buffer,path,size) ;
			return -ENAMETOOLONG;
		}
		else
			(void) strcpy(buffer, p+1 ) ;
                return 0 ;
        }
        /*
         * path is machine:/XXXX and XXXX != nfsroot
         */
        if ( (  p=strstr(path,":/")) != NULL             &&
                strncmp( p+1 , nfsroot ,strlen(nfsroot)) ) {
                        if ( (n=seelink( p+1 , buffer, size)) > 0 ) {
                                buffer[n]='\0' ;
                                if ( !strncmp( buffer,nfsroot,strlen(nfsroot)) )
                                        return 0 ;
                                else {
                                        (void) strcpy(buffer,path) ;
                                        return 0 ;
                                }
                        }
                        else {
                                (void) strcpy(buffer,path) ;
                                return 0 ;
                        }
        }
        /*
         * Now we know that path starts by '/',  & path does not have nfsroot
         * in its beginning
         */
        gethostname( hostname, MAXHOSTNAMELEN ) ;
         if ( (n=seelink(path, buffer, size)) < 0 ) {
#if defined(sgi) && !defined(IRIX5)
                if ( errno == ENXIO || errno == ENOENT )  {
#else
                if ( errno == EINVAL || errno == ENOENT ) {
#endif
                        sprintf( buffer, "%s:%s",hostname,path ) ;
                        return 0 ;
                }
                else
                        return -errno ;
         }
         else {
                buffer[n]='\0' ;
                if ( !strncmp( buffer , nfsroot ,strlen(nfsroot)) || 
		     strstr(buffer,":/") !=NULL )
                        return 0 ;
                else {/* File is a simple local file */
			char mobuf[MAXFILENAMSIZE] ;
			strcpy(mobuf,buffer) ;
                        sprintf( buffer, "%s:%s",hostname,mobuf ) ;
                        return 0 ;
                }
        }
}

