
/*
 * parse.c,v 1.17 2004/03/02 09:17:40 obarring Exp
 */

/*
 * Copyright (C) 1990-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)parse.c,v 1.17 2004/03/02 09:17:40 CERN/IT/PDP/DM Frederic Hemmer";
#endif /* not lint */

/* parse.c      Remote File I/O - parse file path                       */

#define RFIO_KERNEL     1       /* system part of Remote File I/O       */

#if !defined(_WIN32)
#include <sys/param.h>		/* system dependent parameters		*/
#endif
#include <stdlib.h>
#include <string.h>
#include <Castor_limits.h>
#ifndef CA_MAXDOMAINNAMELEN
#define CA_MAXDOMAINNAMELEN CA_MAXHOSTNAMELEN
#endif
#include "osdep.h"
#include "Cns_api.h"
#include "rfio.h"               /* remote file I/O definitions          */
#include <Cglobals.h>
#include <RfioTURL.h>

extern char *getconfent();
#if defined(CNS_ROOT)
extern int rfio_HsmIf_IsCnsFile _PROTO((char *));
#endif
EXTERN_C int DLL_DECL Cdomainname _PROTO((char *, int));
static int rfio_parseln_old _PROTO((char *, char **, char **,int));

static int name1_key = -1;
static int buffer_key = -1;

int DLL_DECL rfio_parseln(name, host, path, ln) /* parse name to host and path  */
char    *name;
char    **host;
char    **path;
int   	ln ; 	/* solve links or not ? */
{
  RfioTURL_t turl;
  char buf[CA_MAXHOSTNAMELEN+1];
  int hasHost = 0;;
  
  /* First try out the name as if it was a tURL !
     If it isn't try the old RFIO parsing algorithm */
  if (rfioTURLFromString(name, &turl) < 0) {
    TRACE (3,"rfio","rfioTURLFromString does not recognize TURL");	
    return  rfio_parseln_old(name, host, path, ln);
  }
  
  TRACE (3,"rfio","name was TURL <%s> <%d> <%s>", 
         turl.rfioHostName, turl.rfioPort,turl.rfioPath);	
  if (strlen(turl.rfioHostName) > 0) {
    hasHost = 1;
    if (turl.rfioPort > 0) {
      int hostlen, portlen;
      hostlen = strlen(turl.rfioHostName);
      snprintf(buf, CA_MAXHOSTNAMELEN, ":%lx", turl.rfioPort);
      buf[CA_MAXHOSTNAMELEN] = '\0';
      portlen = strlen(buf);
      *host =(char *)malloc(hostlen + portlen + 1);
    if (*host == NULL) {
      serrno = ENOMEM;
      return -1;
    }
    strcpy(*host, turl.rfioHostName);
    strcat(*host, buf);
    } else {
      *host = strdup(turl.rfioHostName);
      if (*host == NULL) {
        serrno = ENOMEM;
        return -1;
      }
    }
  } else {
    *host = NULL;
    TRACE (3,"rfio","No host given, using old RFIO parseln on path %s",
           turl.rfioPath );
    return  rfio_parseln_old(turl.rfioPath, host, path, ln);
  }
  
  if (strlen(turl.rfioPath)>0) {
    *path = strdup(turl.rfioPath);
    if (*path == NULL) {
      serrno = ENOMEM;
      return -1;
    }
  }
  
  return hasHost;
}




static int rfio_parseln_old(name, host, path, ln) /* parse name to host and path  */
char    *name;
char    **host;
char    **path;
int   	ln ; 	/* solve links or not ? */
{
   char    *cp1, *cp2, *cp3;
   register int i;
   char	localhost[CA_MAXHOSTNAMELEN+1];
   char localdomain[CA_MAXDOMAINNAMELEN+1];  /* Local domain */
   int 	n = 0;
   char 	*name_1 = NULL;
   char  	*buffer = NULL;    /* To hold temporary strings */
   char		*cwd_server;       /* Current CASTOR working directory server */
/*
 * forms recognized: host:<unix path name>
 *                   /<prefix>/host/<unix path name>
 *
 * where prefix is defined in /etc/shift.conf :
 *      RFIO  NFS_ROOT          /.....
 */

   /* Get local domain */
   if (Cdomainname(localdomain,CA_MAXDOMAINNAMELEN) != 0) {
     strcpy(localdomain,"localdomain");
   }

   if ( rfioreadopt(RFIO_CONNECTOPT) == RFIO_FORCELOCAL )  {
      TRACE (2,"rfio","rfio_parseln(): Forcing local calls");
      *host = NULL;
      *path = name;
      return(0) ;
   }

   if ( name == NULL ) {
     serrno = EINVAL;
     return(-1);
   }
   
   if (strlen(name) > CA_MAXPATHLEN) {
     /* Not enough space */
     serrno = SENAMETOOLONG;
     return(-1);
   }
   if (gethostname(localhost, sizeof(localhost)) < 0)	{
      return(-1);
   }

   Cglobals_get(&name1_key, (void**)&name_1, CA_MAXPATHLEN+1);
   Cglobals_get(&buffer_key, (void**)&buffer, CA_MAXPATHLEN+1);
   
   if (ln != NORDLINKS ) {
      int countln = 0 ;
      char     buffer1[CA_MAXPATHLEN+1],buffer2[CA_MAXPATHLEN+1];

      strcpy ( buffer1, name );
#if defined(FollowRtLinks)
      while ( (n= rfio_readlink(buffer1,buffer,CA_MAXPATHLEN+1)) > 0  && countln < 4 ) 
#else
#if !defined(_WIN32)
	 while ( (n= readlink(buffer1,buffer,CA_MAXPATHLEN+1)) > 0  && countln < 4 ) 
#else
	    if (countln) 	/* i.e. never, because links are not impl. */
#endif
#endif /* FollowRtLinks */
	    {
	       /* save original filename */
			if (strlen(buffer1) > CA_MAXPATHLEN) {
				/* Not enough space */
				serrno = SENAMETOOLONG;
				return(-1);
			}
	       strcpy(buffer2,buffer1);
	       countln ++ ;
	       buffer[n]='\0';
	       /* Is it a relative path ? */
	       if (buffer[0] != '/')  {
		  char *last_slash;
		  /* Is the relative link of the form host:/path ? */
		  if (strstr(buffer,":/") != NULL) {
		     /* Simply replace name */
		     strcpy(buffer1,buffer);
		  }  else  {
		     /* Host name is NOT like XXXX:/YYYYY */
		     if (strchr(buffer,'$') != NULL &&
			 (cp3 = (char *)strchr(buffer,':')) != NULL ) {
			/* This is a VMS syntax ! */
			strcpy(buffer1,buffer);
		     }
		     if ((last_slash =  strrchr(buffer1,'/')) == NULL) {
			/* Simply replace name */
			strcpy(buffer1,buffer);
		     }  else  {
			/* Concat basename of old name with new name */
			strcpy(last_slash + 1,buffer);
		     }
		  }
	       }    else
		  strcpy(buffer1, buffer);
	       TRACE (3,"rfio","rfio_parseln(): %s converted to %s",buffer2,buffer1);	
	    }
      strcpy(name_1,buffer1);
   }
   else
      strcpy(name_1,name);

   if (((cp1 = (char *)strstr(name_1,":/")) == NULL) || (strchr(name_1,'/') < (cp1+1))  ){
      /* Host name is NOT like XXXX:/YYYYY */
	   /* The other case: it contains ":/". So we a sure that a strchr() of character "/" will not */
	   /* return NULL - we then demand that there is no other "/" character before the one at cp1+1... */
      cp1 = name_1;
      if ( strchr(name_1,'$') != NULL && (cp3=(char *)strchr(name_1,':')) != NULL ) {
	 /* This is a VMS syntax ! */
	 *host= name_1 ;
	 *path= cp3+1 ;
	 cp3[0]='\0' ;
	 return (1) ;
      }
#if defined(CNS_ROOT)
      TRACE(3,"rfio","rfio_parseln() check %s against castor root %s",
            name_1,CNS_ROOT);
      if ( rfio_HsmIf_IsCnsFile(name_1) > 0 ) {
         cwd_server = rfio_HsmIf_GetCwdServer();
         *host = NULL;
         *path = NULL;
         *buffer = '\0';
         TRACE(3,"rfio","rfio_parseln() call Cns_selectsrvr(%s,0x%lx,0x%lx,0x%lx)",
               name_1,cwd_server,buffer,path);
               
	 if ( Cns_selectsrvr(name_1,cwd_server,buffer,path) == -1 ) {
             TRACE(3,"rfio","rfio_parseln() Cns_selectsrvr(): %s",
                   sstrerror(serrno));
	    *host = NULL;
	    *path = name_1;
	    return(-1);
	 }
         *host = buffer;
         TRACE(3,"rfio","rfio_parseln() Cns_selectsrvr() returns host=%s, path=%s",
               *host,*path);
	 return(0);
      }
#endif /* CNS_ROOT */
	  {
		  int sav_serrno = serrno;
		  cp2 = getconfent("RFIO","NFS_ROOT", 0);
#ifdef NFSROOT
		  if (cp2 == NULL) {
			  TRACE(3,"rfio","rfio_parseln() Using site-wide NFS ROOT \"%s\"", NFSROOT);
			  cp2 = NFSROOT;
			  /* Restore serrno */
			  serrno = sav_serrno;
		  }
#endif
	  }
	  if (cp2 == NULL)     {
	 *host = NULL;
	 *path = name_1;
	 serrno=0; /* reset error */
	 return(0);
      }
      /* compare the 2 strings        */
      for (i=0;i< (int)strlen(cp2);i++)     {
	 if (*(cp1+i) != *(cp2+i))       {
	    *host = NULL;
	    *path = name_1;
	    return(0);
	 }
      }
      cp1 += strlen(cp2);
      /* cp2 may be matching cp1 but being shorted */
      if (*cp1 != '/')        {
	 *host = NULL;
	 *path = name_1;
	 return(0);
      }
      /* cp1 now points to rooted nfs_path_name     */
      /* next token must be the node name prefixed by '/' */
      if ((cp2 = strchr(++cp1,'/')) == NULL)   {
	 /* incomplete name: /prefix/node  */
	 *host = NULL;
	 *path = name_1;
	 return(0);
      }

      strncpy(buffer,cp1,(int)(cp2-cp1));
      buffer[cp2-cp1] = '\0';
      *host = buffer;
      *path = name_1;
      /* is it localhost ? */
      if (strcmp(localhost, buffer) == 0 || strcmp("localhost",buffer) == 0)  {
	 *host = NULL;
	 return(0);
      } else {
	char *p;

	/* Perhaps localhost is a FQDN and buffer is not ? */
	if ((strchr(buffer,'.') == NULL) &&                       /* buffer is not a FDQN */
	    ((p = strstr(localhost,localdomain)) != NULL) &&        /* but localhost is because */
	    (p > localhost) &&                                    /* we are sure it ends in the form */
	    (*(p-1) == '.') &&                                    /* .localdomain\0 */
	    (p[strlen(localdomain)] == '\0')) {
	  *(p-1) = '\0';
	  if (strcmp(buffer,localhost) == 0) {
	    *host = NULL;
	    return(0);
	  }
	  
	}
	return(1);
      }
   } else {
     /* first check if the path is in DOS format, e.g. hostname:x:/path/name */
     if( ((cp2 = strchr(name_1, ':')) != NULL) && (cp1 == cp2+2))  {
       strncpy(buffer, name_1, cp2-name_1);
       buffer[cp2-name_1] = '\0';
       *host = buffer;
       *path = cp2+1;
     }  else  {
       strncpy(buffer,name_1,cp1-name_1);
       buffer[cp1-name_1] = '\0';
       *host = buffer;
       *path = cp1+1;
     }
     /* is it localhost ? */
     if (strcmp(localhost, buffer) == 0 || strcmp("localhost", buffer) == 0)  {
       *host = NULL;
       return(0);
     } else {
       char *p;
	
       /* Perhaps localhost is a FQDN and buffer is not ? */
       if ((strchr(buffer,'.') == NULL) &&                       /* buffer is not a FDQN */
	   ((p = strstr(localhost,localdomain)) != NULL) &&        /* but localhost is because */
	   (p > localhost) &&                                    /* we are sure it ends in the form */
	   (*(p-1) == '.') &&                                    /* .localdomain\0 */
	   (p[strlen(localdomain)] == '\0')) {
	 *(p-1) = '\0';
	 if (strcmp(buffer,localhost) == 0) {
	   *host = NULL;
	   return(0);
	 } 
       }
       return(1);
     }
   }
}

int DLL_DECL rfio_parse(name, host, path)  /* parse name to host and path  */
char    *name;
char    **host;
char    **path;
{
   return ( rfio_parseln(name, host, path, RDLINKS) );
}
