// CASTOR


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
#include "Csnprintf.h"
#include <Cglobals.h>


extern char *getconfent();
#if defined(CNS_ROOT)
extern int rfio_HsmIf_IsCnsFile _PROTO((char *));
#endif
EXTERN_C int DLL_DECL Cdomainname _PROTO((char *, int));

static int name1_key = -1;
static int buffer_key = -1;




/**************************************************************************/
/*      code taken from RfioTURL .... file disapeared after the merge     */ 
/**************************************************************************/


#define DEFAULT_RFIO_TURL_PREFIX "rfio://"
#define RFIO_PROTOCOL_NAME "rfio"

#include <stdlib.h>
#include <stdio.h>
#ifndef _WIN32
#include <unistd.h>
#else
#include "net.h"
#endif
#include <ctype.h>
#include <errno.h>
#include <serrno.h>
#include <rfio_api.h>

#include <stager_mapper.h>
#include <grp.h>
#include <sys/types.h>
#include <common.h>

#define DEFAULT_HOST "stagepublic"
#define DEFAULT_PORT2 9002
#define DEFAULT_PORT1 5007
#define DEFAULT_SVCCLASS ""  
#define DEFAULT_VERSION 1

typedef struct RfioTURL
{
  char rfioProtocolName[10]; /**< Should always be "rfio" */
  unsigned long rfioPort;    /**< The RFIO port number */
  char rfioHostName[CA_MAXHOSTNAMELEN+1]; /**< The rfio server host */
  char rfioPath[CA_MAXPATHLEN+1]; /**< The remote path */

} RfioTURL_t;


static int tURLPrefixKey = -1;
static int tURLPrefixLenKey = -1;

int tStageHostKey = -1;
int tStagePortKey = -1;
int tSvcClassKey = -1;
int tCastorVersionKey = -1;

// functions used only in this file.

static int checkAlphaNum(char *p) 
{
  int i;
  if ( p == NULL ) return(-1);
  for ( i=0; p[i] != '\0'; i++ ) {
    if ( !isalnum(p[i]) ) return(0);
  }
  return(1);
}

static int checkNum(char *p) 
{ 
  int i;
  if ( p == NULL ) return(-1);
  for ( i=0; p[i] != '\0'; i++ ) {
    if ( !isdigit(p[i]) ) return(0);
  }
  return(1);
}


char *getRfioTURLPrefix() 
{
  char *str;
  int rc, *tURLLen = NULL;
  
  rc = Cglobals_get(&tURLPrefixLenKey,(void *)&tURLLen,sizeof(int));
  if ( rc == -1 || tURLLen == NULL ) return(NULL);

  if ( *tURLLen == 0 ) return(DEFAULT_RFIO_TURL_PREFIX);
  
  rc = Cglobals_get(&tURLPrefixKey,(void *)&str,*tURLLen);
  if ( rc == -1 || str == NULL ) return(NULL);
  return(str);
}


/********************************************************************************************************************
 * This Function:                                                                                                   *
 * if *host or *svc are null or empty strings retrive the values, the same if *port and *version are <= 0.          * 
 * To retrive the values it looks if:                                                                               *
 * enviroment variables are set or stager_mapper as values defined or castor.conf or (if it doesn't have valid)     * 
 * it uses default hard coded values.                                                                               *
 *                                                                                                                  *
 *******************************************************************************************************************/   

int getDefaultForGlobal(
			char** host,
			int* port,
			char** svc,
			int* version)


{ 
	char *hostMap, *hostDefault, *svcMap, *svcDefault;
	int versionMap,versionDefault, portDefault, ret;
	char* aux=NULL; 
        struct group* grp=NULL; 
	gid_t gid;

       hostMap=hostDefault=svcMap=svcDefault=NULL;
       versionMap=versionDefault=portDefault=ret=0;

	if(host == NULL || port == NULL || svc == NULL || version == NULL ){ return (-1);}
	hostDefault=*host;
	svcDefault=*svc;
	portDefault=*port;
	versionDefault=*version;

	gid=getgid();

	if((grp = getgrgid(gid)) <= 0 ){
		ret=just_stage_mapper(getenv("USER"),NULL,&hostMap,&svcMap,&versionMap); 
	}
	else{
		ret=just_stage_mapper(NULL,grp->gr_name,&hostMap,&svcMap,&versionMap);
	}

	if(hostDefault==NULL || strcmp(hostDefault,"")==0){
		if(hostDefault){free(hostDefault);}
		aux=getenv("STAGE_HOST");
		hostDefault=aux==NULL?NULL:strdup(aux);
		if (hostDefault==NULL || strcmp(hostDefault,"")==0 ){
			if(hostDefault){free(hostDefault);}
			if (hostMap==NULL || strcmp(hostMap,"")==0 ){
				aux=(char*)getconfent("STAGER","HOST", 0);
				hostDefault=aux==NULL?NULL:strdup(aux);
				if (hostDefault==NULL || strcmp(hostDefault,"")==0 ){
					if(hostDefault){free(hostDefault);}
					hostDefault=strdup(DEFAULT_HOST);
				}
			}
			else{
				if(hostDefault){free(hostDefault);}
				hostDefault=strdup(hostMap);
			}
		}
	}
	

	if (svcDefault==NULL || strcmp(svcDefault,"")==0){
		if(svcDefault){free(svcDefault);}
		aux=getenv("STAGE_SVCCLASS");
		svcDefault= aux==NULL? NULL: strdup(aux);
		if (svcDefault==NULL || strcmp(svcDefault,"")==0 ){
			if(svcDefault){free(svcDefault);}
			if (svcMap==NULL || strcmp(svcMap,"")==0 ){
				aux=(char*)getconfent("STAGER","SVC_CLASS", 0);
				svcDefault=aux==NULL?NULL:strdup(aux);
				if (svcDefault==NULL || strcmp(svcDefault,"")==0 ){
					if(svcDefault){free(svcDefault);}
					svcDefault=strdup("");
				}
			}
			else{
				if(svcDefault){free(hostDefault);}
				svcDefault=strdup(DEFAULT_SVCCLASS);
			}
		}

	}
	if (versionDefault<=0){
		aux=getenv("RFIO_USE_CASTOR_V2");
		if(aux){
		        
			versionDefault=strcasecmp(aux,"YES")==0?2:1;
		}else{versionDefault=0;}
    		if (versionDefault<=0){
			versionDefault=versionMap+1;
			if (versionDefault<=0){
			aux=(char*)getconfent("STAGER","VERSION",0);
			versionDefault=aux==NULL?0:atoi(aux);
				if (versionDefault<=0){
					   versionDefault= DEFAULT_VERSION;
			 	}
			}
		}
	}

	if (portDefault<=0){
		aux=getenv("STAGE_PORT");
		portDefault=aux==NULL?0:atoi(aux);
		if (portDefault<=0){
			aux=(char*)getconfent("STAGER", "PORT", 0);
			portDefault=aux==NULL?0:atoi(aux);
			if (portDefault<=0){
			   portDefault= versionDefault==2?DEFAULT_PORT2:DEFAULT_PORT1;
			}
		}
		
	}

	if (*host==NULL || strcmp(*host,"")){*host=hostDefault;}	
	if (port==NULL || *port<=0) {*port=portDefault;}
	if (*svc==NULL || strcmp(*svc,"")){*svc=svcDefault;}
	if (version==NULL || *version<=0){*version=versionDefault;}

	return (1);
}

/********************************************************************************************************************
 * This Function is parsing the string and returning the Turl struct that will be used by rfio_parseln.             *
 * Also it sets global variables thread safe that are used in the other functions.                                  *
 *                                                                                                                  *
 *******************************************************************************************************************/   

int rfioTURLFromString(
                       char *tURLStr,
                       RfioTURL_t *tURL
                       ) 
{
  char *p, *q, *q1, *orig, *prefix;
  RfioTURL_t _tURL;
  int ret;
  char *path1, *path2, *mySvcClass, *myCastorVersion;
  char* buff;
  int versionNum=0;
  //void ** auxPointer=0;
  char endMark;

  char ** globalHost,  **globalSvc;
  int *globalVersion, *globalPort;

  globalHost=globalSvc=0;
  globalVersion=globalPort=0;

  path1=path2=myCastorVersion=mySvcClass=myCastorVersion=NULL;
  if ( tURLStr == NULL || tURL == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  prefix = getRfioTURLPrefix();
  if ( prefix == NULL ) return(-1);	
  if ( strstr(tURLStr,prefix) != tURLStr) {
    serrno = EINVAL;
    return(-1);
  }  
  /*
   * Extract the protocol name
   */

  orig = p = strdup(tURLStr);
  if ( p == NULL ) return(-1);
  
  q = strstr(p,":");
  if ( q == NULL ) {
    serrno = EINVAL;
    free(orig);
    return(-1);
  }
  *q = '\0';
  if ( strlen(p) >= sizeof(tURL->rfioProtocolName) ) {
    serrno = E2BIG;
    free(orig);
    return(-1);
  }

  if ( checkAlphaNum(p) != 1 ) {
    serrno = EINVAL;
    free(orig);
    return(-1);
  }
  
  strcpy(_tURL.rfioProtocolName,p);
  *q = ':';

  /*
   * Split off the prefix since we don't need it anymore.
   */
  p += strlen(prefix);

  /*
   * Expect the [host][:port] name at this point. The hostname is delimited from the path by a '/'
   * which is mandatory (thus, even if there is no hostname, like in a castor path)
   */

  q = strstr(p,"/");
  q1= strstr(p,"?");

  if ( q == NULL) {
       serrno = EINVAL;
       free(orig);
       return(-1);
  }

  if (q1 && q1<q) q=q1;

  endMark=*q;
  *q = '\0';

  q1 = strstr(p,":");
  if ( q1 != NULL ) {
    *q1 = '\0';
    q1++;
    if ( checkNum(q1) == 0 ) {
      serrno = EINVAL;
      free(orig);
      return(-1);
    }
    _tURL.rfioPort = atoi(q1);
  } else _tURL.rfioPort = 0;

  if ( strlen(p) >= sizeof(tURL->rfioHostName) ) {
    serrno = E2BIG;
    free(orig);
    return(-1);
  }
  strcpy(_tURL.rfioHostName,p);
   
  /*
   * Finally we only have the path and parameters  left
   */

/*  magic things to deal with a single slash */	
  
  *q=endMark;
  p=q;

  q=strstr(p,"?");
  if (!q){
     // no parameters specified
        path1=p;
  }
  else{  // parameters to parse
  
        if(p==q){
	    // no path after the port number
            path1=NULL;
	    *p='\0';
             p++;
        }
        else {
	    path1=p;
            *q='\0';
	    p=q+1;	
        }

	mySvcClass=strstr(p,"svcClass=");
	if (mySvcClass){
	    mySvcClass+=9; // to remove "svcClass="
       
	}


	myCastorVersion=strstr(p,"castorVersion=");
	if (myCastorVersion){
	    myCastorVersion+=14; // to remove "castorVersion="	
	
	}

	path2=strstr(p,"path=");
	if (path2){
	    path2+=5; // to remove "path="	
	   
	}

	if (mySvcClass){
              q1=strstr(mySvcClass,"&");
	       if (q1){*q1='\0';}
	}
	if (myCastorVersion){
              q1=strstr(myCastorVersion,"&");
              if(q1){*q1='\0';}
	}
	if (path2){
	      q1=strstr(path2,"&");
              if(q1){*q1='\0';}
	}

  }
   
   if (path2)
       path1=NULL;  // if I have the path given as option is ignored the other one.


   if (!path1 && !path2 ){
   // at least the path should be specified
      serrno = EINVAL;
      free(orig);
      return(-1);

   }

   if (path1){
     
     if ( strlen(path1) >= sizeof(tURL->rfioPath) ) {
         serrno = E2BIG;
         free(orig);
         return(-1);

     }

     strcpy(_tURL.rfioPath,path1);
     
   }

   if (path2){

     if ( strlen(path2) >= sizeof(tURL->rfioPath) ) {
         serrno = E2BIG;
         free(orig);
         return(-1);
     }
     
     strcpy(_tURL.rfioPath,path2);
   }

/* to remove the // or /// because of srm1 problems */

   if (_tURL.rfioPath && (strstr(_tURL.rfioPath,"//")==_tURL.rfioPath )){ 
	strcpy(_tURL.rfioPath,(char*)&_tURL.rfioPath[1]);
        if (_tURL.rfioPath && (strstr(_tURL.rfioPath,"//") == _tURL.rfioPath)){ 
	   strcpy(_tURL.rfioPath,(char*)&_tURL.rfioPath[1]);
        }
   }

   if (strstr(_tURL.rfioPath,"/castor") == _tURL.rfioPath ){ 
     
    /* from here setting the proper enviroment variable */
    /* getting default value that can be used */

	if (myCastorVersion){ 
      		if (!strcmp(myCastorVersion,"2")){
       			 versionNum=2;
       		}
		if (!strcmp(myCastorVersion,"1")){
       			 versionNum=1;
       		}
		
     	}
	
	/* Let's now set the global variable thread specific */
	
	ret=Cglobals_get(&tStageHostKey,(void **)&globalHost,sizeof(void*));
        
	if (ret<0){
		serrno = EINVAL;
		free(orig);
		return -1;

	}
	if(*globalHost){free(*globalHost); *globalHost=NULL;}

	if (strcmp(_tURL.rfioHostName,"")){
		*globalHost=strdup(_tURL.rfioHostName);	
	}

	ret=Cglobals_get(&tSvcClassKey,(void **)&globalSvc,sizeof(void*));

	if (ret<0){
		serrno = EINVAL;
		if(*globalHost){free(*globalHost);*globalHost=NULL;}
		free(orig);
		return -1;

	}
	if(*globalSvc){free(*globalSvc);*globalSvc=NULL;}

	if (mySvcClass && strcmp(mySvcClass,"")){
		*globalSvc=strdup(mySvcClass);
	}
	ret=Cglobals_get(&tStagePortKey,(void **)&globalPort,sizeof(int));
	if (ret<0){
		serrno = EINVAL;
		if(*globalHost){free(*globalHost);*globalHost=NULL;}
		if(*globalSvc){free(*globalSvc);*globalSvc=NULL;}
		free(orig);
		return -1;

	}
	*globalPort=0;
	if (_tURL.rfioPort){
		*globalPort=_tURL.rfioPort;
	}
	ret=Cglobals_get(&tCastorVersionKey,(void **)&globalVersion,sizeof(int));

	if (ret<0){
		serrno = EINVAL;
		if(*globalHost){free(*globalHost);*globalHost=NULL;}
		if(*globalSvc){free(*globalSvc);*globalSvc=NULL;}
		free(orig);
		return -1;

	}
	*globalVersion=0;
	if (versionNum){
		*globalVersion=versionNum;
	}
	ret=getDefaultForGlobal(globalHost,globalPort,globalSvc,globalVersion);
	if (ret<0){
			serrno = EINVAL;
			if(*globalHost){free(*globalHost);*globalHost=NULL;}
			if(*globalSvc){free(*globalSvc);*globalSvc=NULL;}
			free(orig);
			return -1;

	}
	_tURL.rfioHostName[0]=0;
	_tURL.rfioPort=-1;
     }
    
     free(orig);

  /*
   * Everything OK. Copy the temporary structure to the output
   */
    *tURL = _tURL;

     return(0);
}





/*******************************************************/
/*      rfio_parseln_old used only by rfio_parseln     */ 
/*******************************************************/

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
      Csnprintf(buf, CA_MAXHOSTNAMELEN, ":%ld", turl.rfioPort);
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
  } else {
    *path = "";
    /*  Setting path to empty string rather than NULL */
  }
  
  
  return hasHost;
}



int DLL_DECL rfio_parse(name, host, path)  /* parse name to host and path  */
char    *name;
char    **host;
char    **path;
{
   return ( rfio_parseln(name, host, path, RDLINKS) );
}
