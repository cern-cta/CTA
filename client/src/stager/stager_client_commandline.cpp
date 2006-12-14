/*
 * $Id: stager_client_commandline.cpp,v 1.2 2006/12/14 14:50:00 itglp Exp $
 *
 * Copyright (C) 2004-2006 by CERN/IT/FIO/FD
 * All rights reserved
 */

/* ============== */
/* System headers */
/* ============== */
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>
#include <sys/types.h>
#include <grp.h>
#if !defined(_WIN32)
#include <unistd.h>
#else
#include "pwd.h"	// For getuid(), getgid()
#endif

/* ============= */
/* Local headers */
/* ============= */
#include "serrno.h"
#include "Cglobals.h"
#include "Csnprintf.h"
#include "stager_api.h"
#include "stager_mapper.h"
#include "stager_client_api_common.hpp"
#include "stager_client_commandline.h"

EXTERN_C char DLL_DECL *getconfent _PROTO((char *, char *, int));


/* This function was in RfioTURL, now it's imported here after the DPM merge.
   Check the header file for its explanation.
*/

int DLL_DECL getDefaultForGlobal (
                      char** host,
                      int* port,
                      char** svcClass,
                      int* version)
{ 
	char *hostMap, *hostDefault, *svcMap, *svcDefault;
	int versionMap,versionDefault, portDefault, ret;
	char* aux=NULL; 
  struct group* grp=NULL; 
	gid_t gid;
  
  hostMap=hostDefault=svcMap=svcDefault=NULL;
  versionMap=versionDefault=portDefault=ret=0;
  
	if(host == NULL || port == NULL || svcClass == NULL || version == NULL)
    { return (-1); }
	hostDefault=*host;
	svcDefault=*svcClass;
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
	if (*svcClass==NULL || strcmp(*svcClass,"")){*svcClass=svcDefault;}
	if (version==NULL || *version<=0){*version=versionDefault;}
  
	return (1);
}
