/*
 *
 * Copyright (C) 2003 by CERN/IT/ADC/CA
 * All rights reserved
 *
 * @(#)$RCSfile: RfioTURL.c,v $ $Revision: 1.4 $ $Release$ $Date: 2006/03/22 11:26:14 $ $Author: gtaur $
 *
 *
 *
 * @author Olof Barring
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: RfioTURL.c,v $ $Revision: 1.4 $ $Release$ $Date: 2006/03/22 11:26:14 $ Olof Barring";
#endif /* not lint */
/** RfioTURL.c - RFIO TURL handling
 *
 * Auxiliary routines for handling the RFIO TURL convention:
 * <BR><CODE>
 * rfio://[hostname][:port]/path
 * </CODE><P>
 * Examples:
 * - Physical (remote) disk file following the SHIFT/CASTOR "NFS" convention:
 *   - TURL: rfio://pub001d//shift/pub001d/data01/c3/stage/abc.123
 *   - RFIO path: /shift/pub001d/data01/c3/stage/abc.123
 * - Physical (remote) disk file, standard format:
 *   - TURL: rfio://wacdr002d//tmp/abc.123
 *   - RFIO path: wacdr002d:/tmp/abc.123
 * - CASTOR file:
 *   - TURL: rfio:////castor/cern.ch/user/n/nobody/abc.123 or 
 *           rfio://STAGE_HOST:STAGE_PORT/?
 *                     svcClass=myClass
 *                      &castorVersion=2
 *                       &path=/castor/cern.ch/user/n/nobody/abc.123
 *           rfio://STAGE_HOST:STAGE_PORT//castor/cern.ch/user/n/nobody/abc.123?
 *                     svcClass=myClass
 *                      &castorVersion=2
 *
 *           SvcClass and CastorVersion can be undefined  and default values are used.  
 *
 *   - RFIO path: /castor/cern.ch/user/n/nobody/abc.123
 *
 * - Remote file on a windows file server (shows the importance of the '/' delimiter)
 *   - TURL: rfio://pcwin32/c:\temp\abc.123
 *   - RFIO path: pcwin32:c:\temp\abc.123
 *
 */

#include <RfioTURL.h>
#include <stager_mapper.h>



static int tURLPrefixKey = -1;
static int tURLPrefixLenKey = -1;

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
 * This Function is parsing the string and returning the Turl struct that will be used by parse.c.                  *
 * Also it sets STAGE_HOST, STAGE_PORT and STAGE_SVCCLASS enviroment variables that are used in the other functions,*
 * and eventually it overwrite that if they are already set in the enviroment.                                      *  
 * It's not a clean solution ... but we couldn't do aything else better without changing the interface... :(.       * 
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
  path1=path2=mySvcClass=myCastorVersion=NULL;
  char* buff;
  char* svcDefault=0;
  char* hostDefault=0;
  int versionDefault=0;

  if ( tURLStr == NULL || tURL == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  prefix = getRfioTURLPrefix();
  if ( prefix == NULL ) return(-1);	

  if ( strstr(tURLStr,prefix) != tURLStr ) {
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
  if ( q == NULL ) {
    serrno = EINVAL;
    free(orig);
    return(-1);
  }

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


  p = q+1;

  if (!p){

   // the path should be specified ... it cannot be missing

      serrno = EINVAL;
      free(orig);
      return(-1);
  }

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


   if (path1 && path2){

     // not possible to have the path twice
      serrno = EINVAL;
      free(orig);
      return(-1);
   }


   if (!path1 && !path2){

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

   if (strstr(_tURL.rfioPath,"/castor")){

    /* from here setting the proper enviroment variable */
    /* getting default value that can be used */

	svcDefault=malloc(sizeof(tURL->rfioPath));
	strcpy(svcDefault,"");
	hostDefault=malloc(sizeof(tURL->rfioHostName));
	strcpy(hostDefault,"");
	// I set the default values taken from stagermap.conf
	ret=stage_mapper_setenv(getenv("USER"),NULL,&hostDefault,&svcDefault,&versionDefault); 
 	
  	if(strcmp(_tURL.rfioHostName,"")){ // overwrite stage_host if I have a value in the turl
      		 if (setenv("STAGE_HOST",_tURL.rfioHostName,1)){
                // setenv failed
                  serrno = EINVAL;
                  free(orig);
		  free(svcDefault);
		  free(hostDefault);
                  return(-1);
        	}
     	}
    	if (_tURL.rfioPort){  
		buff=malloc(sizeof("STAGE_PORT")+6); // a port number is made of 5 digit max if converted to string
      		sprintf(buff,"%d",_tURL.rfioPort);     
       		if (setenv("STAGE_PORT",buff,1)){ // overwrite in case of value given by Turl
               		 // setenv failed
               		  serrno = EINVAL;
			  free(svcDefault);
		  	  free(hostDefault);
                	  free(buff);
			  free(orig);
                	  return(-1);

        	}
		free(buff); 
     	}
    	if (mySvcClass && strcmp(mySvcClass,"")){ // overwrite in case of value given by Turl
	
       		 if (setenv("STAGE_SVCCLASS",mySvcClass,1)){
                	// setenv failed
               		   serrno = EINVAL;
                	   free(orig);
			   free(svcDefault);
		 	   free(hostDefault);
                  	   return(-1);

        	}
     	}
   	
     	if (myCastorVersion){ // overwrite in case of value given by Turl
      		 if (!strcmp(myCastorVersion,"2")){
       			 if (setenv("RFIO_USE_CASTOR_V2","YES",1)){
             			   // setenv failed
                  		serrno = EINVAL;
                 		free(orig);
		  		free(svcDefault);
		  		free(hostDefault);
                  		return(-1);

        		}
       		}
     	} 
    	free(svcDefault);
     	free(hostDefault);	
     }
     
     free(orig);
  /*
   * Everything OK. Copy the temporary structure to the output
   */
    *tURL = _tURL;

     return(0);
}


/********************************UNUSED FUNCTIONS TO BE DELETED ********************************************
 *                                                                                                         * 
 *  All the functions from here to the end are not used anymore in Castor code and they should be deleted  *   
 *  soon. In SRM are used fuctions with the same name but the implementation is in RfioTURL.c of SRM       *
 *  folder. That file is has not been touched.                                                             *
 *                                                                                                         *
 **********************************************************************************************************/

/*

EXTERN_C int DLL_DECL rfio_parse _PROTO((char *, char **, char **));

int initRfioTURLPrefix(char *prefix) 
{
  char *str = NULL;
  int rc, len, *tURLLen = NULL;
  if ( prefix == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  
  len = strlen(prefix)+1;
  if ( len < CA_MAXLINELEN + 1 ) len = CA_MAXLINELEN+1;
  rc = Cglobals_get(&tURLPrefixKey,(void *)&str,len);
  if ( rc == -1 || str == NULL ) return(-1);
  strcpy(str,prefix);

  rc = Cglobals_get(&tURLPrefixLenKey,(void *)&tURLLen,sizeof(int));
  if ( rc == -1 || tURLLen == NULL ) return(-1);
  *tURLLen = len;
  
  return(0);
}


int rfioTURLToString(RfioTURL_t *tURL, char *str, int len) 
{
  char *prefix, portStr[12];
  int _len = 0;

  if ( tURL == NULL || str == NULL || len <= 0 ) {
    serrno = EINVAL;
    return(-1);
  }

  prefix = getRfioTURLPrefix();
  if ( prefix != NULL ) _len = strlen(prefix);

  _len += strlen(tURL->rfioHostName);
  *portStr = '\0';
  if ( tURL->rfioPort > 0 ) {
    _len++; // for the ':' delimiter 
    sprintf(portStr,"%lu",tURL->rfioPort);
    _len += strlen(portStr);
  }
  _len++; // '/' between [host][:port] and path.
  _len += strlen(tURL->rfioPath);
  _len++; // terminating '\0'
  if ( len < _len ) {
    serrno = E2BIG;
    return(-1);
  }

  sprintf(str,"%s%s%s%s/%s",prefix,tURL->rfioHostName,(tURL->rfioPort > 0 ? ":" : ""),
     (tURL->rfioPort > 0 ? portStr : ""),tURL->rfioPath);  

  return(0);
}

int rfioPathToTURL(char *rfioPath, RfioTURL_t *tURL) 
{
  char *host = NULL, *path = NULL;
  char lhost[CA_MAXHOSTNAMELEN+1];
  RfioTURL_t _tURL;
  int rc;
  char* svc;

  if ( rfioPath == NULL || tURL == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  if ( (rc = rfio_parse(rfioPath,&host,&path)) == -1 ) return(-1);

  if ( path == NULL ) path = rfioPath;
  if ( strlen(path) >= sizeof(tURL->rfioPath) ) {
    serrno = E2BIG;
    return(-1);
  }
  strcpy(_tURL.rfioProtocolName,RFIO_PROTOCOL_NAME);
  _tURL.rfioPort = 0; // use whatever local default is set to
  if ( host == NULL ) {

    // Local file 
    if ( gethostname(lhost,sizeof(lhost)) == -1 ) return(-1);
    host = lhost;
    if ( strlen(host) >= sizeof(tURL->rfioHostName) ) {
      serrno = E2BIG;
      return(-1);
    }
    strcpy(_tURL.rfioHostName,host);
  } else if ( rc == 0 ) {
    // HSM (Castor) file 

    *_tURL.rfioHostName = '\0';

  } else {

    //  host specified  

    strcpy(_tURL.rfioHostName,host);
  }
   
  strcpy(_tURL.rfioPath,path);

  *tURL = _tURL;  
  return(0);
}


*/
