/*
 *
 * Copyright (C) 2003 by CERN/IT/ADC/CA
 * All rights reserved
 *
 * @(#)$RCSfile: RfioTURL.c,v $ $Revision: 1.3 $ $Release$ $Date: 2005/10/26 16:32:17 $ $Author: jdurand $
 *
 *
 *
 * @author Olof Barring
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: RfioTURL.c,v $ $Revision: 1.3 $ $Release$ $Date: 2005/10/26 16:32:17 $ Olof Barring";
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
 *   - TURL: rfio:////castor/cern.ch/user/n/nobody/abc.123
 *   - RFIO path: /castor/cern.ch/user/n/nobody/abc.123
 * - Remote file on a windows file server (shows the importance of the '/' delimiter)
 *   - TURL: rfio://pcwin32/c:\temp\abc.123
 *   - RFIO path: pcwin32:c:\temp\abc.123
 *
 */

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#ifndef _WIN32
#include <unistd.h>
#else
#include "net.h"
#endif
#include <ctype.h>
#include <errno.h>
#include <Castor_limits.h>
#include <osdep.h>
#include <serrno.h>
#include <Cglobals.h>
#include <rfio_api.h>
#include <RfioTURL.h>
EXTERN_C int DLL_DECL rfio_parse _PROTO((char *, char **, char **));

static int tURLPrefixKey = -1;
static int tURLPrefixLenKey = -1;

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

int rfioTURLFromString(
                       char *tURLStr,
                       RfioTURL_t *tURL
                       ) 
{
  char *p, *q, *q1, *orig, *prefix;
  RfioTURL_t _tURL;
  
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
   * Finally we only have the path left
   */
  p = q+1;
  if ( strlen(p) >= sizeof(tURL->rfioPath) ) {
    serrno = E2BIG;
    free(orig);
    return(-1);
  }
  strcpy(_tURL.rfioPath,p);
  free(orig);
  /*
   * Everything OK. Copy the temporary structure to the output
   */
  *tURL = _tURL;
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
    _len++; /* for the ':' delimiter */
    sprintf(portStr,"%lu",tURL->rfioPort);
    _len += strlen(portStr);
  }
  _len++; /* '/' between [host][:port] and path */  
  _len += strlen(tURL->rfioPath);
  _len++; /* terminating '\0' */
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
  _tURL.rfioPort = 0; /* use whatever local default is set to */
  if ( host == NULL ) {
    /* Local file */
    if ( gethostname(lhost,sizeof(lhost)) == -1 ) return(-1);
    host = lhost;
    if ( strlen(host) >= sizeof(tURL->rfioHostName) ) {
      serrno = E2BIG;
      return(-1);
    }
    strcpy(_tURL.rfioHostName,host);
  } else if ( rc == 0 ) {
    /* HSM (Castor) file */
    *_tURL.rfioHostName = '\0';
  } else {
    /* Normal remote file */
    strcpy(_tURL.rfioHostName,host);
  }
  
  strcpy(_tURL.rfioPath,path);

  *tURL = _tURL;  
  return(0);
}
