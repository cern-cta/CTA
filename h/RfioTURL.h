/*
 *
 * Copyright (C) 2003 by CERN/IT/ADC/CA
 * All rights reserved
 *
 * @(#)$RCSfile: RfioTURL.h,v $ $Revision: 1.4 $ $Release$ $Date: 2006/04/07 10:14:31 $ $Author: gtaur $
 *
 *
 *
 * @author Olof Barring
 */

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
 *           
 *            rfio://STAGE_HOST:STAGE_PORT//castor/cern.ch/user/n/nobody/abc.123?
 *                     svcClass=myClass
 *                      &castorVersion=2
 *            SvcClass and CastorVersion can be undefined  and default values are used.
 *  
 *   - RFIO path: /castor/cern.ch/user/n/nobody/abc.123
 *
 * - Remote file on a windows file server (shows the importance of the '/' delimiter)
 *   - TURL: rfio://pcwin32/c:\temp\abc.123
 *   - RFIO path: pcwin32:c:\temp\abc.123
 *
 */

#ifndef _RFIOTURL_H 
#define _RFIOTURL_H 1

#define DEFAULT_RFIO_TURL_PREFIX "rfio://"
#define RFIO_PROTOCOL_NAME "rfio"

#include <Castor_limits.h>
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


/** RFIO TURL structure */

typedef struct RfioTURL
{
  char rfioProtocolName[10]; /**< Should always be "rfio" */
  unsigned long rfioPort;    /**< The RFIO port number */
  char rfioHostName[CA_MAXHOSTNAMELEN+1]; /**< The rfio server host */
  char rfioPath[CA_MAXPATHLEN+1]; /**< The remote path */

} RfioTURL_t;



EXTERN_C int DLL_DECL getDefaultForGlobal _PROTO((
				char** host,
				int* port,
				char** svc,
				int* version));


/** Build a RFIO TURL structure from a string representation
 *
 * @param tURLString - the string to be parsed
 * @param rfioTURL - output RFIO TURL structure
 *
 * Parses the passed tURLString parameter and sets the corresponding
 * fields in the output rfioTURL structure.
 *
 * @return 0 == OK, -1 parsing failed. On failure the serrno global is set
 * to one of following values:
 * - EINVAL either the TURL PREFIX did not match the RFIO 
 *   TURL prefix or the tURLString format was wrong
 * - E2BIG The protocol name, host name or remote path was too long
 */

EXTERN_C int DLL_DECL rfioTURLFromString _PROTO((
                                                 char *tURLString,
                                                 RfioTURL_t *rfioTURL
                                                 ));


/**************************************************************************
 *                                                                        *
 *    All this function are not used anymore and they will be deleted.    *
 *                                                                        *
 *************************************************************************/   


/* * Initialize a new RFIO TURL prefix
 *
 * @param prefix - the new prefix
 *
 * Sets the (thread-specific) RFIO prefix. The default RFIO
 * prefix is defined by the macro DEFAULT_RFIO_TURL_PREFIX to "rfio://".
 * This routine should only be called if a different RFIO TURL
 * prefix is required.
 *
 * @return 0 == OK, -1 initialisation of (thread specific) memory failed
 */

/* EXTERN_C int DLL_DECL initRfioTURLPrefix _PROTO((
                                                 char *prefix
                                                 ));

/** Get the current RFIO TURL prefix
 *
 * Returns a character pointer to the current RFIO TURL in use.
 *
 * @see initRfioTURLPrefix()
 */
/* EXTERN_C char DLL_DECL *getRfioTURLPrefix _PROTO((
                                         void
                                         ));


/** Build a RFIO TURL string representation
 *
 * @param rfioTURL - RFIO TURL structure
 * @param tURLString - output RFIO TURL string allocated by caller
 * @param len - lenght of the passed string array
 * 
 * This routine is dual to rfioTURLFromString(). It builds
 * a string representation of the RFIO TURL.
 *
 * @return 0 == OK, -1 if a string representation could not be built. In
 * case of failure the serrno global is set to one of following values:
 * - EINVAL if rfioTURL or tURLString are NULL or len is negative
 * - E2BIG the string representation exceeds the passed len parameter
 */
/*EXTERN_C int DLL_DECL rfioTURLToString _PROTO((
                                               RfioTURL_t *rfioTURL,
                                               char *tURLString,
                                               int len
                                               ));

/** Builds a RFIO TURL from the input RFIO path
 *  
 * @param rfioPath - input RFIO path
 * @param rfioTURL - output RFIO TURL structure
 *
 * This routine builds a RFIO TURL structure given the input RFIO
 * path.
 *
 * @return 0 == OK, -1 if a string representation could not be built. In
 * case of failure the serrno global is set to one of following values:
 * - EINVAL if rfioPath or rfioTURL are NULL 
 * - E2BIG The protocol name, host name or remote path was too long
 *
 */
/*
EXTERN_C int DLL_DECL rfioPathToTURL _PROTO((
                                             char *rfioPath,
                                             RfioTURL_t *rfioTURL
                                             ));
*/

#endif // _RFIOTURL_H
