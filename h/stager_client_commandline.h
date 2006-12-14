/*
 * $Id: stager_client_commandline.h,v 1.1 2006/12/14 13:40:52 itglp Exp $
 *
 * Internal header file for command line stager clients and for the RFIO plugin.
 */

#ifndef STAGER_CLIENT_COMMANDLINE
#define STAGER_CLIENT_COMMANDLINE

#include <sys/types.h>
#include "osdep.h"

/**
 * getDefaultForGlobal
 *
 * if *host or *svcClass are null or empty strings retrieve 
 * the values, the same if *port and *version are <= 0.
 * 
 * To retrieve the values it looks if:
 * - enviroment variables are set
 * - or stager_mapper has defined values
 * - or castor.conf provides them
 * - or it uses default hard coded values,
 *   defined in stager_client_api_common.hpp                                                                               *
 *
 * @param host       stager (RH) host
 * @param port       stager port
 * @param svcClass   service class
 * @param version    castor version
 */
EXTERN_C int DLL_DECL getDefaultForGlobal _PROTO((
  char** host, int* port, char** svcClass, int* version));

#endif  /* STAGER_CLIENT_COMMANDLINE */
