/*
 * $Id: stager_client_commandline.h,v 1.5 2009/01/14 17:38:21 sponcec3 Exp $
 *
 * Internal header file for command line stager clients and for the RFIO plugin.
 */

#ifndef STAGER_CLIENT_COMMANDLINE
#define STAGER_CLIENT_COMMANDLINE

#include <sys/types.h>
#include "osdep.h"
#include "stager_api.h"

#define DEFAULT_PROTOCOL "rfio"
#define ERRBUFSIZE 255

/**
 * getDefaultForGlobal
 *
 * if *host or *svcClass are null or empty strings retrieve 
 * the values, the same if *port is <= 0.
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
 */
EXTERN_C int getDefaultForGlobal _PROTO((
  char** host, int* port, char** svcClass));


/* Common functions for command line executables */
EXTERN_C int parseCmdLine _PROTO((int argc, char *argv[], int (*cb)(const char *),
                                           char** service_class, char** usertag, int* display_reqid));
EXTERN_C int printFileResponses _PROTO((int nbresps, struct stage_fileresp* responses)); 
EXTERN_C int printPrepareResponses _PROTO((int nbresps, struct stage_prepareToGet_fileresp *responses));
EXTERN_C void printReceivedResponses _PROTO((int nbresps));

#endif  /* STAGER_CLIENT_COMMANDLINE */
