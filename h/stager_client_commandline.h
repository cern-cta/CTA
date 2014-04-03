/*
 * $Id: stager_client_commandline.h,v 1.5 2009/01/14 17:38:21 sponcec3 Exp $
 *
 * Internal header file for command line stager clients and for the RFIO plugin.
 */

#pragma once

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
EXTERN_C int getDefaultForGlobal (
				  char** host, int* port, char** svcClass);


/* Common functions for command line executables */
EXTERN_C int parseCmdLine (int argc, char *argv[], int (*cb)(const char *),
			               char** service_class, char** usertag, int* display_reqid);
EXTERN_C int putDone_parseCmdLine (int argc, char *argv[], int (*cb)(const char *),
			                       char** service_class, char** reqid);
EXTERN_C int printFileResponses (int nbresps, struct stage_fileresp* responses); 
EXTERN_C int printPrepareResponses (int nbresps, struct stage_prepareToGet_fileresp *responses);
EXTERN_C void printReceivedResponses (int nbresps);

