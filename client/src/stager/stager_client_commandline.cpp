/*
 * $Id: stager_client_commandline.cpp,v 1.14 2009/01/14 17:33:32 sponcec3 Exp $
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
#include <unistd.h>

/* ============= */
/* Local headers */
/* ============= */
#include "serrno.h"
#include "Cglobals.h"
#include "Csnprintf.h"
#include "stager_mapper.h"
#include "stager_client_api_common.hpp"
#include "stager_client_commandline.h"
#include "Cgetopt.h"
#include "getconfent.h"
#include <fstream>
#include <string>


/********************************************************************************************************************
 * This Function:                                                                                                   *
 * if *host or *svc are null or empty strings retrieve the values, the same if *port <= 0.                          *
 * To retrive the values it looks if:                                                                               *
 * enviroment variables are set or stager_mapper as values defined or castor.conf or (if it doesn't have valid)     *
 * it uses default hard coded values.                                                                               *
 *                                                                                                                  *
 *******************************************************************************************************************/

int DLL_DECL getDefaultForGlobal(
                                 char** host,
                                 int* port,
                                 char** svc)
{
  char *hostMap, *hostDefault, *svcMap, *svcDefault;
  int portDefault, ret;
  char* aux=NULL;
  struct group* grp=NULL;
  char* security=NULL;
  gid_t gid;

  hostMap = hostDefault = svcMap = svcDefault = NULL;
  portDefault = ret = 0;

  if (host == NULL || port == NULL || svc == NULL) {
    return (-1);
  }
  if (*host) hostDefault = strdup(*host);
  else hostDefault = NULL;

  if (*svc) svcDefault = strdup(*svc);
  else svcDefault = NULL;

  portDefault = *port;

  gid = getgid();
  grp = getgrgid(gid);

  if (grp != NULL) {
    ret = just_stage_mapper(getenv("USER"), grp->gr_name, &hostMap, &svcMap);
  } else {
    ret = just_stage_mapper(getenv("USER"), NULL, &hostMap, &svcMap);
  }

  if (hostDefault == NULL || strcmp(hostDefault, "") == 0) {
    if (hostDefault != NULL) {
      free(hostDefault);
      hostDefault=NULL;
    }
    aux=  getenv("STAGE_HOST");
    hostDefault = aux == NULL ? NULL : strdup(aux);
    if (hostDefault == NULL || strcmp(hostDefault, "") == 0) {
      if (hostDefault != NULL) {
        free(hostDefault);
        hostDefault = NULL;
      }
      if (hostMap == NULL || strcmp(hostMap, "") == 0) {
        aux = (char*)getconfent("STAGER", "HOST", 0);
        hostDefault = aux == NULL ? NULL : strdup(aux);
        if (hostDefault == NULL || strcmp(hostDefault,"") == 0) {
          if (hostDefault != NULL) {
            free(hostDefault);
            hostDefault = NULL;
          }
        }
      } else {
        if (hostDefault != NULL) {
          free(hostDefault);
          hostDefault = NULL;
        }
        hostDefault = strdup(hostMap);
      }
    }
  }

  if (svcDefault == NULL || strcmp(svcDefault, "") == 0) {
    if (svcDefault != NULL) {
      free(svcDefault);
      svcDefault = NULL;
    }
    aux = getenv("STAGE_SVCCLASS");
    svcDefault = aux == NULL ? NULL : strdup(aux);
    if (svcDefault == NULL || strcmp(svcDefault, "") == 0) {
      if (svcDefault != NULL && strcmp(svcDefault, "")) {
        free(svcDefault);
        svcDefault = NULL;
      }
      if (svcMap == NULL || strcmp(svcMap, "") == 0) {
        aux = (char*)getconfent("STAGER", "SVCCLASS", 0);
        svcDefault = aux == NULL ? NULL : strdup(aux);
        if (svcDefault == NULL || strcmp(svcDefault, "") == 0) {
          if (svcDefault != NULL) {
            free(svcDefault);
            svcDefault = NULL;
          }
          svcDefault = strdup(DEFAULT_SVCCLASS);
        }
      } else {
        if (svcDefault != NULL && strcmp(svcDefault, "")) {
          free(svcDefault);
          svcDefault = NULL;
        }
        svcDefault = strdup(svcMap);
      }
    }
  }

  if (portDefault <= 0) {
    if ((security = getenv ("SECURE_CASTOR")) != 0 && strcasecmp(security, "YES") == 0) {
      aux = getenv("STAGE_SEC_PORT");
      portDefault = aux == NULL ? 0 : atoi(aux);
      if (portDefault <= 0) {
        aux = (char*)getconfent("STAGER", "SEC_PORT", 0);
        portDefault = aux == NULL ? 0 : atoi(aux);
        if (portDefault <= 0) {
          portDefault = DEFAULT_SEC_PORT;
        }
      }
    } else {
      aux = getenv("STAGE_PORT");
      portDefault = aux == NULL ? 0 : atoi(aux);
      if (portDefault <= 0) {
        aux = (char*)getconfent("STAGER", "PORT", 0);
        portDefault = aux == NULL ? 0 : atoi(aux);
        if (portDefault <= 0) {
          portDefault = DEFAULT_PORT;
        }
      }
    }
  }

  if ((*host == NULL || strcmp(*host, "")) && (hostDefault != NULL)) {
    *host = strdup(hostDefault);
  }
  if (port == NULL || *port <= 0) {
    *port = portDefault;
  }
  if (*svc == NULL || strcmp(*svc, "")) {
    *svc = strdup(svcDefault);
  }

  return (1);
}

extern "C" {

/* command line parser for a generic stager command line client */
int DLL_DECL parseCmdLine(int argc, char *argv[], int (*callback)(const char *),
                          char** service_class, char** usertag, int* display_reqid)
{
  int nargs, Coptind, Copterr, errflg;
  char c;
  static struct Coptions longopts[] =
    {
      {"filename",      REQUIRED_ARGUMENT,  NULL,      'M'},
      {"filelist",      REQUIRED_ARGUMENT,  NULL,      'f'},
      {"service_class", REQUIRED_ARGUMENT,  NULL,      'S'},
      {"usertag",       REQUIRED_ARGUMENT,  NULL,      'U'},
      {"display_reqid", NO_ARGUMENT,        NULL,      'r'},
      {"help",          NO_ARGUMENT,        NULL,      'h'},
      {NULL,            0,                  NULL,        0}
    };

  nargs = argc;
  Coptind = 1;
  Copterr = 1;
  errflg = 0;

  while ((c = Cgetopt_long (argc, argv, "f:M:S:U:rh", longopts, NULL)) != -1) {
    switch (c) {
    case 'M':
      if (0 != callback) {
        callback(Coptarg);
      }
      break;
    case 'f':
      {
        // loop over lines of the input file
        std::ifstream fin(Coptarg);
        if (!fin) {
          fprintf (stderr, "unable to read file %s\n", Coptarg);
          errflg++;
          break;
        }
        std::string s;
        while (getline(fin,s)) {
          callback(s.c_str());
        }
      }
      break;
    case 'S':
      *service_class = Coptarg;
      break;
    case 'U':
      *usertag = Coptarg;
      break;
    case 'r':
      *display_reqid = 1;
      break;
    case 'h':
      errflg++;
      break;
    default:
      errflg++;
      break;
    }
    if (errflg != 0) break;
  }

  return errflg;
}
}

void DLL_DECL printReceivedResponses(int nbresps) {
  // this piece of code is essentially copied from the stage_trace function
  // the call to replace this function should be :
  //   stage_trace(1, "Received %d responses", nbresps);
  // However, for backward compatibility reasons, the format of
  // stage_trace is not suitable, so we log by hand, still respecting
  // the log level
  struct stager_client_api_thread_info *thip;
  if (0 == stage_apiInit(&thip)) {
    if (((int*)thip->trace)[1] >= 1) {
      fprintf(stdout, "Received %d responses\n", nbresps);
    }
  }
}

int DLL_DECL printFileResponses(int nbresps, struct stage_fileresp *responses) {
  int i;
  if (responses == NULL) {
    fprintf(stderr, "Error: Response object is NULL\n");
    exit(EXIT_FAILURE);
  }

  int rc = EXIT_SUCCESS;
  printReceivedResponses(nbresps);

  for (i=0; i<nbresps; i++) {
    printf("%s %s",
           responses[i].filename,
           stage_statusName(responses[i].status));
    if (responses[i].errorCode != 0) {
      printf(" %d %s",
             responses[i].errorCode,
             responses[i].errorMessage);
      rc = EXIT_FAILURE;
    }
    printf ("\n");
  }

  return rc;
}

int DLL_DECL printPrepareResponses(int nbresps, struct stage_prepareToGet_fileresp *responses) {
  int i;
  if (responses == NULL) {
    fprintf(stderr, "Error: Response object is NULL\n");
    exit(EXIT_FAILURE);
  }

  int rc = EXIT_SUCCESS;
  printReceivedResponses(nbresps);

  for (i=0; i<nbresps; i++) {
    printf("%s %s",
           responses[i].filename,
           stage_statusName(responses[i].status));
    if (responses[i].errorCode != 0) {
      printf(" %d %s",
             responses[i].errorCode,
             responses[i].errorMessage);
      rc = EXIT_FAILURE;
    }
    printf ("\n");
  }

  return rc;
}

