/*
 * $Id: stager_get.c,v 1.5 2005/07/06 05:55:24 jdurand Exp $
 */

/*
 * Copyright (C) 2005 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stager_get.c,v $ $Revision: 1.5 $ $Date: 2005/07/06 05:55:24 $ CERN IT-FIO/DS Benjamin Couturier";
#endif /* not lint */

#include <stdio.h>
#include <string.h>
#include "stager_api.h"
#include "serrno.h"
#include "Cgetopt.h"

#define BUFSIZE 1000
#define DEFAULT_PROTOCOL "rfio"


void usage _PROTO((char *));


/* Global flags */
static int display_reqid = 0;
static char* service_class = NULL;
static char* usertag = NULL;
static char* stage_host = NULL;

/* Global vars used by _fillStruct and _countFiles */
static int filenb; /* Number of files to be staged in */
static struct stage_prepareToGet_filereq *requests;
static char *protocol = DEFAULT_PROTOCOL;

/* Local functions */
int parseCmdLine(int argc, char *argv[], int (*cb)(char *) );
static int _countFiles(char *filename);
static int _fillStruct(char *filename);


int parseCmdLine(int argc, char *argv[], int (*cb)(char *) ) {
  int nargs, Coptind, Copterr, errflg;
  char c;
  static struct Coptions longopts[] =
    {
      {"filename",      REQUIRED_ARGUMENT,  NULL,      'M'},
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

  while ((c = Cgetopt_long (argc, argv, "M:S:U:rh", longopts, NULL)) != -1) {
    switch (c) {
    case 'M':
      cb(Coptarg);
      break;
    case 'S':
      service_class = Coptarg;
      break;
    case 'U':
      usertag = Coptarg;
      break;
    case 'r':
      display_reqid = 1;
      break;
    case 'h':
    case '?':
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

/* Uses the filenb global variable
   that should be set to 0 before 1st call */
static int _countFiles(char *filename) {
  filenb++;
}

/* Uses the filenb global variable,
   that should be set to 0 before 1st call */
/* Uses the requests global variable,
   which should already be initialized */
static int _fillStruct(char *filename) {
  requests[filenb].filename = (char *)strdup(filename);
  requests[filenb].protocol = (char *)strdup(protocol);
  requests[filenb].priority = 0;
  filenb++;
}

int main(argc, argv)
     int argc;
     char **argv;
{
  int errflg, total_nb_files, rc, nbresps, i, ret;
  char *reqid;
  struct stage_prepareToGet_fileresp *responses;
  char errbuf[BUFSIZE];
  struct stage_options opts;

  opts.stage_host = NULL;
  opts.service_class = NULL;


  usertag = NULL;

  filenb = 0;
  errflg =  parseCmdLine(argc, argv, _countFiles);
  if (errflg != 0 || filenb <= 0) {
    usage (argv[0]);
    exit (1);
  }
  total_nb_files = filenb;

  if (service_class) {
    opts.service_class = service_class;
  }

  if (stage_host) {
    opts.stage_host = stage_host;
  }


  /* Setting the error buffer and preparing the array of file requests */
  stage_seterrbuf(errbuf, sizeof(errbuf));
  create_prepareToGet_filereq(&requests, total_nb_files);

  /* Iterating over the command line again to fill in the array of requests */
  filenb = 0;
  errflg =  parseCmdLine(argc, argv, _fillStruct);

  /* Actual call to prepareToGet */
  rc = stage_prepareToGet(usertag,
                          requests,
                          total_nb_files,
                          &responses,
                          &nbresps,
                          &reqid,
                          &opts);

  if (rc < 0) {
    fprintf(stderr, "Error %s\n", sstrerror(serrno));
    fprintf(stderr, "<%s>\n", errbuf);
    exit(1);
  }

  printf("Received %d responses\n", nbresps);

  ret = 0;
  for (i=0; i<nbresps; i++) {
    printf("%s %s",
           responses[i].filename,
           stage_statusName(responses[i].status));
    if (responses[i].errorCode != 0) {
      printf(" %d %s",
             responses[i].errorCode,
             responses[i].errorMessage);
      ret = 1;
    }
    printf ("\n");
  }
  if (display_reqid) {
    fprintf(stdout, "Stager request ID: %s\n", reqid);
  }

  free_prepareToGet_filereq(requests, total_nb_files);
  free_prepareToGet_fileresp(responses, nbresps);

  return(ret);
}



void usage(cmd)
     char *cmd;
{
  fprintf (stderr, "usage: %s ", cmd);
  fprintf (stderr, "%s",
           "-M hsmfile [-M...] [-S service_class] [-U usertag] [-r] [-h]\n");

}
