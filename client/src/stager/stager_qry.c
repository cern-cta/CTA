/*
 * $Id: stager_qry.c,v 1.6 2005/06/21 14:40:57 sponcec3 Exp $
 */

/*
 * Copyright (C) 2005 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stager_qry.c,v $ $Revision: 1.6 $ $Date: 2005/06/21 14:40:57 $ CERN IT-FIO/DS Benjamin Couturier";
#endif /* not lint */

#include <stdio.h>
#include <stdlib.h>
#include "stager_api.h"
#include "serrno.h"
#include "Cgetopt.h"

#define BUFSIZE 200
struct cmd_args {
  int nbreqs;
  struct stage_query_req *requests;
  struct stage_options opts;
};


static struct Coptions longopts[] =
  {
    {"hsm_filename",       REQUIRED_ARGUMENT,  NULL,      'M'},
    {"fileid",             REQUIRED_ARGUMENT,  NULL,      'F'},
    {"usertag",            REQUIRED_ARGUMENT,  NULL,      'U'},
    {"requestid",          REQUIRED_ARGUMENT,  NULL,      'r'},
    {"help",               NO_ARGUMENT,        NULL,      'h'},
    {NULL,                 0,                  NULL,        0}
  };



void usage _PROTO((char *));
int cmd_countArguments(int argc, char *argv[]);
int cmd_parse(int argc, char *argv[], struct cmd_args *args);






int
main(int argc, char *argv[]) {
  struct cmd_args args;
  struct  stage_filequery_resp *responses;
  int nbresps, rc, errflg, i;
  char errbuf[BUFSIZE];

  args.opts.stage_host = NULL;
  args.opts.service_class = NULL;

  if ((args.nbreqs = cmd_countArguments(argc, argv)) <= 0) {
    usage(argv[0]);
    exit(EXIT_FAILURE);
  }

  create_query_req(&(args.requests), args.nbreqs);

  errflg =  cmd_parse(argc, argv, &args);
  if (errflg != 0) {
    usage (argv[0]);
    exit (EXIT_FAILURE);
  }


  /* Setting the error buffer */
  stage_seterrbuf(errbuf, sizeof(errbuf));

  /* Actual call to prepareToGet */
  rc = stage_filequery(args.requests,
                       args.nbreqs,
                       &responses,
                       &nbresps,
                       &(args.opts));

  if (rc < 0) {
    fprintf(stderr, "Error: %s\n", sstrerror(serrno));
    fprintf(stderr, "%s\n", errbuf);
    exit(EXIT_FAILURE);
  }

  printf("Received %d responses\n", nbresps);

  for (i=0; i<nbresps; i++) {
    printf("%s %s",
           responses[i].filename,
           stage_fileStatusName(responses[i].status));
    if (responses[i].errorCode != 0) {
      printf(" Error %d/%s (%s)",
             responses[i].errorCode,
             sstrerror(responses[i].errorCode),
             responses[i].errorMessage);
    }
    printf ("\n");
  }

  free_query_req(args.requests, args.nbreqs);
  free_filequery_resp(responses, nbresps);

}



int
cmd_parse(int argc, char *argv[], struct cmd_args *args) {
  int nbargs, Coptind, Copterr, errflg;
  char c;

  Coptind = 1;
  Copterr = 1;
  errflg = 0;
  nbargs = 0;
  while ((c = Cgetopt_long (argc, argv, "M:F:U:r:h", longopts, NULL)) != -1) {
    switch (c) {
    case 'M':
      args->requests[nbargs].type = BY_FILENAME;
      args->requests[nbargs].param = (char *)strdup(Coptarg);
      nbargs++;;
      break;
    case 'F':
      args->requests[nbargs].type = BY_FILEID;
      args->requests[nbargs].param = (char *)strdup(Coptarg);
      nbargs++;;
      break;
    case 'U':
      args->requests[nbargs].type = BY_USERTAG;
      args->requests[nbargs].param = (char *)strdup(Coptarg);
      nbargs++;
      break;
    case 'r':
      args->requests[nbargs].type = BY_REQID;
      args->requests[nbargs].param = (char *)strdup(Coptarg);
      nbargs++;
      break;
    case 'h':
    default:
      errflg++;
      break;
    }
    if (errflg != 0) break;
  }

  return errflg;
}

/**
 * Just count the number of args so as to prepare stage_fileQuery argument list
 */
int
cmd_countArguments(int argc, char *argv[]) {
  int Coptind, Copterr, errflg, nbargs;
  char c;

  Coptind = 1;
  Copterr = 1;
  errflg = 0;
  nbargs = 0;
  while ((c = Cgetopt_long (argc, argv, "M:F:U:r:h", longopts, NULL)) != -1) {
    switch (c) {
    case 'M':
    case 'F':
    case 'U':
    case 'r':
      nbargs++;
      break;
    case 'h':
    default:
      errflg++;
      break;
    }
    if (errflg != 0) break;
  }

  if (errflg)
    return -1;
  else
    return nbargs;
}



void
usage(char *cmd) {
  fprintf (stderr, "usage: %s ", cmd);
  fprintf (stderr, "%s",
           "[-M hsmfile [-M ...]] [-F fileid@nshost] [-U usertag] [-r requestid] [-h]\n");
}
