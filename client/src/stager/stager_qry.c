/*
 * $Id: stager_qry.c,v 1.14 2006/04/07 10:14:31 gtaur Exp $
 */

/*
 * Copyright (C) 2005 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stager_qry.c,v $ $Revision: 1.14 $ $Date: 2006/04/07 10:14:31 $ CERN IT-FIO/DS Benjamin Couturier";
#endif /* not lint */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stager_api.h"
#include "serrno.h"
#include "Cgetopt.h"
#include "RfioTURL.h"

#define BUFSIZE 200
struct cmd_args {
  int nbreqs;
  struct stage_query_req *requests;
  struct stage_options opts;
};


static struct Coptions longopts[] =
  {
    {"hsm_filename",       REQUIRED_ARGUMENT,  NULL,      'M'},
    {"regexp",             REQUIRED_ARGUMENT,  NULL,      'E'},
    {"fileid",             REQUIRED_ARGUMENT,  NULL,      'F'},
    {"usertag",            REQUIRED_ARGUMENT,  NULL,      'U'},
    {"requestid",          REQUIRED_ARGUMENT,  NULL,      'r'},
    {"next",               NO_ARGUMENT,        NULL,      'n'},
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
  int nbresps, rc, errflg, i,ret;
  char errbuf[BUFSIZE];

  args.opts.stage_host = NULL;
  args.opts.service_class = NULL;
  args.opts.stage_version=0;
  args.opts.stage_port=0;

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

  ret=getDefaultForGlobal(&args.opts.stage_host,&args.opts.stage_port,&args.opts.service_class,&args.opts.stage_version);

  /* Actual call to fileQuery */
  rc = stage_filequery(args.requests,
                       args.nbreqs,
                       &responses,
                       &nbresps,
                       &(args.opts));

  if (rc < 0) {
    if(serrno != 0) {
      fprintf(stderr, "Error: %s\n", sstrerror(serrno));
    }
    fprintf(stderr, "%s\n", errbuf);
    exit(EXIT_FAILURE);
  }

  printf("Received %d responses\n", nbresps);

  for (i=0; i<nbresps; i++) {
    if (responses[i].errorCode == 0) {
      printf("%s %s %s",
            responses[i].castorfilename,
            responses[i].filename,
            stage_fileStatusName(responses[i].status));
    } else {
      printf("Error %d/%s (%s)",
            responses[i].errorCode,
            sstrerror(responses[i].errorCode),
            responses[i].errorMessage);
    }
    printf ("\n");
  }

  free_query_req(args.requests, args.nbreqs);
  free_filequery_resp(responses, nbresps);

  exit(EXIT_SUCCESS);
}



int
cmd_parse(int argc, char *argv[], struct cmd_args *args) {
  int nbargs, Coptind, Copterr, errflg, getNextMode;
  char c;

  Coptind = 1;
  Copterr = 1;
  errflg = 0;
  nbargs = 0;
  getNextMode = 0;
  while ((c = Cgetopt_long (argc, argv, "M:E:F:U:r:nh", longopts, NULL)) != -1) {
    switch (c) {
    case 'M':
      args->requests[nbargs].type = BY_FILENAME;
      args->requests[nbargs].param = (char *)strdup(Coptarg);
      nbargs++;
      break;
    case 'E':
      args->requests[nbargs].type = BY_FILENAME;
      args->requests[nbargs].param = (char *)malloc(strlen("regexp:") + strlen(Coptarg) + 1);
      if(args->requests[nbargs].param == NULL) {
	errflg++;
	break;
      }
      strcpy(args->requests[nbargs].param, "regexp:");
      strcat(args->requests[nbargs].param, Coptarg);
      nbargs++;
      break;
    case 'F':
      args->requests[nbargs].type = BY_FILEID;
      args->requests[nbargs].param = (char *)strdup(Coptarg);
      nbargs++;
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
    case 'n':
      getNextMode = 1;
      break;
    case 'h':
    default:
      errflg++;
      break;
    }
    if (errflg != 0) break;
  }
  
  if(getNextMode) {
    errflg++;
    for(c = 0; c < nbargs; c++) {
      if(args->requests[c].type == BY_REQID) {
        args->requests[c].type = BY_REQID_GETNEXT;
        errflg = 0;
      }
      else if(args->requests[c].type == BY_USERTAG) {
        args->requests[c].type = BY_USERTAG_GETNEXT;
        errflg = 0;
      }
    }
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
  while ((c = Cgetopt_long (argc, argv, "M:E:F:U:r:nh", longopts, NULL)) != -1) {
    switch (c) {
    case 'M':
    case 'E':
    case 'F':
    case 'U':
    case 'r':
      nbargs++;
      break;
    case 'n':
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
  fprintf (stderr, "%s", "[-M hsmfile [-M ...]] [-E regular_expression [-E ...]] [-F fileid@nshost] [-U usertag] [-r requestid] [-n] [-h]\n");
}
