/*
 *
 */

/*
 * Copyright (C) 2005 by CERN/IT/ADC/CA Benjamin Couturier
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "";
#endif /* not lint */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stager_api.h"
#include "serrno.h"
#include "Cgetopt.h"
#include "RfioTURL.h"

struct cmd_args {
  int nbreqs;
  struct stage_prepareToPut_filereq* reqs;
  char *protocol;
  char *usertag;
  int display_reqid;
  struct stage_options opts;
};

static struct Coptions longopts[] =
  {
    {"filename",      REQUIRED_ARGUMENT,  NULL,      'M'},
    {"svc_class",     REQUIRED_ARGUMENT,  NULL,      'S'},
    {"usertag",       REQUIRED_ARGUMENT,  NULL,      'U'},
    {"display_reqid", NO_ARGUMENT,        NULL,      'r'},
    {"help",          NO_ARGUMENT,        NULL,      'h'},
    {NULL,            0,                  NULL,        0}
  };



void usage _PROTO((char *));
int cmd_parse(int argc, char *argv[], struct cmd_args *args);
int cmd_countHsmFiles(int argc, char *argv[]);

#define ERRBUFSIZE 255

int
main(int argc, char *argv[]) {
  struct cmd_args args;
  struct stage_prepareToPut_fileresp *response;
  int nbresps,ret;
  char *reqid;
  char errbuf[ERRBUFSIZE+1];
  int errflg, rc,  i;
  
  args.opts.stage_host = NULL;
  args.opts.service_class = NULL;
  args.opts.stage_version=0;
  args.opts.stage_port=0;
  
 /* Parsing the command line */
  memset(&errbuf,  '\0', sizeof(errbuf));

  errflg =  cmd_parse(argc, argv, &args);
  if (errflg != 0) {
    usage (argv[0]);
    exit (EXIT_FAILURE);
  }


  /* Setting the error buffer */
  stage_seterrbuf(errbuf, sizeof(errbuf));

  ret=getDefaultForGlobal(&args.opts.stage_host,&args.opts.stage_port,&args.opts.service_class,&args.opts.stage_version);

  /* Performing the actual call */
  rc = stage_prepareToPut(args.usertag,
                          args.reqs,
                          args.nbreqs,
                          &response,
                          &nbresps,
                          &reqid,
                          &(args.opts));

  if (rc < 0) {
    fprintf(stderr, "Error: %s\n", sstrerror(serrno));
    fprintf(stderr, "<%s>\n", errbuf);
    exit(1);
  }

  if (response == NULL) {
    fprintf(stderr, "Error: Response object is NULL\n");
    exit(1);
  }

  if (args.display_reqid) {
    printf("%s\n", reqid);
  }

  for (i=0; i<nbresps; i++) {
    printf("%s %s",
           response[i].filename,
           stage_statusName(response[i].status));
    if (response[i].errorCode != 0) {
      printf(" %d %s",
             response[i].errorCode,
             response[i].errorMessage);
    }
    printf ("\n");

  }

}



int
cmd_parse(int argc, char *argv[], struct cmd_args *args) {
  int nbfiles, Coptind, Copterr, errflg;
  char c;

  /* Reinitializing the structure with null pointers */
  memset(args, '\0', sizeof(struct cmd_args));

  /* Counting the number of HSM files */
  if ((args->nbreqs = cmd_countHsmFiles(argc, argv)) < 0) {
    return -1;
  }

  if (args->nbreqs > 0) {
    /* Creating the structure for files */
    create_prepareToPut_filereq(&(args->reqs), args->nbreqs);
  }

  /* Now parsing the command line */
  Coptind = 1;
  Copterr = 1;
  errflg = 0;
  nbfiles = 0;
  while ((c = Cgetopt_long
          (argc, argv, "M:S:U:rh", longopts, NULL)) != -1) {
    switch (c) {
    case 'M':
      args->reqs[nbfiles].filename = Coptarg;
      nbfiles++;
      break;
    case 'U':
      if (args->usertag!= NULL) {
        errflg++;
      } else {
        args->usertag = Coptarg;
      }
      break;
    case 'S':
      if (args->opts.service_class!= NULL) {
        errflg++;
      } else {
        args->opts.service_class = Coptarg;
      }
      break;
    case 'r':
      args->display_reqid = 1;
      break;
    case 'h':
    default:
      errflg++;
      break;
    }
    if (errflg != 0) break;
  }
  if (args->nbreqs == 0) errflg++;
  return errflg;
}


/**
 * Just count the number of args so as to prepare stage_fileQuery argument list
 */
int
cmd_countHsmFiles(int argc, char *argv[]) {
  int Coptind, Copterr, errflg, nbargs;
  char c;

  Coptind = 1;
  Copterr = 1;
  errflg = 0;
  nbargs = 0;
  while ((c = Cgetopt_long (argc, argv, "M:S:U:rh", longopts, NULL)) != -1) {
    switch (c) {
    case 'M':
      nbargs++;;
      break;
    default:
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
           "-M hsmfile [-M hsmfile ...] [-S svcClass] [-U usertag] [-r] [-h]\n");
}
