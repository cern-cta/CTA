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
#include "stager_api.h"
#include "serrno.h"
#include "Cgetopt.h"

struct cmd_args {
  char *filename;
  char *protocol;
  char *usertag;
  int display_reqid;
  mode_t mode;
  u_signed64 size;
  struct stage_options opts;
};

static struct Coptions longopts[] =
  {
    {"protocol",           REQUIRED_ARGUMENT,  NULL,      'P'},
    {"migration_filename", REQUIRED_ARGUMENT,  NULL,      'M'},
    {"mode",               REQUIRED_ARGUMENT,  NULL,      'm'},
    {"size",               REQUIRED_ARGUMENT,  NULL,      's'},
    {"svc_class",          REQUIRED_ARGUMENT,  NULL,      'S'},
    {"usertag",            REQUIRED_ARGUMENT,  NULL,      'U'},
    {"display_reqid",      NO_ARGUMENT,        NULL,      'r'},
    {"host",               NO_ARGUMENT,        NULL,      'H'},
    {"help",               NO_ARGUMENT,        NULL,      'h'},
    {NULL,                 0,                  NULL,        0}
  };



void usage _PROTO((char *));
int cmd_parse(int argc, char *argv[], struct cmd_args *args);


#define ERRBUFSIZE 255

int 
main(int argc, char *argv[]) {
  struct cmd_args args;
  struct stage_io_fileresp *response;
  char *reqid;
  char errbuf[ERRBUFSIZE+1];
  int errflg, rc, ret = 1;

  /* Parsing the command line */
  memset(&errbuf,  '\0', sizeof(errbuf));
  errflg =  cmd_parse(argc, argv, &args);  
  if (errflg != 0) {
    usage (argv[0]);
    exit (EXIT_FAILURE);
  }

  /* Setting the error buffer */
  stage_seterrbuf(errbuf, sizeof(errbuf));

  /* Performing the actual call */
  rc = stage_put(args.usertag,
                 args.protocol,
                 args.filename,
                 args.mode,
                 args.size,
                 &response,
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

   if (response->errorCode != 0) {
     fprintf(stderr, "Error: %d (%s) Status: %s\n", 
	     response->errorCode,
	     response->errorMessage,
	     stage_statusName(response->status));
     ret = 1;
   } else {
     if (args.display_reqid) {
       fprintf(stdout, "%s\n", reqid);
     }  
     fprintf(stdout, "%s\n", stage_geturl(response));
     ret = 0;
   }
   return ret;
}



int 
cmd_parse(int argc, char *argv[], struct cmd_args *args) {
  int nbargs, Coptind, Copterr, errflg;
  char c;  
  
  /* Reinitializing the structure with null pointers */
  memset(args, '\0', sizeof(struct cmd_args));

  /* Now parsing the command line */
  Coptind = 1;
  Copterr = 1;
  errflg = 0;
  nbargs = 0;  
  while ((c = Cgetopt_long (argc, argv, "P:M:U:rm:s:S:h", longopts, NULL)) != -1) {
    switch (c) {
    case 'M':
      if (args->filename!= NULL) {
        errflg++;
      } else {
      	args->filename = Coptarg;
      }
      break;
    case 'P':
      if (args->protocol!= NULL) {
        errflg++;
      } else {
      	args->protocol = Coptarg;
      }
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
    case 's':
      args->size = strtou64(Coptarg);
      break;
    case 'm':
      args->mode = strtol(Coptarg, NULL, 0);
      break;
    case 'r':
      args->display_reqid = 1;
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
  if (args->filename== NULL) errflg++;
  return errflg;
}




void 
usage(char *cmd) {
  fprintf (stderr, "usage: %s ", cmd);
  fprintf (stderr, "%s",
	   "[-H stagerhost] [-P protocol] [-U usertag] [-S svc_class] [-m filemode] [-s filesize] [-r] [-h] -M hsmfile\n");
}
