/******************************************************************************
 *                      stager_rm.c
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003-2007 CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile: stager_rm.c,v $ $Revision: 1.9 $ $Release$ $Date: 2007/12/06 18:29:31 $ $Author: waldron $
 *
 * command line for stager_rm
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stager_api.h>
#include <serrno.h>
#include "Cgetopt.h"
#include "stager_client_commandline.h"

static struct Coptions longopts[] =
  {
    {"filename",      REQUIRED_ARGUMENT,  NULL,      'M'},
    {"svcClass",      REQUIRED_ARGUMENT,  NULL,      'S'},
    {"help",          NO_ARGUMENT,        NULL,      'h'},
    {"all",           NO_ARGUMENT,        NULL,      'a'},
    {NULL,            0,                  NULL,        0}
  };

void usage _PROTO((char *));
int cmd_parse(int argc, char *argv[], struct stage_filereq **reqs, int* nbreqs, struct stage_options* opts);
int cmd_countHsmFiles(int argc, char *argv[]);

int main(int argc, char *argv[]) {
  struct stage_filereq *reqs;
  struct stage_fileresp *responses;
  int nbresps, nbreqs;
  char *reqid;
  char errbuf[ERRBUFSIZE+1];
  int errflg, rc, ret;
  struct stage_options opts;

  opts.stage_host = NULL;
  opts.service_class = NULL;
  opts.stage_version=2;
  opts.stage_port=0;
  ret=getDefaultForGlobal (&opts.stage_host,&opts.stage_port,&opts.service_class,&opts.stage_version);

  /* Parsing the command line */
  memset(&errbuf,  '\0', sizeof(errbuf));
  errflg = cmd_parse(argc, argv, &reqs, &nbreqs, &opts);
  if (errflg != 0) {
    usage (argv[0]);
    exit (EXIT_FAILURE);
  }

  /* Setting the error buffer */
  stage_seterrbuf(errbuf, sizeof(errbuf));

  /* Performing the actual call */
  rc = stage_rm(reqs,
                nbreqs,
                &responses,
                &nbresps,
                &reqid,
                &opts);
 
  if (rc < 0) {
    fprintf(stderr, "Error: %s\n", sstrerror(serrno));
    fprintf(stderr, "<%s>\n", errbuf);
    exit(EXIT_FAILURE);
  }

  ret = printFileResponses(nbresps, responses);
  
  return ret;
}


int cmd_parse(int argc,
              char *argv[],
              struct stage_filereq **reqs,
              int* nbreqs,
              struct stage_options* opts) {
  int nbfiles, Coptind, Copterr, errflg;
  char c;

  /* Counting the number of HSM files */
  if ((*nbreqs = cmd_countHsmFiles(argc, argv)) < 0) {
    return -1;
  }

  /* Creating the structure for files */
  if (*nbreqs > 0) {
    create_filereq(reqs, *nbreqs);
  }

  /* Now parsing the command line */
  Coptind = 1;
  Copterr = 1;
  errflg = 0;
  nbfiles = 0;
  while ((c = Cgetopt_long
          (argc, argv, "M:S:ha", longopts, NULL)) != -1) {
    switch (c) {
    case 'M':
      (*reqs)[nbfiles].filename = Coptarg;
      nbfiles++;
      break;
    case 'S':
      opts->service_class = (char *)strdup(Coptarg);
      break;
    case 'a':
      opts->service_class = "*";
      break;
    case 'h':
    default:
      errflg++;
      break;
    }
    if (errflg != 0) break;
  }
  if (*nbreqs == 0) errflg++;
  return errflg;  
}

/**
 * Counts the number of HSM files given on the command line
 * @param argc the number of arguments on the command line
 * @param argv the arguments on the command line
 * @return the number of HSM files given on the command line
 * or -1 if an error occured
 */
int cmd_countHsmFiles(int argc, char *argv[]) {
  int Coptind, Copterr, errflg, nbargs;
  char c;

  Coptind = 1;
  Copterr = 1;
  errflg = 0;
  nbargs = 0;
  while ((c = Cgetopt_long (argc, argv, "S:M:ha", longopts, NULL)) != -1) {
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

/**
 * Displays usage 
 * @param cmd command name 
 */
void usage(char* cmd) {
  fprintf (stderr, "usage: %s ", cmd);
  fprintf (stderr, "%s",
           "[-h] [-S svcClass | -a] -M hsmfile [-M ...]\n");
}
