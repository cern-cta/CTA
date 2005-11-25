/******************************************************************************
 *                      stager/stager_setFileGCWeight.c
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
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
 * @(#)$RCSfile: stager_setFileGCWeight.c,v $ $Revision: 1.1 $ $Release$ $Date: 2005/11/25 11:11:01 $ $Author: sponcec3 $
 *
 * command line for stager_setFileGCWeight
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <stager_api.h>
#include <serrno.h>
#include "Cgetopt.h"

static struct Coptions longopts[] =
  {
    {"filename",      REQUIRED_ARGUMENT,  NULL,      'M'},
    {"help",          NO_ARGUMENT,        NULL,      'h'},
    {NULL,            0,                  NULL,        0}
  };

void usage _PROTO((char *));
int cmd_parse(int argc, char *argv[], struct stage_filereq **reqs, int* nbreqs);
int cmd_countHsmFiles(int argc, char *argv[]);

#define ERRBUFSIZE 255

int main(int argc, char *argv[]) {
  struct stage_filereq *reqs;
  struct stage_fileresp *response;
  float weight = 0.0;
  int nbresps, nbreqs;
  char *reqid;
  char errbuf[ERRBUFSIZE+1];
  int errflg, rc, i;
  
  /* Parsing the command line */
  memset(&errbuf,  '\0', sizeof(errbuf));
  errflg =  cmd_parse(argc, argv, &reqs, &nbreqs, &weight);
  if (errflg != 0) {
    usage (argv[0]);
    exit (EXIT_FAILURE);
  }

  /* Setting the error buffer */
  stage_seterrbuf(errbuf, sizeof(errbuf));

  /* Performing the actual call */
  rc = stage_setFileGCWeight(reqs,
                             nbreqs,
                             weight,
                             &response,
                             &nbresps,
                             &reqid,
                             NULL);
 
  if (rc < 0) { 
    fprintf(stderr, "Error %s\n", sstrerror(serrno));
    fprintf(stderr, "<%s>\n", errbuf);
    exit(1);
  }

  if (response == NULL) {
    fprintf(stderr, "Error: Response object is NULL\n");
    exit(1);
  }
  
  fprintf(stdout, "%s Received %d responses\n", reqid, nbresps);
  for (i = 0; i < nbresps; i++) {
    fprintf(stdout, "%s:%s",response[i].filename, 
            stage_statusName(response[i].status));
    if (response[i].errorCode != 0) {      
      fprintf(stdout, " (%d, %s)",  
              response[i].errorCode,  
              response[i].errorMessage);
    }
    fprintf(stdout, "\n");
  }
  return 0;
}


int cmd_parse(int argc,
              char *argv[],
              struct stage_filereq **reqs,
              int* nbreqs, &float weight) {
  int nbfiles, Coptind, Copterr, errflg;
  char c;

  /* Counting the number of HSM files */
  if ((*nbreqs = cmd_countHsmFiles(argc, argv)) < 0) {
    return -1;
  }

  /* Creating the structure for files */
  if (nbreqs > 0) {
    create_filereq(reqs, *nbreqs);
  }

  /* Now parsing the command line */
  Coptind = 1;
  Copterr = 1;
  errflg = 0;
  nbfiles = 0;
  while ((c = Cgetopt_long
          (argc, argv, "M:h", longopts, NULL)) != -1) {
    switch (c) {
    case 'M':
      (*reqs)[nbfiles].filename = Coptarg;
      nbfiles++;
      break;
    case 'h':
    default:
      errflg++;
      break;
    }
    if (errflg != 0) break;
  }
  if (*nbreqs == 0) {
    errflg++;
    return errflg;
  }
  // get weight
  argc -= Coptind;
  argv += Coptind;
  if (argc != 1) {
    errflg++;
  } else {
    weight = atof(argv[0]);
  }
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
  while ((c = Cgetopt_long (argc, argv, "M:h", longopts, NULL)) != -1) {
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
           "[-h] -M hsmfile [-M ...] <weight>\n");
}
