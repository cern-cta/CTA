/******************************************************************************
 *                      stager_putdone.c
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003-2007 CERN/IT
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
 *
 * command line for stage_putDone
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stager_api.h"
#include "stager_errmsg.h"
#include "serrno.h"
#include "Cgetopt.h"
#include "stager_client_commandline.h"

void usage (char *);

/* Global vars used by common functions */
static int filenb;
static struct stage_filereq *requests;


/* Uses the filenb global variable
   that should be set to 0 before 1st call */
static int _countFiles(const char *filename) {
  (void)filename;
  filenb++;
  return 0;
}

/* uses the requests global variable,
   which should already be initialized */
static int _fillStruct(const char *filename) {
  requests[filenb].filename = (char *)strdup(filename);
  filenb++;
  return 0;
}


int
main(int argc, char *argv[]) {
  struct stage_fileresp *responses;
  int errflg, total_nb_files, rc, nbresps, ret;
  char *reqid;
  char errbuf[ERRBUFSIZE+1];
  int display_reqid = 0;
  char* usertag = NULL;
  char* unused = NULL;
  struct stage_options opts;
  opts.stage_host = NULL;
  opts.service_class = NULL;
  opts.stage_port=0;
  usertag = NULL;
  filenb = 0;

  /* Parsing command line */
  errflg =  parseCmdLine(argc, argv, _countFiles, &opts.service_class, &usertag, &display_reqid);
  if (errflg != 0 || filenb <= 0) {
    usage (argv[0]);
    exit (EXIT_FAILURE);
  }
  total_nb_files = filenb;

  ret=getDefaultForGlobal(&opts.stage_host,&opts.stage_port,&opts.service_class);

  /* Setting the error buffer and preparing the array of file requests */
  stager_seterrbuf(errbuf, sizeof(errbuf));
  create_filereq(&requests, total_nb_files);

  /* Iterating over the command line again to fill in the array of requests */
  filenb = 0;
  errflg = parseCmdLine(argc, argv, _fillStruct, &unused, &unused, &display_reqid);

  /* Performing the actual call */
  rc = stage_putDone(usertag,
                     requests,
                     total_nb_files,
                     &responses,
                     &nbresps,
                     &reqid,
                     &opts);

  if (rc < 0) {
    fprintf(stderr, "Error: %s\n", sstrerror(serrno));
    fprintf(stderr, "%s\n", errbuf);
    exit(EXIT_FAILURE);
  }

  ret = printFileResponses(nbresps, responses);
  if (display_reqid) {
    printf("Stager request ID: %s\n", reqid);
  }

  return ret;
}


void usage(char *cmd) {
  fprintf (stderr, "usage: %s ", cmd);
  fprintf (stderr, "%s",
           "[-M hsmfile [-M hsmfile ...]] [-f hsmFileList] [-r requestid] [-h]\n");
}
