/******************************************************************************
 *                      stager/stager_changePrivilege.c
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
 *
 * command line for stager_changePrivilege
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "stager_api.h"
#include "stager_errmsg.h"
#include "serrno.h"
#include "Cgetopt.h"
#include "stager_client_commandline.h"
#include "stager_client_changePrivilege.h"

/**
 * list of command line options
 */
static struct Coptions longopts[] =
  {
    {"users",         REQUIRED_ARGUMENT,  NULL,      'U'},
    {"reqTypes",      REQUIRED_ARGUMENT,  NULL,      'R'},
    {"serviceClass",  REQUIRED_ARGUMENT,  NULL,      'S'},
    {"help",          NO_ARGUMENT,        NULL,      'h'},
    {NULL,            0,                  NULL,        0}
  };

/**
 * parses input
 * @param argc number of command line arguments
 * @param argv the command line
 * @param users filled with the user list, if one is given
 * @param reqTypes filled with the request type list, if one is given
 * @param serviceClass filled with the service class name, if one is given
 * @return 0 if everything went fine
 */
int cmd_parse(int argc,
              char *argv[],
              char **users,
              char **reqTypes,
              char **serviceClass) {
  int c, Coptind, Copterr, errflg;
  Coptind = 1;
  Copterr = 1;
  errflg = 0;
  while ((c = Cgetopt_long
          (argc, argv, "U:R:S:h", longopts, NULL)) != -1) {
    switch (c) {
    case 'U':
      *users = Coptarg;
      break;
    case 'R':
      *reqTypes = Coptarg;
      break;
    case 'S':
      *serviceClass = Coptarg;
      break;
    case 'h':
    default:
      errflg++;
      break;
    }
    if (errflg != 0) break;
  }
  /* check that arguments exist */
  if (argc == 1) {
    return 1;
  }
  /* check that all arguments were parsed */
  argc -= Coptind;
  if (argc != 0) {
    errflg++;
  }
  return errflg;
}

/**
 * Displays usage
 * @param cmd command name
 */
void usage(char* cmd) {
  fprintf (stderr, "usage: %s ", cmd);
  fprintf (stderr, "%s",
           "[-h] [-S service class] [-U userList] [-R requestTypes]\n");
}

int changePrivilege(int argc, char *argv[], int isAdd) {
  struct stage_options opts;
  char errbuf[ERRBUFSIZE+1];
  int errflg, rc;
  char *users = "";
  char *reqTypes = "";

  /* default values, + environment */
  opts.stage_host = NULL;
  opts.service_class = NULL;
  opts.stage_port=0;
  getDefaultForGlobal(&opts.stage_host,
                      &opts.stage_port,
                      &opts.service_class);

  /* Parsing the command line */
  memset(&errbuf,  '\0', sizeof(errbuf));
  errflg = cmd_parse(argc, argv, &users, &reqTypes, &opts.service_class);
  if (errflg != 0) {
    usage (argv[0]);
    exit (EXIT_FAILURE);
  }

  /* Setting the error buffer */
  stager_seterrbuf(errbuf, sizeof(errbuf));

  /* Performing the actual call */
  if (isAdd) {
    rc = stage_addPrivilege(users, reqTypes, &opts);
  } else {
    rc = stage_removePrivilege(users, reqTypes, &opts);
  }
  if (rc < 0) {
    fprintf(stderr, "Error: %s\n", sstrerror(serrno));
    fprintf(stderr, "%s\n", errbuf);
    exit(EXIT_FAILURE);
  }

  /* success */
  fprintf(stdout, "Change done successfully\n");
  return 0;
}

