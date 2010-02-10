/******************************************************************************
 *                      stager/stager_listPrivileges.c
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
 * @(#)$RCSfile $ $Revision $ $Release$ $Date $ $Author $
 *
 * command line for stager_listPrivileges
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <pwd.h>
#include <grp.h>
#include "stager_api.h"
#include "stager_errmsg.h"
#include "serrno.h"
#include "Cgetopt.h"
#include "stager_client_commandline.h"
#include "castor/Constants.hpp"

/**
 * list of command line options
 */
static struct Coptions longopts[] =
  {
    {"user" ,         REQUIRED_ARGUMENT,  NULL,      'U'},
    {"group",         REQUIRED_ARGUMENT,  NULL,      'G'},
    {"reqType",       REQUIRED_ARGUMENT,  NULL,      'R'},
    {"serviceClass",  REQUIRED_ARGUMENT,  NULL,      'S'},
    {"help",          NO_ARGUMENT,        NULL,      'h'},
    {NULL,            0,                  NULL,        0}
  };

/**
 * parses input
 * @param argc number of command line arguments
 * @param argv the command line
 * @param user filled with the user id, if one is given
 * @param group filled with the group id, if one is given
 * @param reqTypes filled with the request type list, if one is given
 * @param serviceClass filled with the service class name, if one is given
 * @return 0 if everything went fine
 */
int cmd_parse(int argc,
              char *argv[],
              int *user,
              int *group,
              unsigned int *reqType,
              char **serviceClass) {
  int Coptind, Copterr, errflg, c;
  Coptind = 1;
  Copterr = 1;
  errflg = 0;
  char *dp = NULL;
  while ((c = Cgetopt_long
          (argc, argv, "U:G:R:S:h", longopts, NULL)) != -1) {
    switch (c) {
    case 'U':
      {
        int euid = (int)strtol(Coptarg, &dp, 10);
        if (*dp == 0) {
          *user = euid;
        } else {
          struct passwd *pass = getpwnam(Coptarg);
          if (0 == pass) {
            errflg++;
            fprintf(stderr, "Unknown user %s\n", Coptarg);
            break;
          }
          *user = pass->pw_uid;
        }
        break;
      }
    case 'G':
      {
        int egid = (int)strtol(Coptarg, &dp, 10);
        if (*dp == 0) {
          *group = egid;
        } else {
          struct group *grp = getgrnam(Coptarg);
          if (0 == grp) {
            errflg++;
            fprintf(stderr, "Unknown group %s\n", Coptarg);
            break;
          }
          *group = grp->gr_gid;
        }
        break;
      }
    case 'R':
      {
        // find out the request type
        unsigned int t;
        for (t = 1; t < castor::ObjectsIdsNb; t++) {
	  const char* s = castor::ObjectsIdStrings[t];
          if (0 == strcmp(s, Coptarg)) {
            *reqType = t;
            break;
          }
        }
        if (castor::ObjectsIdsNb == t) {
          errflg++;
          fprintf(stderr, "Unknown request type %s\n", Coptarg);
        }
	break;
      }
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
  // check number of arguments
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
  fprintf(stderr, "usage: %s ", cmd);
  fprintf(stderr, "%s",
          "[-h] [-S service class] [-U user] [-G group] [-R requestType]\n");
}

int main(int argc, char *argv[]) {
  struct stage_options opts;
  char errbuf[ERRBUFSIZE+1];
  int errflg, rc;
  int user = -1;
  int group = -1;
  unsigned int reqType = 0;
  int nbPrivs = 0;
  struct stage_listpriv_resp* privileges = 0;
  int i;

  /* default values, + environment */
  opts.stage_host = NULL;
  opts.service_class = NULL;
  opts.stage_port=0;
  getDefaultForGlobal(&opts.stage_host,
                      &opts.stage_port,
                      &opts.service_class);

  /* Parsing the command line */
  memset(&errbuf,  '\0', sizeof(errbuf));
  errflg = cmd_parse(argc, argv, &user, &group,
                     &reqType, &opts.service_class);
  if (errflg != 0) {
    usage (argv[0]);
    exit (EXIT_FAILURE);
  }

  /* Setting the error buffer */
  stager_seterrbuf(errbuf, sizeof(errbuf));

  /* Performing the actual call */
  rc = stage_listPrivileges(user, group, reqType,
                            &privileges, &nbPrivs, &opts);
  if (rc < 0) {
    fprintf(stderr, "Error: %s\n", sstrerror(serrno));
    fprintf(stderr, "<%s>\n", errbuf);
    exit(EXIT_FAILURE);
  }

  /* success, print output */
  if (0 == nbPrivs) {
    fprintf(stdout, "No privileges found\n");
  } else {
    fprintf(stdout, "%-15s %-8s %-8s %-30s %s\n",
	    "ServiceClass", "User", "Group", "RequestType", "Status");
    for (i = 0; i < nbPrivs; i++) {
      char* user;
      char* group;
      // get user name
      if (privileges[i].uid == -1) {
	user = (char*) malloc(2);
	sprintf(user, "*");
      } else {
	struct passwd *pass = getpwuid(privileges[i].uid);
	if (0 == pass) {
	  user = (char*) malloc(13);
	  sprintf(user, "[%d]", privileges[i].uid);
	} else {
	  user = strdup(pass->pw_name);
	}
      }
      // get group name
      if (privileges[i].gid == -1) {
	group = (char*) malloc(2);
	sprintf(group, "*");
      } else {
	struct group *grp = getgrgid(privileges[i].gid);
	if (0 == grp) {
	  group = (char*) malloc(13);
	  sprintf(group, "[%d]", privileges[i].gid);
	} else {
	  group = strdup(grp->gr_name);
	}
      }
      fprintf(stdout, "%-15s %-8s %-8s %-30s %s\n",
	      privileges[i].svcClass,
	      user,
	      group,
	      privileges[i].requestType == 0 ? "*" :
	      castor::ObjectsIdStrings[privileges[i].requestType],
	      privileges[i].isGranted ? "Granted" : "Denied");
    }
  }
  return 0;
}

