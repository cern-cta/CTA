/******************************************************************************
 *                      stager_get.c
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
 * @(#)$RCSfile: stager_actualput.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2009/04/17 13:36:25 $ $Author: sponcec3 $
 *
 * command line for stage_prepareToGet 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

extern "C" {

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "stager_api.h"
#include "stager_errmsg.h"
#include "serrno.h"
#include "stager_client_commandline.h"
#include "Cgetopt.h"

  void usage (char *);

  int main(int argc, char *argv[]) {
    // parse command line
    static struct Coptions longopts[] =
      {
        {"filename",      REQUIRED_ARGUMENT,  NULL,      'M'},
        {"service_class", REQUIRED_ARGUMENT,  NULL,      'S'},
        {"protocol",      REQUIRED_ARGUMENT,  NULL,      'P'},
        {"help",          NO_ARGUMENT,        NULL,      'h'},
        {NULL,            0,                  NULL,        0}
      };

    int Coptind, Copterr;
    Coptind = 1;
    Copterr = 1;
    int errflg = 0;
    char c;
    struct stage_options opts;
    opts.stage_host = NULL;
    opts.service_class = NULL;
    opts.stage_port=0;
    const char* protocol = "rfio";
    int filenb = 0;
    char* filename;
 
    while ((c = Cgetopt_long (argc, argv, "M:S:P:h", longopts, NULL)) != -1) {
      switch (c) {
      case 'M':
        filenb++;
        filename = Coptarg;
        break;
      case 'S':
        opts.service_class = Coptarg;
        break;
      case 'P':
        protocol = Coptarg;
        break;
      case 'h':
      default:
        errflg++;
        break;
      }
      if (errflg != 0) break;
    }

    // Stop here if parsing went bad
    if (errflg != 0 || filenb != 1) {
      usage (argv[0]);
      exit (EXIT_FAILURE);
    }

    // setup environment
    if (getDefaultForGlobal(&opts.stage_host,
                            &opts.stage_port,
                            &opts.service_class) < 0) {
      printf("Could not setup environment\n");    
      exit (EXIT_FAILURE);    
    };

    // Setting the error buffer
    char errbuf[ERRBUFSIZE+1];
    stager_seterrbuf(errbuf, sizeof(errbuf));

    // call stager_put effectively
    struct stage_io_fileresp *response;
    char *reqid;
    int rc = stage_put(0, protocol, filename, 0644, 0,
                       &response, &reqid, &opts);
    if (rc < 0) {
      fprintf(stderr, "Error: %s\n", sstrerror(serrno));
      fprintf(stderr, "%s\n", errbuf);
      exit(EXIT_FAILURE);
    }

    // output transfer URL
    if (response->errorCode == 0) {
      printf("%s://%s:%d/%s\n",
             response->protocol,response->server,
             response->port,response->filename);
    } else {
      printf("ERROR : %s\n%s\n",
             strerror(response->errorCode),response->errorMessage);
    }

    exit(0);
  }

  void usage(char* cmd) {
    fprintf (stderr, "usage: %s ", cmd);
    fprintf (stderr, "%s",
             "-M hsmfile [-S svcClass] [-P protocol] [-h]\n");
  }

}
