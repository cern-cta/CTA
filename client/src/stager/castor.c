/*
 * $Id: castor.c,v 1.7 2007/08/27 14:57:45 sponcec3 Exp $
 */

/*	castor - display current CASTOR version */
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <Cgetopt.h>
#include <stager_api.h>
#include <stager_client_commandline.h>
#include "patchlevel.h"
#define __BASEVERSION__ "?"
#define __PATCHLEVEL__ 0

int main(int argc, char ** argv) {
  struct stage_options opts;
  int c, Mv,mv,Mr,mr, ret;

  opts.stage_host = NULL;
  opts.service_class = NULL;
  opts.stage_port=0;
  opts.stage_version=2;
  ret=getDefaultForGlobal(&opts.stage_host,&opts.stage_port,&opts.service_class,&opts.stage_version);

  while ((c = getopt (argc, argv, "hvs")) != EOF) {
    switch (c) {
    case 'v':
      printf ("%s.%d\n", BASEVERSION, PATCHLEVEL);
      break;
    case 's':
      ret = stage_version(&Mv, &mv, &Mr, &mr, &opts);
      if (0 == ret) {
	printf ("%d.%d.%d-%d\n", Mv, mv, Mr, mr);
      }
      break;
    case 'h':
    default:
      printf ("usage : %s [-r] [-s] [-h]\n", argv[0]);
      break;
    }
  }
  exit (0);
}
