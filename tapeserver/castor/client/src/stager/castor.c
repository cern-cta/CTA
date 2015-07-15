/*
 * $Id: castor.c,v 1.9 2009/01/14 17:33:32 sponcec3 Exp $
 */

/*	castor - display current CASTOR version */
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <Cgetopt.h>
#include <stager_api.h>
#include <stager_client_commandline.h>
#include "patchlevel.h"

int main(int argc, char ** argv) {
  struct stage_options opts;
  int c, Mv,mv,Mr,mr, ret;

  opts.stage_host = NULL;
  opts.service_class = NULL;
  opts.stage_port=0;
  ret=getDefaultForGlobal(&opts.stage_host,&opts.stage_port,&opts.service_class);

  while ((c = getopt (argc, argv, "hvs")) != EOF) {
    switch (c) {
    case 'v':
      printf ("%d.%d.%d-%d\n", MAJORVERSION, MINORVERSION, MAJORRELEASE, MINORRELEASE);
      break;
    case 's':
      ret = stage_version(&Mv, &mv, &Mr, &mr, &opts);
      if (0 == ret) {
	printf ("%d.%d.%d-%d\n", Mv, mv, Mr, mr);
      }
      break;
    case 'h':
    default:
      printf ("usage : %s [-v] [-s] [-h]\n", argv[0]);
      break;
    }
  }
  exit (0);
}
