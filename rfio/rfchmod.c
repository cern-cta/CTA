/*
 * $Id: rfchmod.c,v 1.6 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 1998-2002 by IN2P3 CC
 * All rights reserved
 */

/*
 * Change mode of a file
 */
#include <string.h>
#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>
#include <getopt.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <rfio_api.h>
#include "getconfent.h"

static char *ckpath();

static char *cmdid;         /* Command Id                     */

/* Summary help                                               */
void help(int rcode) {
  fprintf(stderr,"Usage: %s absolute-mode filename ...\n", cmdid);
  exit(rcode);
}

int main(argc, argv)
     int argc;
     char *argv[];
{
  extern int optind;
  char     *path;
  int      exit_rc = 0;
  int      c;
  mode_t mode = 0777;
  long int lmode = 0;       /* For conversion, then casting to mode  IN2P3*/
  char     *endprt;             /* For conversion                        IN2P3*/

  cmdid = argv[0];

  /* Options decoding                              */
  while ( (c = getopt(argc,argv,"")) != EOF ) {
    switch(c) {
    case '?':
      help(2);
    }
  }

  /* First argument is permission mode in octal    */
  if ( optind >= argc ) {
    fprintf(stderr,"Missing access mode\n");
    help(2);
  }

  lmode = strtol(argv[optind], &endprt, 8);
  if ( lmode > 0 && lmode <= 0777 && *endprt == '\0' ) mode = lmode;
  else {
    fprintf(stderr, "Invalid mode '%s'.\n", argv[optind]);
    help(2);
  }
  optind++;

  /* the next arguments are a list of file names   */
  if ( optind >= argc ) {
    fprintf(stderr,"Missing file name\n");
    help(2);
  }

  for (;optind<argc;optind++) {
    path = ckpath(argv[optind]);


    if ( rfio_chmod(path,mode) ) {
      fprintf(stderr, "chmod(): %s: %s\n", path, rfio_serror() );
      exit_rc++;
    }
  }
  return(exit_rc);
}

static char *ckpath(path)
     char *path;
{
  char *cp;
  static char newpath[BUFSIZ];
  /* Special treatment for filenames starting with /scratch/... */
  if (!strncmp ("/scratch/", path, 9) &&
      (cp = getconfent ("SHIFT", "SCRATCH", 0)) != NULL) {
    strcpy (newpath, cp);
    strcat (newpath, path+9);
  } else
    strcpy(newpath,path);
  return(newpath);
}
