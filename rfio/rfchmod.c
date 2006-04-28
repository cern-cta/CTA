/*
 * $Id: rfchmod.c,v 1.3 2006/04/28 16:24:46 gtaur Exp $
 */

/*
 * Copyright (C) 1998-2002 by IN2P3 CC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rfchmod.c,v $ $Revision: 1.3 $ $Date: 2006/04/28 16:24:46 $ IN2P3 CC Philippe Gaillardon";
#endif /* not lint */

/*
 * Change mode of a file
 */
#include <string.h>
#include <sys/types.h>
#include <stdio.h>
#include <sys/stat.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif /* _WIN32 */
#include <rfio_api.h>


static char *ckpath();
char *getconfent();

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
  extern char * optarg ; 
  extern int    optind ;
  int      recursive = 0;
  char     *path,*root_path,*p;
  int      rc;
  int      exit_rc = 0;
  int      c;
  mode_t mode = 0777;
  long int lmode = 0;       /* For conversion, then casting to mode  IN2P3*/
  char     *endprt;             /* For conversion                        IN2P3*/
  struct   stat st;
  
#if defined(_WIN32)
  WSADATA wsadata;
#endif
  
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

#if defined(_WIN32)
  if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
    fprintf (stderr, "WSAStartup unsuccessful\n");
    exit (2);
  }
#endif

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
 /* Special treatment for filenames starting with /hpss/... */
    if ( !strncmp("/hpss/",path,6) &&
	 (cp = getconfent("SHIFT","HPSS",0)) != NULL) {
      strcpy(newpath,cp);
      strcat(newpath,path+6);
    } else strcpy(newpath,path);
  return(newpath);
}
