/*
 * Copyright (C) 1998-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rfmkdir.c,v $ $Revision: 1.9 $ $Date: 2006/04/28 16:24:46 $ CERN/IT/PDP/DM Olof Barring";
#endif /* not lint */
 
/*
 * Make remote directory
 */

#include <limits.h>
#include <string.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <rfio_api.h>

#if defined(_WIN32)
#include <winsock2.h>
#endif /* _WIN32 */

static char *ckpath();
char *getconfent();

int main(argc, argv) 
int argc;
char *argv[];
{
  extern char * optarg ; 
  extern int    optind ;
  int recursive = 0;
  char *path,*root_path,*p;
  int rc, c;
  mode_t mode = 0777;
  long int lmode = 0;       /* For conversion, then casting to mode  IN2P3*/
  char *endprt;             /* For conversion                        IN2P3*/
  struct stat64 st;
#if defined(_WIN32)
  WSADATA wsadata;
#endif /* _WIN32 */
  
  if ( argc < 2 ) {
    fprintf(stderr,"Usage: %s [-m mode] [-p] dirname ...\n",argv[0]);
    exit(2);
  }

  mode &= ~umask(0);
  while ( (c = getopt(argc,argv,"m:p")) != EOF ) {
    switch(c) {
    case 'm':
      /* Converts mode into long then casts it - IN2P3 */
      lmode = strtol(optarg, &endprt, 8);
      if ( lmode > 0 && lmode <= 0777 && *endprt == '\0' ) mode = lmode;
      else {
	fprintf(stderr, "Invalid mode '%s'.\n", optarg);
	exit(2);
      }
      break;
    case 'p':
      recursive++;
      break;
    case '?':
      fprintf(stderr,"Usage: %s [-m mode] [-p] dirname ...\n",argv[0]);
      exit(2);
    }
  }

#if defined(_WIN32)
  if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
    fprintf (stderr, "WSAStartup unsuccessful\n");
    exit (2);
  }
#endif

  for (;optind<argc;optind++) {
    path = ckpath(argv[optind]);
    
    
    if ( recursive) {
      root_path = (char *)malloc(strlen(path)+1);
      strcpy(root_path,path);
      while ( (p = strrchr(root_path,'/')) != NULL ) {
	*p = '\0';
	if ( !rfio_stat64(root_path,&st) ) break;
      }
      while ( strlen(root_path) != strlen(path) ) {
	root_path[strlen(root_path)] = '/';
	if ( rfio_mkdir(root_path,mode) ) {
	  rfio_perror("mkdir()");
	  exit(1);
	}
      }
      free(root_path);
    } else {
      if ( rfio_mkdir(path,mode) ) {
	rfio_perror("mkdir()");
	exit(1);
      }
    }
  }
  return(0);
}


static char *ckpath(path)
char *path;
{
  char *cp;
  static char newpath[BUFSIZ];
 /* Special treatment for filenames starting with /scratch/... */
  if (!strncmp ("/scratch/", path, 9) &&
      (cp = (char *)getconfent ("SHIFT", "SCRATCH", 0)) != NULL) {
    strcpy (newpath, cp);
    strcat (newpath, path+9);
  } else 
 /* Special treatment for filenames starting with /hpss/... */
    if ( !strncmp("/hpss/",path,6) &&
	 (cp = (char *)getconfent("SHIFT","HPSS",0)) != NULL) {
      strcpy(newpath,cp);
      strcat(newpath,path+6);
    } else strcpy(newpath,path);
  return(newpath);
}
