/*
 * $Id: rfmkdir.c,v 1.3 1999/12/09 13:47:12 jdurand Exp $
 */

/*
 * Copyright (C) 1998-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rfmkdir.c,v $ $Revision: 1.3 $ $Date: 1999/12/09 13:47:12 $ CERN/IT/PDP/DM Olof Barring";
#endif /* not lint */
 
/*
 * Make remote directory
 */
#include <limits.h>
#include <string.h>
#include <sys/types.h>
#include <stdio.h>
#include <sys/stat.h>
#include <rfio.h>

#if !defined(_WIN32)
static char *ckpath();
char *getconfent();
#endif /* _WIN32 */

int main(argc, argv) 
int argc;
char *argv[];
{
#if !defined(_WIN32)
  extern char * optarg ; 
  extern int    optind ;
  int recursive = 0;
  char *path,*root_path,*p;
  int rc, c;
  mode_t mode = 0777;
  struct stat st;
  
  if ( argc < 2 ) {
    fprintf(stderr,"Usage: %s [-m mode] [-p] dirname ...\n",argv[0]);
    exit(2);
  }

  mode &= ~umask(0);
  while ( (c = getopt(argc,argv,"m:p")) != EOF ) {
    switch(c) {
    case 'm':
      if ( isdigit(*optarg) ) sscanf(optarg,"%o",&mode);
      else {
	fprintf(stderr,"Invalid mode.\n");
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

  for (;optind<argc;optind++) {
    path = ckpath(argv[optind]);
    if ( recursive) {
      root_path = (char *)malloc(strlen(path)+1);
      strcpy(root_path,path);
      while ( (p = strrchr(root_path,'/')) != NULL ) {
	*p = '\0';
	if ( !rfio_stat(root_path,&st) ) break;
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
#endif /* _WIN32 */
  return(0);
}

#if !defined(_WIN32)

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

#endif /* _WIN32 */
