/*
 * $Id: rfrm.c,v 1.4 2000/01/06 06:43:32 baud Exp $
 */

/*
 * Copyright (C) 1998-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */


#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rfrm.c,v $ $Revision: 1.4 $ $Date: 2000/01/06 06:43:32 $ CERN/IT/PDP/DM Olof Barring";
#endif /* not lint */

/*
 * Remove remote file
 */
#include <limits.h>
#include <string.h>
#include <sys/types.h>
#include <stdio.h>
#include <sys/stat.h>
#if !defined(_WIN32)
#include <dirent.h>
#endif
#include <rfio.h>
static char *ckpath();
char *getconfent();
static int rm_recursive(); 

char *cmd;
main(argc, argv) 
int argc;
char *argv[];
{
  int i, c, status;
  extern char * optarg ; 
  extern int    optind ;
  char *path,*root_path;
  int recursive = 0;
  struct stat st;

  cmd = argv[0];
  if ( argc < 2 ) {
    fprintf(stderr,"%s [-r] pathname ...\n",cmd);
    exit(1);
  }
  while ( (c = getopt(argc,argv,"r")) != EOF ) {
    switch(c) {
    case 'r':
      recursive++;
      break;
    case '?':
      fprintf(stderr,"Usage: %s [-r] pathname ...\n",cmd);
      exit(2);
    }
  }

  for (;optind<argc;optind++) {
    path = ckpath(argv[optind]);
    if ( recursive ) {
      root_path = (char *)malloc(strlen(path)+1);
      strcpy(root_path,path);
      rm_recursive(root_path);
      free(root_path);
    } else {
      if ( rfio_lstat(path,&st) ) {
        rfio_perror(path);
        exit(2);
      }
      if (st.st_mode & S_IFDIR) {
        fprintf(stderr,"%s: %s directory\n",cmd,path);
        exit(2);
      } 
      status = rfio_unlink(path);
      if ( status ) {
	rfio_perror(path);
	exit(1);
      }
    }
  }
  exit(0);
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

static int rm_recursive(path) 
char *path;
{
#if !defined(_WIN32)
   DIR *dirp;
  struct dirent *de;
  struct stat st;
  char *p;
  char yesno[4];
  
  if ( !rfio_lstat(path,&st) ) {
    if ( (st.st_mode & S_IFDIR) ) {
      printf("%s: remove files in directory %s? ",cmd,path);
      scanf("%[^\n]",yesno);
      fflush(stdin);
      if ( strcmp(yesno,"y") ) return(0);
      dirp = rfio_opendir(path);
      while ( ( de = readdir(dirp) ) != NULL ) {
	if ( strcmp(de->d_name,".") && strcmp(de->d_name,"..") ) {
	  p = (char *)malloc(strlen(path)+strlen(de->d_name)+2);
	  strcpy(p,path);
	  strcat(p,"/");
	  strcat(p,de->d_name);
	  rm_recursive(p);
	  free(p);
	}
      }
      closedir(dirp);
    }
    if ( st.st_mode & S_IFDIR ) {
      if ( rfio_rmdir(path) ) {
        rfio_perror("rmdir()");
        exit(1);
      }
    } else {
      if ( rfio_unlink(path) ) {
        rfio_perror("unlink()");
        exit(1);
      }
    }
  } else {
    rfio_perror(path);
    exit(2);
  }
#endif  
  return(0);
}

