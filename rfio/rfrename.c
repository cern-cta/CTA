/*
 * $Id: rfrename.c,v 1.2 1999/07/20 12:48:18 jdurand Exp $
 *
 * $Log: rfrename.c,v $
 * Revision 1.2  1999/07/20 12:48:18  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Timeouted version of RFIO. Using netread_timeout() and netwrite_timeout
 *   on all control and data sockets.
 *
 */

/*
 * Copyright (C) 1998,1999 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)rfrename.c	1.2 08 Jan 1999 CERN IT-PDP/DM Olof Barring";
#endif /* not lint */
 
/*
 * Make remote directory
 */
#include <stdio.h>
#include <rfio.h>
static char *ckpath();
char *getconfent();

int main(argc, argv) 
int argc;
char *argv[];
{
  char *old_path,*new_path;
  
  if ( argc < 3 ) {
    fprintf(stderr,"Usage: %s old-path new-path\n",argv[0]);
    exit(2);
  }
  old_path = ckpath(argv[1]);
  new_path = ckpath(argv[2]);
  
  if ( rfio_rename(old_path,new_path) ) {
    rfio_perror("rename()");
    exit(1);
  }
  return(0);
}

static char *ckpath(path)
char *path;
{
  char *cp;
  char *newpath;
  newpath = (char *)malloc(BUFSIZ*sizeof(char));
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


