/*
 * $Id: rfrename.c,v 1.3 1999/12/09 13:47:13 jdurand Exp $
 */

/*
 * Copyright (C) 1998-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rfrename.c,v $ $Revision: 1.3 $ $Date: 1999/12/09 13:47:13 $ CERN/IT/PDP/DM Olof Barring";
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


