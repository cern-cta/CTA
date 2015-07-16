/*
 * $Id: rfrename.c,v 1.11 2008/07/31 13:16:55 sponcec3 Exp $
 */

/*
 * Copyright (C) 1998-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * Make remote directory
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <rfio_api.h>
static char *ckpath();
char *getconfent();

int main(int argc,
         char *argv[])
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

static char *ckpath(char *path)
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
    strcpy(newpath,path);
  return(newpath);
}
