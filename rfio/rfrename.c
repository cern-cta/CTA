/*
 * $Id: rfrename.c,v 1.7 2006/04/28 14:01:56 gtaur Exp $
 */

/*
 * Copyright (C) 1998-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rfrename.c,v $ $Revision: 1.7 $ $Date: 2006/04/28 14:01:56 $ CERN/IT/PDP/DM Olof Barring";
#endif /* not lint */
 
/*
 * Make remote directory
 */

#include <RfioTURL.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif /* _WIN32 */
#include <rfio_api.h>
static char *ckpath();
char *getconfent();

int main(argc, argv) 
int argc;
char *argv[];
{
  char *old_path,*new_path;
#if defined(_WIN32)
  WSADATA wsadata;
#endif
  RfioTURL_t t1;
  RfioTURL_t t2;
  int ret;

  if ( argc < 3 ) {
    fprintf(stderr,"Usage: %s old-path new-path\n",argv[0]);
    exit(2);
  }
  old_path = ckpath(argv[1]);
  new_path = ckpath(argv[2]);
  
  ret= rfioTURLFromString(old_path,&t1);
  if (ret!=-1){old_path=t1.rfioPath;}

  ret= rfioTURLFromString(new_path,&t2);
  if (ret!=-1){new_path=t2.rfioPath;}
  
#if defined(_WIN32)
  if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
    fprintf (stderr, "WSAStartup unsuccessful\n");
    exit (2);
  }
#endif
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


