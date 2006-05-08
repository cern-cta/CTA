/*
 * $Id: rfrm.c,v 1.18 2006/05/08 13:37:03 gtaur Exp $
 */

/*
 * Copyright (C) 1998-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */


#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rfrm.c,v $ $Revision: 1.18 $ $Date: 2006/05/08 13:37:03 $ CERN/IT/PDP/DM Olof Barring";
#endif /* not lint */

/*
 * Remove remote file
 */
#include <limits.h>
#include <string.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <dirent.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#define RFIO_KERNEL 1
#include <rfio.h>

struct dirstack {
  char *dir;
  struct dirstack *prev;
};

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
  int ask_yesno = 1;
  struct stat64 st;
#if defined(_WIN32)
  WSADATA wsadata;
#endif /* _WIN32 */

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
#if defined(_WIN32)
  if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
    fprintf (stderr, "WSAStartup unsuccessful\n");
    exit (2);
  }
#endif

  for (;optind<argc;optind++) {
    path = ckpath(argv[optind]);
    if ( recursive ) {
      root_path = (char *)malloc(strlen(path)+1);
      strcpy(root_path,path);
      /*
       * remove all files
       */
      rfio_errno = 0;
      serrno = 0;
      status = rm_recursive(root_path,&ask_yesno);
      if ( status == -1 ) {
         rfio_perror(root_path);
         exit(2);
      }
      /*
       * remove all directories. ENOENT can happen if the directory
       * was empty and removed already in previous call.
       */
      rfio_errno = 0;
      serrno = 0;
      status = rm_recursive(root_path,&ask_yesno);
      if ( (status == -1) &&
           (((rfio_errno != 0) && (rfio_errno != ENOENT)) ||
            ((rfio_errno == 0) &&
             (serrno     != 0) && (serrno     != ENOENT)) ||
            ((rfio_errno == 0) && (serrno     ==      0) &&
             (errno !=      0) && (errno != ENOENT)))
           ) {
         rfio_perror(root_path);
         exit(2);
      }
      free(root_path);
    } else {
      if ( rfio_lstat64(path,&st) ) {
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

static int rfio_pushdir(ds,dir)
struct dirstack **ds;
char *dir;
{
  struct dirstack *tmp;
  if ( ds == NULL || dir == NULL ) return(0);
  tmp = (struct dirstack *)malloc(sizeof(struct dirstack));
  tmp->prev = *ds;
  tmp->dir = (char *)malloc((strlen(dir)+1)*sizeof(char));
  strcpy(tmp->dir,dir);
  *ds = tmp;
  return(0);
}
static struct dirstack *rfio_popdir(ds)
struct dirstack **ds;
{
   struct dirstack *tmp;
   if ( ds == NULL ) return(NULL);
   tmp = *ds;
   *ds = (*ds)->prev;
   free(tmp->dir);
   free(tmp);
   return(*ds);
}

static int read_yesno() {
    int i, rc, retval;
    i =0;
    retval = 'n';
    do {
        rc = fgetc(stdin);
        if ( rc == ' ' ) continue;
        if ( i == 0 ) {
            retval = rc;
            i++;
        }
    } while ( rc != EOF && rc != '\n');
    return(retval);
}

static int rm_recursive(path, yesno) 
char *path;
int *yesno;
{
  DIR *dirp;
  struct dirent *de;
  struct stat64 st;
  char *p;
  struct dirstack *ds = NULL;
  int ask_yesno = 1;
  int empty = 1;
  char* hostname,*pathname;
  int rc;
  
  if ( !rfio_lstat64(path,&st) ) {
    if ( S_ISDIR(st.st_mode) ) {
      if ( yesno == NULL || *yesno ) {
        printf("%s: descend into directory `%s'? ",cmd,path);
        if ( read_yesno() != 'y' ) return(-1);
        if ( yesno != NULL ) *yesno = 0;
      }
     
      dirp = (DIR *)rfio_opendir(path);
      while ( ( de = (struct dirent *)rfio_readdir((RDIR *)dirp) ) != NULL ) {
        if ( strcmp(de->d_name,".") && strcmp(de->d_name,"..") ) {
          empty = 0;
          p = (char *)malloc(strlen(path)+strlen(de->d_name)+2);
          rfio_parse(path,&hostname,&pathname);
          if (pathname && (strstr(pathname,"/castor")==pathname ||strstr(pathname,"//castor")==pathname  )) strcpy(p,pathname);
	  else strcpy(p,path);
          strcat(p,"/");
          strcat(p,de->d_name);
          if ( rfio_lstat64(p,&st) == -1 ) {
            fprintf(stderr,"%s: %s\n",p,rfio_serror());
            free(p);
          } else {
            if ( S_ISDIR(st.st_mode) ) {
              rfio_pushdir(&ds,p);
            } else {
              if ( rfio_unlink(p) ) {
                fprintf(stderr,"unlink(%s): %s\n",p,rfio_serror());
                exit(1);
              }
              free(p);
            } 
          }
        }
      }
      
      rfio_closedir((RDIR *)dirp);
      if ( empty ) {
        printf("%s: remove directory `%s'? ",cmd,path);
        if ( read_yesno() != 'y' ) return(-1);
        if ( rfio_rmdir(path) == -1 ) {
            fprintf(stderr,"rmdir(%s): %s\n",path,rfio_serror());
            exit(2);
        }
      }
    } else { /* if ( S_ISDIR(st.st_mode) ) ... */
      if ( rfio_unlink(path) ) {
        fprintf(stderr,"unlink(%s): %s\n",path,rfio_serror());
        exit(1);
      }
    }
    while ( ds != NULL ) {
      while ( rm_recursive(ds->dir,&ask_yesno) == 0 );
      rfio_popdir(&ds);
    }
  } else { /* if ( !rfio_lstat64(path,&st) ) .... */
    return(-1);
  }
  return(0);
}
