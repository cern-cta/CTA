/*
 *
 * Copyright (C) 2003 by CERN/IT/ADC/CA
 * All rights reserved
 *
 * @(#)$RCSfile: test_longpaths.c,v $ $Revision: 1.2 $ $Release$ $Date: 2004/03/02 17:42:26 $ $Author: obarring $
 *
 *
 *
 * @author Olof Barring
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: test_longpaths.c,v $ $Revision: 1.2 $ $Release$ $Date: 2004/03/02 17:42:26 $ Olof Barring";
#endif /* not lint */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <Castor_limits.h>
#include <Cgetopt.h>
#include <rfio_api.h>
#include <serrno.h>

static int doOpen = 0;

void usage(char *cmd) 
{
  printf("Usage: %s <root-path1> <root-path2> ...\n",cmd);
  return;
}

int randomElement(char *element, int len) 
{
  int i, j, nbLetters;
  char nextLetter;

  if ( element == NULL || len < 0 ) return(-1);
  nbLetters = 'z' - 'a';
  for (i=0; i<len; i++) {
    nextLetter = 'a' + (int) (((float)nbLetters)*rand()/(RAND_MAX+1.0));
    *(element+i) = nextLetter;
  }
  *(element+len) = '\0';
  return(0);
}

int doValidPathTest(char *rootPath) 
{
  int rc, nextLen, totLen, save_errno;
  char nextElement[CA_MAXNAMELEN+1], fullPath[CA_MAXPATHLEN+1];
  struct stat st;
  if ( strlen(rootPath) > CA_MAXPATHLEN ) {
    fprintf(stdout,"doValidPathTest(): %s too long\n",rootPath);
    return(-1);
  }
  
  memset(fullPath,'\0',sizeof(fullPath));
  sprintf(fullPath,"%s/",rootPath);
  totLen = strlen(fullPath);
  while ( totLen < CA_MAXPATHLEN ) {
    nextLen = CA_MAXPATHLEN-totLen;
    if ( nextLen > CA_MAXNAMELEN ) nextLen = CA_MAXNAMELEN;
    nextLen = 1+(int)(((float)nextLen)*rand()/(RAND_MAX+1.0));
    rc = randomElement(nextElement,nextLen);
    if ( rc == -1 ) {
      fprintf(stdout,"randomElement(%p,%d) returned -1\n",nextElement,nextLen);
      return(-1);
    }
    strcat(fullPath,nextElement);
    printf("Create path %s\n",fullPath);
    rc = rfio_mkdir(fullPath,0755);
    if ( rc == -1 ) {
      fprintf(stdout,"rfio_mkdir(%s,0755): %s\n",fullPath,rfio_serror());
      return(-1);
    }
    rc = rfio_stat(fullPath,&st);
    if ( rc == -1 ) {
      fprintf(stdout,"rfio_stat(%s,%p): %s\n",fullPath,&st,rfio_serror());
      return(-1);
    }
    rc = rfio_access(fullPath,R_OK);
    if ( rc == -1 ) {
      fprintf(stdout,"rfio_access(%s,0x%x): %s\n",fullPath,R_OK,rfio_serror());
      return(-1);
    }

    if ( doOpen == 1 ) {
      rc = rfio_open(fullPath,O_RDWR,0755);
      if ( rc == -1 ) {
        save_errno = rfio_serrno();
        if ( save_errno == EISDIR ) {
          fprintf(stdout,"rfio_open(%s) expected error %s\n",
                  fullPath,rfio_serror());
        } else {
          fprintf(stdout,"rfio_open(%s) unexpected error %s\n",
                  fullPath,rfio_serror());
          return(-1);
        }
      } else {
        fprintf(stdout,"rfio_open(%s) unexpected success\n",fullPath);
        rfio_close(rc);
        return(-1);
      }
    }
    
    totLen = strlen(fullPath);
    if ( totLen < CA_MAXPATHLEN ) strcat(fullPath,"/");
    totLen = strlen(fullPath);
  }
  fullPath[CA_MAXPATHLEN] = '\0';
  rc = rmPath(rootPath,fullPath);
  return(rc);
}

int doInvalidPathTest(char *rootPath) 
{
  int rc, nextLen, totLen, save_errno, nameTooLong = 0;
  char nextElement[2*CA_MAXNAMELEN+1], fullPath[2*CA_MAXPATHLEN+1];
  struct stat st;
  if ( strlen(rootPath) > 2*CA_MAXPATHLEN ) {
    fprintf(stdout,"doValidPathTest(): %s too long\n",rootPath);
    return(-1);
  }
  
  memset(fullPath,'\0',sizeof(fullPath));
  sprintf(fullPath,"%s/",rootPath);
  totLen = strlen(fullPath);
  while ( totLen < 2*CA_MAXPATHLEN ) {
    nextLen = 2*CA_MAXPATHLEN-totLen;
    if ( nextLen > 2*CA_MAXNAMELEN ) nextLen = 2*CA_MAXNAMELEN;
    nextLen = 1+(int)(((float)nextLen)*rand()/(RAND_MAX+1.0));
    rc = randomElement(nextElement,nextLen);
    if ( rc == -1 ) {
      fprintf(stdout,"randomElement(%p,%d) returned -1\n",nextElement,nextLen);
      return(-1);
    }
    strcat(fullPath,nextElement);
    printf("Create path %s\n",fullPath);
    rc = rfio_mkdir(fullPath,0755);
    if ( rc == -1 ) {
      save_errno = rfio_serrno();
      if ( nameTooLong == 1 || strlen(nextElement) > CA_MAXNAMELEN || 
           strlen(fullPath) > CA_MAXPATHLEN ) {
        fprintf(stdout,"rfio_mkdir(%s,0755) expected error %s\n",fullPath,
                rfio_serror());
        fprintf(stdout,"strlen(path)=%d (max %d), strlen(name)=%d (max %d)\n",
                strlen(fullPath),CA_MAXPATHLEN,
                strlen(nextElement),CA_MAXNAMELEN);
        nameTooLong = 1;
      } else {
        fprintf(stderr,"rfio_mkdir(%s,0755): %s\n",fullPath,rfio_serror());
        return(-1);
      }
    } else {
      if ( nameTooLong == 1 || strlen(nextElement) > CA_MAXNAMELEN || 
           strlen(fullPath) > CA_MAXPATHLEN ) {
        fprintf(stdout,"rfio_mkdir(%s,0755) unexpected success\n",fullPath);
        fprintf(stdout,"strlen(path)=%d (max %d), strlen(name)=%d (max %d)\n",
                strlen(fullPath),CA_MAXPATHLEN,
                strlen(nextElement),CA_MAXNAMELEN);
        if ( strlen(nextElement) < 256 && strlen(fullPath) <= CA_MAXPATHLEN ) {
          fprintf(stdout,"Accept because strlen(name)=%d < 256\n",
                  strlen(nextElement));
        } else {
          return(-1);
        }
      }      
    }
    
    rc = rfio_access(fullPath,R_OK);
    if ( rc == -1 ) {
      save_errno = rfio_serrno();
      if ( nameTooLong == 1 && 
           (save_errno == SENAMETOOLONG || save_errno == ENAMETOOLONG || 
            save_errno == ENOENT ) ) {
        fprintf(stdout,"rfio_access(%s,0x%x) expected error %s\n",
                fullPath,R_OK,rfio_serror());
      } else {
        fprintf(stdout,"rfio_access(%s,0x%x): unexpected error %s\n",
                fullPath,R_OK,rfio_serror());
        return(-1);
      }
    }
    rc = rfio_stat(fullPath,&st);
    if ( rc == -1 ) {
      save_errno = rfio_serrno();
      if ( nameTooLong == 1 && 
           (save_errno == SENAMETOOLONG || save_errno == ENAMETOOLONG ||
            save_errno == ENOENT ) ) {
        fprintf(stdout,"rfio_stat(%s,%p) expected error %s\n",
                fullPath,&st,rfio_serror());
      } else {
        fprintf(stdout,"rfio_stat(%s,%p): unexpected error %s\n",
                fullPath,&st,rfio_serror());
        return(-1);
      }
    }

    if ( doOpen == 1 ) {
      rc = rfio_open(fullPath,O_RDWR,0755);
      if ( rc == -1 ) {
        save_errno = rfio_serrno();
        if ( nameTooLong == 1 && 
             (save_errno == SENAMETOOLONG || save_errno == ENAMETOOLONG ||
              save_errno == ENOENT) ) {
          fprintf(stdout,"rfio_open(%s) expected error %s\n",
                  fullPath,rfio_serror());
        } else {
          if ( save_errno != EISDIR ) {
            fprintf(stdout,"rfio_stat(%s): unexpected error %s\n",
                    fullPath,rfio_serror());
            return(-1);
          } else {
            fprintf(stdout,"rfio_open(%s) expected error %s\n",
                    fullPath,rfio_serror());
          }        
        }
      } else {
        fprintf(stdout,"rfio_open(%s) unexpected success\n",fullPath);
        rfio_close(rc);
        return(-1);
      }
    }
 
    totLen = strlen(fullPath);
    if ( totLen < 2*CA_MAXPATHLEN ) strcat(fullPath,"/");
    totLen = strlen(fullPath);
  }
  fullPath[CA_MAXPATHLEN] = '\0';
  rc = rmPath(rootPath,fullPath);
  return(rc);
}

int rmPath(char *rootPath, char *path) 
{
  struct stat st;
  int rc, save_errno, nameTooLong = 0;
  char *p;
  if ( rootPath == NULL || path == NULL ) {
    fprintf(stdout,"rmPath() called with NULL argument (%p,%p)\n",
            rootPath,path);
    return(-1);
  }
  p = strstr(path,rootPath);
  if ( p != path ) {
    fprintf(stdout,"rmPath() path %s does not begin with rootPath %s\n",
            path,rootPath);
    return(-1);
  }

  rc = 0;
  while ( (p == path) && (strcmp(p,rootPath) != 0) ) {
    fprintf(stdout,"rmPath() remove %s\n",path);
    if (strlen(path) > CA_MAXPATHLEN ) nameTooLong = 1;
    rc = rfio_stat(path,&st);
    if ( rc == -1 ) {
      save_errno = rfio_serrno();
      fprintf(stdout,"rmPath rfio_stat(%s): %s\n",path,rfio_serror());
    }
    rc = rfio_rmdir(path);
    if ( rc == -1 ) {
      save_errno = rfio_serrno();
      fprintf(stdout,"rmPath() rfio_rmdir(%s): %s\n",path,rfio_serror());
      if ( nameTooLong == 1 && 
           (save_errno == SENAMETOOLONG || save_errno == ENAMETOOLONG || 
            save_errno == ENOENT ) ) rc = 0;
    }
    p = strrchr(path,'/');
    if ( p != NULL ) *p = '\0';
    else break;
    if ( strlen(p+1) > CA_MAXNAMELEN ) nameTooLong = 1;
    p = strstr(path,rootPath);
  }
  if ( rc == -1 ) serrno = save_errno;
  return(rc);
}  

int main(int argc, char *argv[]) 
{
  char *rootPath;
  char *nextElement;
  char *fullPath;
  int i, rc, c;
  
  if ( argc < 2 ) {
    usage(argv[0]);
    return(2);
  }

  Coptind = 1;
  Copterr = 1;

  while ( (c = Cgetopt(argc, argv, "o")) != -1 ) {
    switch (c) {
    case 'o':
      doOpen = 1;
      break;
    default:
      usage(argv[0]);
      return(2);
    }  
  }

  if ( argc <= Coptind ) {
    usage(argv[0]);
    return(2);
  }

  for (i=Coptind; i<argc; i++) {
    rootPath = argv[i];
    rc = doValidPathTest(rootPath);
    if ( rc == -1 ) break;
    rc = doInvalidPathTest(rootPath);
    if ( rc == -1 ) break;
  }
  if ( rc == 0 ) {    
    fprintf(stdout,"\n\n==> ALL TESTS OK <==\n");
    return(0);
  } else {
    fprintf(stdout,"Test failed: %s\n",rfio_serror());
    return(1);
  }
}
