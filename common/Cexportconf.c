/*
 * $Id: Cexportconf.c,v 1.1 2002/08/15 09:18:28 bcouturi Exp $
 */

/*   
 * Copyright (C) 1993-2000 by CERN/IT/DS/HSM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cexportconf.c,v $ $Revision: 1.1 $ $Date: 2002/08/15 09:18:28 $ CERN IT/DS/HSM Benjamin Couturier";
#endif /* not lint */

#include "Castor_limits.h"
#include "osdep.h"

#if !defined(_WIN32)
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/resource.h>  /* For getrlimit() */
#include <unistd.h>
#endif

#define EXPORTCONF_FILENAME "rlimit"


int DLL_DECL Cexportconf(dirname) 
     char *dirname;
{ 

#if !defined(_WIN32)

  int fd;
  char filename[CA_MAXPATHLEN +1];
  int i;
  char buf[500];
  int nb_limits = 0;

  /* The code below is for unices. With WIN32, this function just returns 0 */

  int rlimit_values[] = {  RLIMIT_CPU,
			   RLIMIT_FSIZE   ,
			   RLIMIT_DATA    ,
			   RLIMIT_STACK   ,
			   RLIMIT_CORE    ,
#if defined(RLIMIT_RSS)
			   RLIMIT_RSS     ,
#endif
#if defined(RLIMIT_VMEM)
			   RLIMIT_VMEM,
#endif
#if defined(RLIMIT_NPROC)
			   RLIMIT_NPROC   ,
#endif
			   RLIMIT_NOFILE  ,
#if defined(RLIMIT_MEMLOCK)
			   RLIMIT_MEMLOCK ,
#endif
			   RLIMIT_AS };

  char rlimit_labels[][30] = { "RLIMIT_CPU", 
			       "RLIMIT_FSIZE", 
			       "RLIMIT_DATA", 
			       "RLIMIT_STACK",
			       "RLIMIT_CORE", 
#if defined(RLIMIT_RSS)
			       "RLIMIT_RSS", 
#endif
#if defined(RLIMIT_VMEM)
			       "RLIMIT_VMEM",
#endif
#if defined(RLIMIT_NPROC)
			       "RLIMIT_NPROC", 
#endif
			       "RLIMIT_NOFILE", 
#if defined(RLIMIT_MEMLOCK)
			       "RLIMIT_MEMLOCK", 
#endif
			       "RLIMIT_AS" };
 

  /* 
   * Checking the directory name given as argument 
   */
  if (dirname == NULL || strlen(dirname) <= 0) {
    errno = EINVAL;
    return(-1);
  }
  
  if ( strlen(dirname) > 
       ( CA_MAXPATHLEN - sizeof(EXPORTCONF_FILENAME))) {
    errno = ENAMETOOLONG;
    return(-1);
  }

  /*
   * Building the complete filename
   */
  strcpy(filename, dirname);

  /* Checking whether the string is finished by "/" */
  if (dirname[strlen(dirname) -1] != '/') {
    strcat(filename, "/");
  }
  
  strcat(filename, EXPORTCONF_FILENAME);

  /*
   * Now opening the file and writing
   */
  if ((fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0666)) == -1) {
    return(-1);
  }

  nb_limits = sizeof(rlimit_values) / sizeof(int);

  for(i=0; i < nb_limits; i++) {
  
    struct rlimit rlim;

    if ((getrlimit(rlimit_values[i], &rlim)) == -1) {
      close(fd);
      return(-1);
    }
    
    memset(buf, 0, sizeof(buf));
    sprintf(buf, "%s\t%d\t%d\n", rlimit_labels[i], rlim.rlim_cur, rlim.rlim_max);
    
    if ((write(fd, buf, strlen(buf) )) == -1) {
      close(fd);
      return(-1);
    }
  }

  close(fd);

#endif

  return(0);

}





