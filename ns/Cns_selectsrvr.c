/*
 * Copyright (C) 2000-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* Cns_selectsrvr - select the CASTOR Name Server hostname */

/* The following rules apply:
 * if the path is in the form server:pathname, "server" is used else
 * if the environment variable CNS_HOST is set, its value is used else
 * if CNS HOST is defined in /etc/castor/castor.conf, the value is used else
 * the second component of path is the domain name and the third component
 * is prefixed by the value of NsHostPfx to give the host name or its alias
 */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include "Castor_limits.h"
#include "Cns_api.h"
#include "Cns.h"
#include "serrno.h"

#define strtok(X,Y) strtok_r(X,Y,&last)

int
Cns_selectsrvr(const char *path,
	       char *current_directory_server,
	       char *server,
	       char **actual_path)
{
  char buffer[CA_MAXPATHLEN+1];
  char *domain;
  char func[16];
  char *getconfent();
  char *getenv();
  char *last = NULL;
  int n;
  char *p;
  char *securemode;
  int securityOpt=0;

  strncpy (func, "Cns_selectsrvr", 16);
  if (! path || ! server || ! actual_path) {
    serrno = EFAULT;
    return (-1);
  }

  if (*path != '/' && (p = strstr (path, ":/"))) {
    n = p - path;
    if (n > CA_MAXHOSTNAMELEN) {
      serrno = EINVAL;
      return (-1);
    }
    strncpy (server, path, n);
    *(server + n) = '\0';
    *actual_path = p + 1;
  } else {
    *actual_path = (char *) path;
    if ((p = getenv (CNS_HOST_ENV)) ||
	(p = getconfent (CNS_SCE, "HOST", 0))) {
      if (strlen (p) > CA_MAXHOSTNAMELEN) {
        serrno = EINVAL;
        return (-1);
      }
      strcpy (server, p);
    } else {
      if (*path != '/') { /* not a full path */
        if (*current_directory_server) /* set by chdir */
          strcpy (server, current_directory_server);
        else
          server[0] = '\0'; /* use localhost */
        return (0);
      }
      strcpy (buffer, path);
      if (! strtok (buffer, "/") ||
          (domain = strtok (NULL, "/")) == NULL ||
          (p = strtok (NULL, "/")) == NULL) {
        server[0] = '\0'; /* use localhost */
        return (0);
      }
      if ((securemode = getenv ("SECURE_CASTOR")) ||
	  (securemode = getconfent(CNS_SCE, "SECURITY", 0))) {
        securityOpt = (strcasecmp(securemode, "YES") == 0);
      }
      if (securityOpt) {
        if (strlen (SCNSHOSTPFX) + strlen (p) + strlen (domain) +
            1 > CA_MAXHOSTNAMELEN) {
          serrno = EINVAL;
          return (-1);
        }
        sprintf (server, "%s%s.%s", SCNSHOSTPFX, p, domain);
      } else {
        if (strlen (CNSHOSTPFX) + strlen (p) + strlen (domain) +
            1 > CA_MAXHOSTNAMELEN) {
          serrno = EINVAL;
          return (-1);
        }
        sprintf (server, "%s%s.%s", CNSHOSTPFX, p, domain);
      }
    }
  }
  return (0);
}
