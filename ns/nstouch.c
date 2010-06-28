/*
 * Copyright (C) 2001-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* nstouch - set last access and modification times */
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <stdlib.h>
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"

static time_t cvt_datime();

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s [OPTION]... PATH...\n", name);
    printf ("Change file timestamps.\n\n");
    printf ("  -a               change only the access time\n");
    printf ("  -c, --no-create  do not create any files\n");
    printf ("  -m               change only the modification time\n");
    printf ("  -t=STAMP         use [[CC]YY]MMDDhhmm instead of current time\n");
    printf ("      --help       display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}

int main(int argc,char **argv)
{
  int aflg = 0;
  int c;
  int cflg = 0;
  int hflg = 0;
  int errflg = 0;
  char fullpath[CA_MAXPATHLEN+1];
  int i;
  int mflg = 0;
  time_t newtime;
  char *p;
  char *path;
  struct Cns_filestat statbuf;
  int tflag = 0;
  struct utimbuf times;

  Coptions_t longopts[] = {
    { "no-create", NO_ARGUMENT, NULL, 'c' },
    { "help",      NO_ARGUMENT, &hflg, 1  },
    { NULL,        0,           NULL,  0  }
  };

  Coptind = 1;
  Copterr = 1;

  newtime = time (0);
  while ((c = Cgetopt_long (argc, argv, "acmt:", longopts, NULL)) != EOF) {
    switch (c) {
    case 'a':
      aflg++;
      break;
    case 'c':
      cflg++;
      break;
    case 'm':
      mflg++;
      break;
    case 't':
      tflag++;
      newtime = cvt_datime (Coptarg);
      break;
    case '?':
      errflg++;
      break;
    default:
      break;
    }
  }
  if (hflg) {
    usage (0, argv[0]);
  }
  if (errflg || Coptind >= argc) {
    usage (USERR, argv[0]);
  }

  if (! aflg && ! mflg) {
    aflg = 1;
    mflg = 1;
  }
  for (i = Coptind; i < argc; i++) {
    path = argv[i];
    if (*path != '/' && strstr (path, ":/") == NULL) {
      if ((p = getenv (CNS_HOME_ENV)) == NULL ||
          strlen (p) + strlen (path) + 1 > CA_MAXPATHLEN) {
        fprintf (stderr, "%s: invalid path\n", path);
        errflg++;
        continue;
      } else
        sprintf (fullpath, "%s/%s", p, path);
    } else {
      if (strlen (path) > CA_MAXPATHLEN) {
        fprintf (stderr, "%s: %s\n", path,
                 sstrerror(SENAMETOOLONG));
        errflg++;
        continue;
      } else
        strcpy (fullpath, path);
    }
    if ((c = Cns_stat (fullpath, &statbuf)) == 0) {
      if (aflg)
        times.actime = newtime;
      else
        times.actime = statbuf.atime;
      if (mflg)
        times.modtime = newtime;
      else
        times.modtime = statbuf.mtime;
      c = Cns_utime (fullpath, tflag ? &times : NULL);
    } else if (serrno == ENOENT) {
      if (cflg) continue;
      c = Cns_creat (fullpath, 0666);
    }
    if (c) {
      fprintf (stderr, "%s: %s\n", path,
               sstrerror(serrno));
      errflg++;
    }
  }
  if (errflg)
    exit (USERR);
  exit (0);
}

static time_t
cvt_datime(arg)
     char *arg;
{
  int cc = 0;
  time_t curtime;
  char *dp;
  int l;
  static int lastd[12] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  int n;
  char *p;
  struct tm tm;

  memset (&tm, 0, sizeof(struct tm));
  curtime = time (0);
  tm.tm_year = localtime(&curtime)->tm_year;
  if ((p = strchr (arg, '.'))) {
    *p = '\0';
    if ((n = strtol (p + 1, &dp, 10)) < 0 || *dp != '\0' || n > 59)
      return (-1);
    tm.tm_sec = n;
  }
  l = strlen (arg);
  switch (l) {
  case 8:
    n = sscanf(arg, "%2d%2d%2d%2d", &tm.tm_mon, &tm.tm_mday,
               &tm.tm_hour, &tm.tm_min);
    break;
  case 10:
    n = sscanf(arg, "%2d%2d%2d%2d%2d", &tm.tm_year, &tm.tm_mon,
               &tm.tm_mday, &tm.tm_hour, &tm.tm_min);
    break;
  case 12:
    n = sscanf(arg, "%2d%2d%2d%2d%2d%2d", &cc, &tm.tm_year,
               &tm.tm_mon, &tm.tm_mday, &tm.tm_hour, &tm.tm_min);
    break;
  default:
    return (-1);
  }
  if (n != l/2)
    return (-1);

  if (tm.tm_min < 0 || tm.tm_min > 59)
    return (-1);
  if (tm.tm_hour < 0 || tm.tm_hour > 23)
    return (-1);
  if (tm.tm_mon < 1 || tm.tm_mon > 12)
    return (-1);
  if (cc == 0) {
    if (tm.tm_year < 69)
      tm.tm_year += 100;
  } else
    tm.tm_year += (cc - 19) * 100;

  if ((tm.tm_mon == 2) && (tm.tm_mday == 29) && (tm.tm_year % 4 != 0))
    return (-1);
  if ((tm.tm_mday < 1) || (tm.tm_mday > lastd[tm.tm_mon-1]))
    return (-1);
  tm.tm_mon--;

  tm.tm_isdst = -1;
  return (mktime (&tm));
}
