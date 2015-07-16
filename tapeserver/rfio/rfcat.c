/*
 * $Id: rfcat.c,v 1.8 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 2001-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include "rfio_api.h"

int catfile(char *inpfile)
{
  char buf [32768];
  int c;
  int rc;
  FILE *s;
  int v;

  /* Streaming opening is always better */
  v = RFIO_STREAM;
  rfiosetopt (RFIO_READOPT, &v, 4);

  if (strcmp (inpfile, "-") == 0)
    s  = stdin;
  else if ((s = rfio_fopen64 (inpfile, "r")) == NULL) {
    rfio_perror (inpfile);
    return (1);
  }
  while ((c = rfio_fread (buf, 1, sizeof(buf), s)) > 0) {
    if ((rc = fwrite (buf, 1, c, stdout)) < c) {
      fprintf (stderr, "rfcat %s: %s\n", inpfile,
               strerror ((rc < 0) ? errno : ENOSPC));
      if (strcmp (inpfile, "-"))
        rfio_fclose (s);
      return (1);
    }
  }
  if (strcmp (inpfile, "-"))
    rfio_fclose (s);
  return (0);
}

int main(int argc,
         char **argv)
{
  int errflg = 0;
  int i;

  if (argc == 1)
    errflg = catfile ("-");
  else
    for (i = 1; i < argc; i++)
      errflg += catfile (argv[i]);
  exit (errflg ? 1 : 0);
}
