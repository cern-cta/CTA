/*
 * Copyright (C) 1999-2004 by CERN/IT/DM
 * All rights reserved
 */

/* nssetsegment - utility to set the checksum and status of a tape segment */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"
#include "u64subr.h"

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s [OPTION]... PATH\n", name);
    printf ("Utility to change the checksum and status for a tape segment.\n\n");
    printf ("  -c, --copyno=COPYNO      specifies which copy of the file should be modified\n");
    printf ("  -s, --segmentno=SEGNO    specifies which tape segment that should be modified\n");
    printf ("  -n, --name=CKSUMNAME     the name of the checksum to be stored\n");
    printf ("  -k, --checksum=CKSUM     the value of the checksum to be stored\n");
    printf ("  -u, --update             use the Cns_updateseg_checksum call rather than Cns_replaceseg\n");
    printf ("      --clear              remove the checksum value\n");
    printf ("  -e, --enable             enable the tape segment\n");
    printf ("  -d, --disable            disable the tape segment\n");
    printf ("  -x, --segsize=SEGSIZE    the size of the segment in bytes\n");
    printf ("      --help               display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}

int main(int argc,
         char **argv)
{
  int c;
  int i;
  int nbseg    = 0;
  int found    = 0;
  int copyno   = -1;
  int fsec     = -1;
  int errflg   = 0;
  int hflg     = 0;
  int xflg     = 0;
  int enable   = 0;
  int disable  = 0;
  int clear    = 0;
  int update   = 0;
  char status  = '-';
  char *path   = NULL;
  char *dp     = NULL;
  char *p;
  char filepath[CA_MAXPATHLEN+1];
  struct Cns_filestat stat;
  struct Cns_segattrs *segattrs;
  struct Cns_segattrs newsegattrs;

  unsigned int checksum = 0;
  u_signed64 segsize = 0;
  char chksumvalue[CA_MAXCKSUMLEN+1];
  char chksumname[CA_MAXCKSUMNAMELEN+1];

  Coptions_t longopts[] = {
    { "checksum",  REQUIRED_ARGUMENT, NULL,   'k' },
    { "clear",     NO_ARGUMENT,       &clear,  1  },
    { "copyno",    REQUIRED_ARGUMENT, NULL,   'c' },
    { "disable",   NO_ARGUMENT,       NULL,   'd' },
    { "enable",    NO_ARGUMENT,       NULL,   'e' },
    { "segmentno", REQUIRED_ARGUMENT, NULL,   's' },
    { "name",      REQUIRED_ARGUMENT, NULL,   'n' },
    { "segsize",   REQUIRED_ARGUMENT, NULL,   'x' },
    { "update",    NO_ARGUMENT,       &update, 1  },
    { "help",      NO_ARGUMENT,       &hflg,   1  },
    { NULL,        0,                 NULL,    0  }
  };

  Coptind = 1;
  Copterr = 1;
  chksumvalue[0] = chksumname[0] = '\0';
  while ((c = Cgetopt_long (argc, argv, "edc:s:k:n:x:", longopts, NULL)) != EOF && !errflg) {
    switch(c) {
    case 'e':
      status = '-';
      enable++;
      break;
    case 'd':
      status = 'D';
      disable++;
      break;
    case 'c':
      if (((copyno = strtol (Coptarg, &dp, 10)) <= 0) || (*dp != '\0')) {
	fprintf (stderr, "%s: invalid copy number: %s\n", argv[0], Coptarg);
	errflg++;
      }
      break;
    case 's':
      if (((fsec = strtol (Coptarg, &dp, 10)) <= 0) || (*dp != '\0')) {
	fprintf (stderr, "%s: invalid segment number: %s\n", argv[0], Coptarg);
	errflg++;
      }
      break;
    case 'k':
      checksum = strtoul (Coptarg, &dp, 16);
      if (*dp != '\0') {
	fprintf (stderr, "%s: invalid checksum: %s\n", argv[0], Coptarg);
	errflg++;
      }
      if (strlen(Coptarg) > CA_MAXCKSUMLEN) {
	fprintf (stderr, "%s: checksum value: %s exceeds %d characters in length\n",
		 argv[0], Coptarg, CA_MAXCKSUMLEN);
	errflg++;
      }
      strncpy(chksumvalue, Coptarg, CA_MAXCKSUMLEN);
      chksumvalue[CA_MAXCKSUMLEN] = '\0';
      break;
    case 'n':
      if (strlen(Coptarg) > CA_MAXCKSUMNAMELEN) {
	fprintf (stderr, "%s: checksum name: %s exceeds %d characters in length\n",
		 argv[0], Coptarg, CA_MAXCKSUMNAMELEN);
	errflg++;
      }
      strncpy(chksumname, Coptarg, CA_MAXCKSUMNAMELEN);
      chksumname[CA_MAXCKSUMNAMELEN] = '\0';
      break;

    case 'x':
      xflg++;
      if (((segsize = strtoull (Coptarg, &dp, 10)) <= 0) || (*dp != '\0') ||
	  (Coptarg[0] == '-') | (errno == ERANGE)) {
	fprintf (stderr, "%s: invalid segment size: %s\n", argv[0], Coptarg);
	errflg++;
      }
      break;
    case '?':
    case ':':
      errflg++;
      break;
    default:
      break;
    }
  }
  if (hflg) {
    usage (0, argv[0]);
  }

  /* Check command line arguments */
  if (errflg || (Coptind >= argc)) {  /* Too few arguments */
    errflg++;
  } else if (fsec < 0) {
    fprintf (stderr, "%s: mandatory option -s, --segmentno is missing\n", argv[0]);
    errflg++;
  } else if (copyno < 0) {
    fprintf (stderr, "%s: mandatory option -c, --copyno is missing\n", argv[0]);
    errflg++;
  } else {
    /* Changes to segment status */
    if (enable || disable) {
      if (enable && disable) {  /* Mutually exclusivity */
	fprintf (stderr, "%s: options -e, --enable and -d, --disable are mutually exclusive\n", argv[0]);
	errflg++;
      }
    }
    /* Changes to segment checksum */
    if (clear && !errflg) {
      if (chksumname[0] != '\0') {
	fprintf (stderr, "%s: option -n, --name cannot be specified with --clear\n", argv[0]);
	errflg++;
      } else if (chksumvalue[0] != '\0') {
	fprintf (stderr, "%s: option -k, --checksum cannot be specified with --clear\n", argv[0]);
	errflg++;
      }
    }
    /* Changes to the segment size */
    if (clear && xflg && !errflg) {
      fprintf (stderr, "%s: option -x, --segsize cannot be specified with --clear\n", argv[0]);
      errflg++;
    }
    if (update && xflg && !errflg) {
      fprintf (stderr, "%s: option -x, --segsize cannot be specified with --update\n", argv[0]);
      errflg++;
    }
    if (update && clear && !errflg) {
      fprintf (stderr, "%s option -u, --update and --clear are mutually exclusive\n", argv[0]);
      errflg++;
    }
    /* Verify that there is at least some action to perform! */
    if (!errflg && (!enable && !disable && !clear && !xflg)) {
      if ((chksumname[0] == '\0') || (chksumvalue[0] == '\0')) {
	fprintf (stderr, "%s: no actions to perform\n", argv[0]);
	errflg++;
      }
    }
  }
  if (errflg) {
    usage (USERR, argv[0]);
  }
  path = argv[Coptind];
  if (*path != '/' && strstr (path, ":/") == NULL) {
    if ((p = getenv (CNS_HOME_ENV)) == NULL ||
	strlen (p) + strlen (path) + 1 > CA_MAXPATHLEN) {
      fprintf (stderr, "%s: %s: invalid path\n", argv[0], path);
      errflg++;
    } else
      sprintf (filepath, "%s/%s", p, path);
  } else {
    if (strlen (path) > CA_MAXPATHLEN) {
      fprintf (stderr, "%s: %s: %s\n", argv[0], path,
	       sstrerror(SENAMETOOLONG));
      errflg++;
    } else
      strcpy (filepath, path);
  }

  /* Stat the file to get the fileid */
  if (Cns_stat(filepath, &stat)) {
    fprintf (stderr, "%s: %s: %s\n", argv[0], filepath, sstrerror(serrno));
    exit (USERR);
  }

  /* Lookup the segments for the file */
  if (Cns_getsegattrs(filepath, NULL, &nbseg, &segattrs)) {
    fprintf (stderr, "%s: %s: %s\n", argv[0], filepath, sstrerror(serrno));
    exit (USERR);
  }
  for (i = 0; i < nbseg; i++) {
    if ((segattrs[i].copyno != copyno) ||
	(segattrs[i].fsec   != fsec)) {
      continue;  /* not interested */
    }
    found++;

    /* Process status changes */
    if ((enable  && (segattrs[i].s_status != '-')) ||
	(disable && (segattrs[i].s_status != 'D'))) {
      if (Cns_updateseg_status(NULL, stat.fileid, &(segattrs[i]), status)) {
	fprintf (stderr, "%s: failed to update segment status: %s\n",
		 argv[0], sstrerror(serrno));
	errflg++;
	break;
      }
    }

    /* Process segment size changes */
    newsegattrs = segattrs[i];
    if (xflg) {
      newsegattrs.segsize = segsize;
    }

    /* Process checksum changes */
    if (clear) {
      if ((strcmp(segattrs[i].checksum_name, "") == 0) &&
	  segattrs[i].checksum == 0) {
	break;  /* nothing to do */
      }

      /* Reset checksum fields */
      newsegattrs.checksum_name[0] = '\0';
      newsegattrs.checksum         = 0;
    } else if ((chksumname[0] != '\0') && (chksumvalue[0] != '\0')) {
      if ((strcmp(segattrs[i].checksum_name, chksumname) == 0) &&
	  segattrs[i].checksum == checksum) {
	break;  /* nothing to do */
      }

      /* Set checksum fields */
      strncpy(newsegattrs.checksum_name, chksumname, CA_MAXCKSUMNAMELEN);
      newsegattrs.checksum = checksum;
    } else if (!xflg) {
      break;
    }

    /* Update the checksum */
    if (update) {
      if (Cns_updateseg_checksum(NULL, stat.fileid, &(segattrs[i]), &newsegattrs)) {
	fprintf (stderr, "%s: failed to update segment checksum: %s\n",
		 argv[0], sstrerror(serrno));
	errflg++;
      }
    } else {
      if (Cns_replaceseg(NULL, stat.fileid, &(segattrs[i]), &newsegattrs, stat.mtime)) {
	fprintf (stderr, "%s: failed to replace segment checksum: %s\n",
		 argv[0], sstrerror(serrno));
	errflg++;
      }
    }

    break;
  }
  if (nbseg > 0)
    free(segattrs);

  if (found == 0) {
    fprintf (stderr, "%s: %s: no segments found\n", argv[0], filepath);
    errflg++;
  }

  if (errflg)
    exit (USERR);
  exit (0);
}
