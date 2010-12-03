/*
 * Copyright (C) 2000-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*      nslisttape - list the file segments residing on a volume */
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"
#include "u64subr.h"
#include "getconfent.h"

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s -V VID [OPTION]...\n", name);
    printf ("List the file segments residing on a volume/tape.\n\n");
    printf ("  -V=VID               specifies the visual identifier for the volume\n");
    printf ("  -i                   print the file uniqueid in front of each entry\n");
    printf ("  -H                   print sizes in human readable format (e.g., 1K 234M 2G) using\n");
    printf ("                       powers of 1024\n");
    printf ("  -D                   only display/count logical deleted segments\n");
    printf ("  -f=FSEQ              restrict the output to the given file sequence number\n");
    printf ("  -s, --summarize      display only the total number of files, total size and largest fileid\n");
    printf ("                       of all files on the volume\n");
    printf ("  -h, --host=HOSTNAME  the name server to query\n");
    printf ("      --ds             print the vid followed by a slash followed by the media side\n");
    printf ("                       number. This option is useful for multi-sided media like DVD.\n");
    printf ("      --checksum       display the tape segments checksum\n");
    printf ("      --help           display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}

int main(int argc,char **argv)
{

  struct Cns_direntape *dtp;
  int c;
  static int dsflag = 0;
  static int checksumflag = 0;
  static int sumflag = 0;
  int errflg = 0;
  int iflag = 0;
  int dflag = 0;
  int hflg = 0;
  int humanflag = 0;
  int flags;
  Cns_list list;
  signed64 parent_fileid = -1;
  char path[CA_MAXPATHLEN+1];
  char *server = NULL;
  char tmpbuf[21];
  char tmpbuf2[21];
  char tmpbuf3[21];
  char tmpbuf4[21];
  char *vid = NULL;
  int fseq = 0;
  u_signed64 count = 0;
  u_signed64 size = 0;
  u_signed64 maxfileid = 0;
  u_signed64 avgcompression = 0;

  Coptions_t longopts[] = {
    { "display_side", NO_ARGUMENT,       &dsflag,       1  },
    { "checksum",     NO_ARGUMENT,       &checksumflag, 1  },
    { "ds",           NO_ARGUMENT,       &dsflag,       1  },
    { "summarize",    NO_ARGUMENT,       &sumflag,      1  },
    { "host",         REQUIRED_ARGUMENT, NULL,         'h' },
    { "help",         NO_ARGUMENT,       &hflg,         1  },
    { NULL,           0,                 NULL,          0  }
  };

  Copterr = 1;
  Coptind = 1;
  while ((c = Cgetopt_long (argc, argv, "Hh:V:isf:D", longopts, NULL)) != EOF) {
    switch (c) {
    case 'h':
      server = Coptarg;
      setenv(CNS_HOST_ENV, server, 1);
      break;
    case 'H':
      humanflag++;
      break;
    case 'V':
      vid = Coptarg;
      break;
    case 'i':
      iflag++;
      break;
    case 's':
      sumflag++;
      break;
    case 'f':
      fseq = (int) strtol(Coptarg, (char **)NULL, 10);
      if ((errno != 0) || (fseq == 0)) {
        fprintf (stderr, "invalid file sequence number: %s\n", Coptarg);
        exit (USERR);
      }
      break;
    case 'D':
      dflag++;
      break;
    case ':':
      errflg++;
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
  if (Coptind < argc || ! vid) {
    errflg++;
  }
  if (errflg) {
    usage (USERR, argv[0]);
  }

  if (sumflag) {
    c = Cns_tapesum(server, vid, &count, &size, &maxfileid, &avgcompression);
    if (c < 0) {
      fprintf (stderr, "%s: %s\n", vid, (serrno == ENOENT) ? "No such volume or no files found" : sstrerror(serrno));
      exit (USERR);
    }
    u64tostr (maxfileid, tmpbuf3, 0);
    u64tostr (avgcompression, tmpbuf4, 0);
    if (humanflag) {
      printf("%s %s %s %s\n", u64tostr (count, tmpbuf, 0), u64tostru (size, tmpbuf2, 0), tmpbuf3, tmpbuf4);
    } else {
      printf("%s %s %s %s\n", u64tostr (count, tmpbuf, 0), u64tostr (size, tmpbuf2, 0), tmpbuf3, tmpbuf4);
    }
    exit (0);
  }

  flags = CNS_LIST_BEGIN;
  serrno = 0;
  while ((dtp = Cns_listtape (server, vid, flags, &list, fseq)) != NULL) {
    if ((signed64)dtp->parent_fileid != parent_fileid) {
      if (Cns_getpath (server, dtp->parent_fileid, path) < 0) {
        fprintf (stderr, "%s\n", sstrerror(serrno));
        exit (USERR);
      }
      parent_fileid = dtp->parent_fileid;
    }
    flags = CNS_LIST_CONTINUE;
    if (dflag && (dtp->s_status != 'D')) {
      continue;
    }
    if (dsflag || dtp->side > 0)
      printf ("%c %d %3d %-6.6s/%d %5d %02x%02x%02x%02x %s %3d",
              dtp->s_status, dtp->copyno, dtp->fsec, dtp->vid,
              dtp->side, dtp->fseq, dtp->blockid[0], dtp->blockid[1],
              dtp->blockid[2], dtp->blockid[3],
              u64tostr (dtp->segsize, tmpbuf, 20), dtp->compression);
    else
      printf ("%c %d %3d %-6.6s   %5d %02x%02x%02x%02x %s %3d",
              dtp->s_status, dtp->copyno, dtp->fsec, dtp->vid,
              dtp->fseq, dtp->blockid[0], dtp->blockid[1],
              dtp->blockid[2], dtp->blockid[3],
              u64tostr (dtp->segsize, tmpbuf, 20), dtp->compression);

    if (checksumflag) {
      if (dtp->checksum_name != NULL &&  strlen(dtp->checksum_name)>0) {
        printf (" %*s %08lx", CA_MAXCKSUMNAMELEN, dtp->checksum_name, dtp->checksum);
      } else {
        printf (" %*s %08x", CA_MAXCKSUMNAMELEN, "-", 0);
      }
    }

    if (iflag) {
      printf (" %s %s/%s\n", u64tostr (dtp->fileid, tmpbuf, 20), path, dtp->d_name);
    } else {
      printf (" %s/%s\n", path, dtp->d_name);
    }
  }
  if (serrno != 0) {
    fprintf (stderr, "%s: %s\n", vid, (serrno == ENOENT) ? "No such volume or no files found" : sstrerror(serrno));
    exit(USERR);
  }
  (void) Cns_listtape (server, vid, CNS_LIST_END, &list, fseq);

  exit (0);
}
