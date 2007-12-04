/*
 * Copyright (C) 2000-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "Cgetopt.h"
#include "serrno.h"
#include "Cupv_api.h"
#include "Cupv.h"

int main(int argc,char **argv)
{
  int c;
  char *dp;
  int errflg = 0;
  int tmp;
  static struct Coptions longopts[] = {
    {"uid", REQUIRED_ARGUMENT, 0, OPT_UID},
    {"gid", REQUIRED_ARGUMENT, 0, OPT_GID},
    {"src", REQUIRED_ARGUMENT, 0, OPT_SRC},
    {"tgt", REQUIRED_ARGUMENT, 0, OPT_TGT},
    {"user", REQUIRED_ARGUMENT, 0, OPT_USR},
    {"group", REQUIRED_ARGUMENT, 0, OPT_GRP},
    {0, 0, 0, 0}
  };
  uid_t uid = -1;
  gid_t gid = -1;
  char src[CA_MAXREGEXPLEN + 1];
  char tgt[CA_MAXREGEXPLEN + 1]; 
  char usr[CA_MAXUSRNAMELEN + 1];
  char grp[MAXGRPNAMELEN + 1];
#if defined(_WIN32)
    WSADATA wsadata;
#endif

  usr[0] = 0;
  grp[0] = 0;
  src[0]=0;
  tgt[0]=0;

  Copterr = 1;
  Coptind = 1;
  while ((c = Cgetopt_long (argc, argv, "", longopts, NULL)) != EOF) {
    switch (c) {
    case OPT_UID:
      if (Cupv_strtoi(&tmp, Coptarg, &dp, 10) == -1) {
	errflg++;
      } else {
	uid = tmp;
      }
      break;
    case OPT_GID:
      if (Cupv_strtoi(&tmp, Coptarg, &dp, 10) == -1) {
	errflg++;
      } else {
	gid = tmp;
      }
      break;
    case OPT_SRC:
      if (strlen(Coptarg) > CA_MAXREGEXPLEN) {
	fprintf(stderr, "%s: SRC too long\n", argv[0]);
	return(USERR);
      }
      strcpy(src, Coptarg);
      break;
    case OPT_TGT:
      if (strlen(Coptarg) > CA_MAXREGEXPLEN) {
	fprintf(stderr, "%s: TGT too long\n", argv[0]);
	return(USERR);
      }
      strcpy(tgt, Coptarg);
      break;
    case OPT_USR:
      if (strlen(Coptarg) > CA_MAXUSRNAMELEN) {
	fprintf(stderr, "%s: Username too long\n", argv[0]);
	return(USERR);
      }
      strcpy(usr, Coptarg);
      break;
    case OPT_GRP:
      if (strlen(Coptarg) > MAXGRPNAMELEN) {
	fprintf(stderr, "%s: Groupname too long\n", argv[0]);
	return(USERR);
      }
      strcpy(grp, Coptarg);
      break;
    case '?':
      errflg++;
      break;
    default:
      break;
    }
  }

/*    printf("%d/%s  %d/%s\n", uid, usr, gid, grp); */

  if (Coptind < argc  ||  ( uid == -1 && usr[0] == 0) 
      || (gid == -1 && grp[0] == 0) || src[0] == 0 || tgt[0] == 0) {
    errflg++;
  }

 if (errflg) {
    fprintf (stderr, "usage: %s %s", argv[0],
	     "(--uid uid | --user username) (--gid gid | --group groupname) --src SourceHost --tgt TargetHost\n");
    exit (USERR);
  }

  if (gid == -1) {
    if ( (gid = Cupv_getgid(grp) ) == -1 ) {
      fprintf (stderr, "%s: %s\n", argv[0], sstrerror(serrno));
      exit(USERR);
    }
  }

  if (uid == -1) {
    if ( (uid = Cupv_getuid(usr) ) == -1 ) {
      fprintf (stderr, "%s: %s\n", argv[0], sstrerror(serrno));
      exit(USERR);
    }
  }
#if defined(_WIN32)
  if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
    fprintf (stderr, CUP52);
    exit (SYERR);
  }
#endif
  if (Cupv_delete (uid, gid, src, tgt) < 0) {
    fprintf (stderr, "Cupvdelete: %s\n", sstrerror(serrno));
#if defined(_WIN32)
      WSACleanup();
#endif
    exit (USERR);
  }
#if defined(_WIN32)
    WSACleanup();
#endif
  exit (0);
}














