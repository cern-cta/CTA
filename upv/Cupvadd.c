/*
 * Copyright (C) 2000-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cupvadd.c,v $ $Revision: 1.1 $ $Date: 2002/05/28 09:37:58 $ CERN IT-DS/HSM Ben Couturier";
#endif /* not lint */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "Cgetopt.h"
#include "serrno.h"
#include "Cupv_api.h"

#define CUPVMODIFYNAME "Cupvmodify"

main(argc, argv)
int argc;
char **argv;
{
  int c;
  int errflg = 0;
  char *dp;
  static struct Coptions longopts[] = {
    {"uid", REQUIRED_ARGUMENT, 0, OPT_UID},
    {"gid", REQUIRED_ARGUMENT, 0, OPT_GID},
    {"src", REQUIRED_ARGUMENT, 0, OPT_SRC},
    {"tgt", REQUIRED_ARGUMENT, 0, OPT_TGT},
    {"priv", REQUIRED_ARGUMENT, 0, OPT_PRV},
    {"user", REQUIRED_ARGUMENT, 0, OPT_USR},
    {"group", REQUIRED_ARGUMENT, 0, OPT_GRP},
    {0, 0, 0, 0}
  };
  uid_t uid = -1;
  gid_t gid = -1;
  char src[CA_MAXHOSTNAMELEN + 1];
  char tgt[CA_MAXHOSTNAMELEN + 1];
  char usr[CA_MAXUSRNAMELEN + 1];
  char grp[CA_MAXGRPNAMELEN + 1];
  int priv;
  src[0]=0;
  tgt[0]=0;
  usr[0] = 0;
  grp[0] = 0;

  Copterr = 1;
  Coptind = 1;
  while ((c = Cgetopt_long (argc, argv, "", longopts, NULL)) != EOF) {
    switch (c) {
    case OPT_UID:
      if (Cupv_strtoi(&uid, Coptarg, &dp, 10) == -1) {
	errflg++;
      }
      break;
    case OPT_GID:
      if (Cupv_strtoi(&gid, Coptarg, &dp, 10) == -1) {
	errflg++;
      }
      break;
    case OPT_SRC:
      strcpy(src, Coptarg);
      break;
    case OPT_TGT:
      strcpy(tgt, Coptarg);
      break;
    case OPT_USR:
      strcpy(usr, Coptarg);
      break;
    case OPT_GRP:
      strcpy(grp, Coptarg);
      break;
    case OPT_PRV:
      priv = Cupv_parse_privstring(Coptarg);
      break;
    case '?':
      errflg++;
      break;
    default:
      break;
    }
  }

  if ((Coptind < argc) ||  ( uid == -1 && usr[0] == 0)  || ( gid == -1 && grp[0] == 0)  || 
      src[0] == 0 || tgt[0] == 0 || priv == -1) { 
    errflg++; 
  } 

  if (errflg) {
    fprintf (stderr, "usage: %s %s%s", argv[0],
	     "--uid uid --gid gid --src SourceHost --tgt TargetHost --priv privilege\n",
	     "Where priv is one of: OPER, TP_OPER, ADMIN, EXPT_ADMIN or NONE\n");
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

  /* Adding a line */
  if (Cupv_add (uid, gid, src, tgt, priv) < 0) {
    fprintf (stderr, "%s: %s\n", argv[0], sstrerror(serrno));
    exit (USERR);
  }
  
  exit (0);
}















