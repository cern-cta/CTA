/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)Cupvlist.c,v 1.5 2002/06/12 08:17:11 CERN IT-DS/HSM Ben Couturier";
#endif /* not lint */

/*      Cupvlist - list privilege entries */

#include <errno.h> 
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "Cgetopt.h"
#include "serrno.h"
#include "u64subr.h"
#include "Cupv.h"
#include "Cupv_api.h"

int displayLine(struct Cupv_userpriv *lp, int verbose);

int main(int argc,char **argv)
{
  int c;
  int flags;
  int errflg = 0;
  int verbose = 0;
  int tmp;
  int priv_specified = 0;

  Cupv_entry_list list;
  struct Cupv_userpriv *lp;
  struct Cupv_userpriv filter;
  int nbentry = 0;
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
  char src[CA_MAXREGEXPLEN + 1];
  char tgt[CA_MAXREGEXPLEN + 1];
  int priv = -1;  
  char usr[CA_MAXUSRNAMELEN + 1];
  char grp[MAXGRPNAMELEN + 1];

/*    char tmpbuf[8]; */
#if defined(_WIN32)
  WSADATA wsadata;
#endif


  usr[0] = 0;
  grp[0] = 0;
  src[0]=0;
  tgt[0]=0;

  Copterr = 1;
  Coptind = 1;
  while ((c = Cgetopt_long (argc, argv, "v", longopts, NULL)) != EOF) {
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
    case OPT_PRV:
      priv = Cupv_parse_privstring(Coptarg);
      priv_specified = 1;
      break;
    case 'v':
      verbose = 1;
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

  if (errflg) {
    fprintf (stderr, "usage: %s %s%s%s%s", argv[0],
	     "[--uid uid | --user username]  [--gid gid | --group groupname] \n\t [--src SourceHost] [--tgt TargetHost] [--priv privilege]\n",  "Where priv is one of:", STR_PRIV_LIST, "\n");
    exit (USERR);
  }
 

  if (gid == -1 && grp[0] != 0) {
    if ( (gid = Cupv_getgid(grp) ) == -1 ) {
      fprintf (stderr, "%s: %s\n", argv[0], sstrerror(serrno));
      exit(USERR);
    }
  }

  if (uid == -1 && usr[0] != 0) {
    if ( (uid = Cupv_getuid(usr) ) == -1 ) {
      fprintf (stderr, "%s: %s\n", argv[0], sstrerror(serrno));
      exit(USERR);
    }
  }

  if (priv == -1 && priv_specified == 1) {
      fprintf (stderr, "%s: The privilege must be one of: %s\n", argv[0],
               STR_PRIV_LIST);
      exit(USERR);


  }

  filter.uid = uid;
  filter.gid = gid;
  strcpy(filter.srchost, src);
  strcpy(filter.tgthost, tgt); 
  filter.privcat = priv;

#if defined(_WIN32)
  if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
    fprintf (stderr, CUP52);
    exit (SYERR);
  }
#endif

  flags = CUPV_LIST_BEGIN;
  while ((lp = Cupv_list (flags, &list, &filter)) != NULL) {
    
    /* Prining header if necessary */
    if (nbentry == 0 && verbose != 1) {
      printf ("       uid        gid               source               target    privilege\n");
      nbentry = 1;
    }

    displayLine(lp, verbose);

    flags = CUPV_LIST_CONTINUE;
  }
  (void) Cupv_list(CUPV_LIST_END, &list, &filter);

#if defined(_WIN32)
  WSACleanup();
#endif
  exit (0);
}

int displayLine(struct Cupv_userpriv *lp, int verbose) {

  char usr[CA_MAXUSRNAMELEN + 50];
  char grp[MAXGRPNAMELEN + 50];
  char buf[MAXPRIVSTRLEN + 1];
  char *c, *p;


  Cupv_build_privstring(lp->privcat, buf);

  if ( (c = Cupv_getuname(lp->uid)) == NULL) {
    if (verbose) {
      sprintf(usr, "--uid %d", lp->uid);
    } else {
      sprintf(usr, "%d", lp->uid);
    }
  } else {
    if (verbose) {
      sprintf(usr, "--user '%s'", c);
    } else {
      strcpy(usr, c);
    }
  }

  if ( (c = Cupv_getgname(lp->gid)) == NULL) {
    if (verbose) {
      sprintf(grp, "--gid %d", lp->gid);
    } else {
      sprintf(grp, "%d", lp->gid);
    }
  } else {
    if (verbose) {
      p = grp;
      sprintf(p, "--group '%s'", c); 
    } else {
      strcpy(grp, c);
    }
  }

  if (verbose) {
    printf("%s %s --src '%s' --tgt '%s' --priv '%s'\n", usr, grp, 
	   lp->srchost, lp->tgthost, buf); 
  } else {
    printf ("%10s %10s %20s %20s    %s\n", usr, grp, 
	    lp->srchost, lp->tgthost, buf); 
  }

  return(0);
}





