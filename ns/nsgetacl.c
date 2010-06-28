/*
 * Copyright (C) 2003-2005 by CERN/IT/ADC/CA
 * All rights reserved
 */

/* nsgetacl - get the Access Control List for a file/directory */
#include <errno.h>
#include <sys/types.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"

static char *decode_group(gid_t);
static char *decode_perm(unsigned char);
static char *decode_user(uid_t);

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s [OPTION]... PATH..\n", name);
    printf ("List directory/file access control lists.\n\n");
    printf ("  -a          only display the access ACL\n");
    printf ("  -d          only display the default ACL\n");
    printf ("      --help  display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}


int main(argc, argv)
     int argc;
     char **argv;
{
  struct Cns_acl acl[CA_MAXACLENTRIES];
  struct Cns_acl *aclp;
  int c;
  int errflg = 0;
  int aflag = 0;
  int dflag = 0;
  int hflg = 0;
  char fullpath[CA_MAXPATHLEN+1];
  int i;
  int j;
  unsigned char mask = 0xFF;
  int nentries;
  char *p;
  char *path;

  Coptions_t longopts[] = {
    { "help", NO_ARGUMENT, &hflg, 1  },
    { NULL,   0,           NULL,  0  }
  };

  Coptind = 1;
  Copterr = 1;
  while ((c = Cgetopt_long (argc, argv, "ad", longopts, NULL)) != EOF) {
    switch (c) {
    case 'a':
      aflag++;
      break;
    case 'd':
      dflag++;
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
    if ((nentries = Cns_getacl (fullpath, CA_MAXACLENTRIES, acl)) < 0) {
      fprintf (stderr, "%s: %s\n", path, sstrerror(serrno));
      errflg++;
      continue;
    }
    /* Print header */

    printf ("# file: %s\n", path);
    for (j = 0, aclp = acl; j < nentries; j++, aclp++) {
      if (aclp->a_type == CNS_ACL_USER_OBJ)
        printf ("# owner: %s\n", decode_user (aclp->a_id));
      else if (aclp->a_type == CNS_ACL_GROUP_OBJ)
        printf ("# group: %s\n", decode_group (aclp->a_id));
      else if (aclp->a_type == CNS_ACL_MASK) {
        mask = aclp->a_perm;
        break;
      }
    }

    for (j = 0, aclp = acl; j < nentries; j++, aclp++) {
      switch (aclp->a_type) {
      case CNS_ACL_USER_OBJ:
        if (! dflag)
          printf ("user::%s\n",
                  decode_perm (aclp->a_perm));
        break;
      case CNS_ACL_USER:
        if (! dflag) {
          printf ("user:%s:%s\t\t",
                  decode_user(aclp->a_id),
                  decode_perm (aclp->a_perm));
          printf ("#effective:%s\n",
                  decode_perm (aclp->a_perm & mask));
        }
        break;
      case CNS_ACL_GROUP_OBJ:
        if (! dflag) {
          printf ("group::%s\t\t",
                  decode_perm (aclp->a_perm));
          printf ("#effective:%s\n",
                  decode_perm (aclp->a_perm & mask));
        }
        break;
      case CNS_ACL_GROUP:
        if (! dflag) {
          printf ("group:%s:%s\t\t",
                  decode_group (aclp->a_id),
                  decode_perm (aclp->a_perm));
          printf ("#effective:%s\n",
                  decode_perm (aclp->a_perm & mask));
        }
        break;
      case CNS_ACL_MASK:
        if (! dflag)
          printf ("mask::%s\n",
                  decode_perm (aclp->a_perm));
        break;
      case CNS_ACL_OTHER:
        if (! dflag)
          printf ("other::%s\n",
                  decode_perm (aclp->a_perm));
        break;
      case CNS_ACL_USER_OBJ | CNS_ACL_DEFAULT:
        if (! aflag)
          printf ("default:user::%s\n",
                  decode_perm (aclp->a_perm));
        break;
      case CNS_ACL_USER | CNS_ACL_DEFAULT:
        if (! aflag)
          printf ("default:user:%s:%s\n",
                  decode_user (aclp->a_id),
                  decode_perm (aclp->a_perm));
        break;
      case CNS_ACL_GROUP_OBJ | CNS_ACL_DEFAULT:
        if (! aflag)
          printf ("default:group::%s\n",
                  decode_perm (aclp->a_perm));
        break;
      case CNS_ACL_GROUP | CNS_ACL_DEFAULT:
        if (! aflag)
          printf ("default:group:%s:%s\n",
                  decode_group (aclp->a_id),
                  decode_perm (aclp->a_perm));
        break;
      case CNS_ACL_MASK | CNS_ACL_DEFAULT:
        if (! aflag)
          printf ("default:mask::%s\n",
                  decode_perm (aclp->a_perm));
        break;
      case CNS_ACL_OTHER | CNS_ACL_DEFAULT:
        if (! aflag)
          printf ("default:other::%s\n",
                  decode_perm (aclp->a_perm));
        break;
      }
    }
    if (i < argc - 1)
      printf ("\n");
  }
  if (errflg)
    exit (USERR);
  exit (0);
}

static char *
decode_group(gid_t gid)
{
  struct group *gr;
  static gid_t sav_gid = -1;
  static char sav_gidstr[256];

  if (gid != sav_gid) {
#ifdef VIRTUAL_ID
    if (gid == 0)
      return ("root");
    sav_gid = gid;
    if (Cns_getgrpbygid (sav_gid, sav_gidstr) < 0)
#else
      sav_gid = gid;
    if ((gr = getgrgid (sav_gid))) {
      strncpy (sav_gidstr, gr->gr_name, sizeof(sav_gidstr) - 1);
      sav_gidstr[sizeof(sav_gidstr) - 1] = '\0';
    } else
#endif
      sprintf (sav_gidstr, "%d", sav_gid);
  }
  return (sav_gidstr);
}

static char *
decode_perm(unsigned char perm)
{
  static char modestr[4] = "---";

  modestr[0] = (perm & S_IROTH) ? 'r' : '-';
  modestr[1] = (perm & S_IWOTH) ? 'w' : '-';
  modestr[2] = (perm & S_IXOTH) ? 'x' : '-';
  return (modestr);
}

static char *
decode_user(uid_t uid)
{
  struct passwd *pw;
  static uid_t sav_uid = -1;
  static char sav_uidstr[256];

  if (uid != sav_uid) {
#ifdef VIRTUAL_ID
    if (uid == 0)
      return ("root");
    sav_uid = uid;
    if (Cns_getusrbyuid (sav_uid, sav_uidstr) < 0)
#else
      sav_uid = uid;
    if ((pw = getpwuid (sav_uid)))
      strcpy (sav_uidstr, pw->pw_name);
    else
#endif
      sprintf (sav_uidstr, "%d", sav_uid);
  }
  return (sav_uidstr);
}
