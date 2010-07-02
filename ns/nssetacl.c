/*
 * Copyright (C) 2003-2005 by CERN/IT/ADC/CA
 * All rights reserved
 */

/* nssetacl - set the Access Control List for a file/directory */
#include <errno.h>
#include <sys/types.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <ctype.h>
#include "Cns.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"

static int parse_entries(char *, struct Cns_acl *);
int dflag;
int mflag;
int sflag;

int cvt_group(char *p);
int cvt_perm(char *p);
int cvt_user(char *p);

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s OPTION ACL PATH...\n", name);
    printf ("Set directory/file access control lists.\n\n");
    printf ("  -d, --delete  remove ACL entries. The \"perm\" field is ignored\n");
    printf ("  -m, --modify  modify existing ACL entries or add new entries\n");
    printf ("  -s, --set     set the ACL entries. The complete set of ACL entries is replaced\n");
    printf ("      --help    display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}

int main(int argc,
         char **argv)
{
  struct Cns_acl *acl;
  struct Cns_acl *aclp;
  int c;
  int errflg = 0;
  int hflg = 0;
  int found;
  char fullpath[CA_MAXPATHLEN+1];
  int i;
  struct Cns_acl inpacl[CA_MAXACLENTRIES];
  int j;
  int k;
  int nb_inp_entries;
  int nb_tmp_entries;
  int nentries;
  int nocheckid;
  char *p;
  char *path;
  struct Cns_acl *taclp;
  struct Cns_acl tmpacl[CA_MAXACLENTRIES];

  Coptions_t longopts[] = {
    { "delete", NO_ARGUMENT, NULL, 'd' },
    { "modify", NO_ARGUMENT, NULL, 'm' },
    { "set",    NO_ARGUMENT, NULL, 's' },
    { "help",   NO_ARGUMENT, &hflg, 1  },
    { NULL,     0,           NULL,  0  }
  };

  Coptind = 1;
  Copterr = 1;
  while ((c = Cgetopt_long (argc, argv, "dms", longopts, NULL)) != EOF) {
    switch (c) {
    case 'd':
      dflag++;
      break;
    case 'm':
      mflag++;
      break;
    case 's':
      sflag++;
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
  if (dflag + mflag + sflag != 1)
    errflg++;
  if (errflg || Coptind >= argc - 1) {
    usage (USERR, argv[0]);
  }
  if ((nb_inp_entries = parse_entries (argv[Coptind], inpacl)) <= 0) {
    exit (USERR);
  }
  for (i = Coptind+1; i < argc; i++) {
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
    if (dflag || mflag) {
      if ((nb_tmp_entries = Cns_getacl (fullpath, CA_MAXACLENTRIES, tmpacl)) < 0) {
        fprintf (stderr, "%s: %s\n", path, sstrerror(serrno));
        errflg++;
        continue;
      }
      if (dflag) {
        for (aclp = inpacl, j = 0; j < nb_inp_entries; aclp++, j++) {
          nocheckid = aclp->a_type == CNS_ACL_USER_OBJ ||
            aclp->a_type == CNS_ACL_GROUP_OBJ ||
            aclp->a_type == (CNS_ACL_DEFAULT | CNS_ACL_USER_OBJ) ||
            aclp->a_type == (CNS_ACL_DEFAULT | CNS_ACL_GROUP_OBJ);
          for (taclp = tmpacl, k = 0; k < nb_tmp_entries; taclp++, k++) {
            if (aclp->a_type == taclp->a_type &&
                (aclp->a_id == taclp->a_id ||
                 nocheckid)) {
              nb_tmp_entries--;
              if (k < nb_tmp_entries)
                memcpy (taclp, taclp + 1,
                        (nb_tmp_entries - k) * sizeof (struct Cns_acl));
              break;
            }
          }
        }
      } else {
        for (aclp = inpacl, j = 0; j < nb_inp_entries; aclp++, j++) {
          found = 0;
          nocheckid = aclp->a_type == CNS_ACL_USER_OBJ ||
            aclp->a_type == CNS_ACL_GROUP_OBJ ||
            aclp->a_type == (CNS_ACL_DEFAULT | CNS_ACL_USER_OBJ) ||
            aclp->a_type == (CNS_ACL_DEFAULT | CNS_ACL_GROUP_OBJ);
          for (taclp = tmpacl, k = 0; k < nb_tmp_entries; taclp++, k++) {
            if (aclp->a_type == taclp->a_type &&
                (aclp->a_id == taclp->a_id ||
                 nocheckid)) {
              found++;
              taclp->a_perm = aclp->a_perm;
              break;
            }
          }
          if (! found) {
            memcpy (taclp, aclp, sizeof(struct Cns_acl));
            nb_tmp_entries++;
          }
        }
      }
      acl = tmpacl;
      nentries = nb_tmp_entries;
    } else {
      acl = inpacl;
      nentries = nb_inp_entries;
    }
    if (Cns_setacl (fullpath, nentries, acl)) {
      fprintf (stderr, "%s: %s\n", path, sstrerror(serrno));
      errflg++;
    }
  }
  if (errflg)
    exit (USERR);
  exit (0);
}

static int
parse_entries(char *entries, struct Cns_acl *acl)
{
  char *entry = entries;
  int n;
  int nentries = 0;
  char *p;
  char *p_id;
  char *q;

  while (entry) {
    if ((p = strchr (entry, ','))) {
      *p = '\0';
      p++;
    }
    if ((q = strchr (entry, ':')) == NULL) {
      fprintf (stderr, "missing colon delimiter\n");
      return (-1);
    }
    *q = '\0';
    if (strcmp (entry, "d") == 0 || strcmp (entry, "default") == 0) {
      entry = q + 1;
      if ((q = strchr (entry, ':')) == NULL) {
        fprintf (stderr, "missing colon delimiter\n");
        return (-1);
      }
      *q = '\0';
      acl->a_type = CNS_ACL_DEFAULT;
    } else
      acl->a_type = 0;
    acl->a_id = 0;
    if (strcmp (entry, "u") == 0 || strcmp (entry, "user") == 0) {
      if (*(q + 1) == ':') {
        acl->a_type |= CNS_ACL_USER_OBJ;
        q++;
      } else
        acl->a_type |= CNS_ACL_USER;
    } else if (strcmp (entry, "g") == 0 || strcmp (entry, "group") == 0) {
      if (*(q + 1) == ':') {
        acl->a_type |= CNS_ACL_GROUP_OBJ;
        q++;
      } else
        acl->a_type |= CNS_ACL_GROUP;
    } else if (strcmp (entry, "m") == 0 || strcmp (entry, "mask") == 0) {
      acl->a_type |= CNS_ACL_MASK;
      if (*(q + 1) == ':')
        q++;
    } else if (strcmp (entry, "o") == 0 || strcmp (entry, "other") == 0) {
      acl->a_type |= CNS_ACL_OTHER;
      if (*(q + 1) == ':')
        q++;
    }
    q++;
    if (acl->a_type == CNS_ACL_USER ||
        acl->a_type == CNS_ACL_GROUP ||
        acl->a_type == (CNS_ACL_DEFAULT | CNS_ACL_USER) ||
        acl->a_type == (CNS_ACL_DEFAULT | CNS_ACL_GROUP)) {
      p_id = q;
      if ((q = strchr (q, ':')) == NULL) {
        if (! dflag) {
          fprintf (stderr, "missing colon delimiter\n");
          return (-1);
        }
      } else
        *q = '\0';
      if (acl->a_type == CNS_ACL_USER ||
          acl->a_type == (CNS_ACL_DEFAULT | CNS_ACL_USER)) {
        if ((acl->a_id = cvt_user (p_id)) < 0)
          return (-1);
      } else {
        if ((acl->a_id = cvt_group (p_id)) < 0)
          return (-1);
      }
      q++;
    }
    if (! dflag) {
      if ((n = cvt_perm (q)) < 0)
        return (-1);
      else
        acl->a_perm = n;
    }
    acl++;
    nentries++;
    entry = p;
  }
  return (nentries);
}

int cvt_group(char *p)
{
  char *dp;
  gid_t gid;
  struct group *gr;

  if (isdigit (*p)) {
    gid = strtol (p, &dp, 10);
    if (*dp != '\0') {
      fprintf (stderr, "invalid group: %s\n", p);
      return (-1);
    }
  } else {
#ifdef VIRTUAL_ID
    if (strcmp (p, "root") == 0)
      gid = 0;
    else if (Cns_getgrpbynam (p, &gid) < 0)
#else
      if ((gr = getgrnam (p)))
        gid = gr->gr_gid;
      else
#endif
      {

        fprintf (stderr, "invalid group: %s\n", p);
        return (-1);
      }
  }
  return (gid);
}

int cvt_perm(char *p)
{
  char *dp;
  mode_t mode;
  char *q;

  if (isdigit (*p)) {
    mode = strtol (p, &dp, 8);
    if (*dp != '\0' || mode > 7) {
      fprintf (stderr, "invalid mode: %s\n", p);
      return (-1);
    }
  } else {
    if (strlen (p) > 3) {
      fprintf (stderr, "invalid mode: %s\n", p);
      return (-1);
    }
    mode = 0;
    q = p;
    while (*q) {
      switch (*q) {
      case 'r':
        mode |= 4;
        break;
      case 'w':
        mode |= 2;
        break;
      case 'x':
        mode |= 1;
      case '-':
        break;
      default:
        fprintf (stderr, "invalid mode: %s\n", p);
        return (-1);
      }
      q++;
    }
  }
  return (mode);
}

int cvt_user(char *p)
{
  char *dp;
  struct passwd *pwd;
  uid_t uid;

  if (isdigit (*p)) {
    uid = strtol (p, &dp, 10);
    if (*dp != '\0') {
      fprintf (stderr, "invalid user: %s\n", p);
      return (-1);
    }
  } else {
#ifdef VIRTUAL_ID
    if (strcmp (p, "root") == 0)
      uid = 0;
    else if (Cns_getusrbynam (p, &uid) < 0)
#else
      if ((pwd = getpwnam (p)))
        uid = pwd->pw_uid;
      else
#endif
      {

        fprintf (stderr, "invalid user: %s\n", p);
        return (-1);
      }
  }
  return (uid);
}
