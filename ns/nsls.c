/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* nsls - list name server directory/file entries */
#include <errno.h>
#include <dirent.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include "Cgetopt.h"
#include "Cns.h"
#include "Cns_api.h"
#include "serrno.h"
#include "u64subr.h"
#define SIXMONTHS (6*30*24*60*60)

/* Forward declarations */
static char *decode_group(gid_t);
static char *decode_user(uid_t);
extern char *getenv();
int procpath(char*);
int listdir(char*);
int listsegs(char*);
int listentry(char*,struct Cns_filestatcs*,char*,char*);
int listtpentry(char*,int,int,u_signed64,int,char*,int,int,
                unsigned char[4],char,char*,unsigned long);
void direntstatg2filestatcs(struct Cns_filestatcs*, struct Cns_direnstatg*);
void direntstatc2filestatcs(struct Cns_filestatcs*, struct Cns_direnstatc*);

int cflag;
int clflag;
int cmflag;
time_t current_time;
int dflag;
int delflag;
int dsflag;
int iflag;
int Lflag;
int lflag;
int Rflag;
int Tflag;
int uflag;
int checksumflag;

void usage(int status, char *name) {
  if (status != 0) {
    fprintf (stderr, "Try `%s --help` for more information.\n", name);
  } else {
    printf ("Usage: %s [OPTION]... [PATH]...\n", name);
    printf ("List CASTOR name server directory/file entries.\n\n");
    printf ("  -c              use time of last metadata modification instead of last file\n");
    printf ("                  modification\n");
    printf ("  -d              if path is a directory, list the directory entry itself, not the\n");
    printf ("                  files in that directory\n");
    printf ("  -i              print the file uniqueid in front of each entry\n");
    printf ("  -L              for symbolic links, print target attributes instead of link\n");
    printf ("                  attributes\n");
    printf ("  -l              long listing\n");
    printf ("  -R              list the contents of directories recursively\n");
    printf ("  -T              list file segments migrated to tape\n");
    printf ("  -u              use last access time instead of last modification\n");
    printf ("      --checksum  prints the checksum (and its type) for each file segment. This\n");
    printf ("                  option is valid only if -T is specified\n");
    printf ("      --class     print the file class in front of each entry\n");
    printf ("      --comment   print the comment associated with the entry after the pathname\n");
    printf ("      --deleted   print also the logically deleted files\n");
    printf ("      --ds        print the vid followed by a slash followed by the media side\n");
    printf ("                  number. This option is valid only if -T is specified and is useful\n");
    printf ("                  for multi-sided media like DVD\n");
    printf ("      --help      display this help and exit\n\n");
    printf ("Report bugs to <castor.support@cern.ch>.\n");
  }
  exit (status);
}

int main(int argc,
         char **argv)
{
  int c;
  int errflg = 0;
  int hflg = 0;
  char fullpath[CA_MAXPATHLEN+1];
  int i;
  char *p;
  char *path;

  Coptions_t longopts[] = {
    { "checksum",     NO_ARGUMENT, &checksumflag, 1 },
    { "class",        NO_ARGUMENT, &clflag,       1 },
    { "comment",      NO_ARGUMENT, &cmflag,       1 },
    { "deleted",      NO_ARGUMENT, &delflag,      1 },
    { "display_side", NO_ARGUMENT, &dsflag,       1 },
    { "ds",           NO_ARGUMENT, &dsflag,       1 },
    { "help",         NO_ARGUMENT, &hflg,         1 },
    { NULL,           0,           NULL,          0 }
  };

  Copterr = 1;
  Coptind = 1;
  while ((c = Cgetopt_long (argc, argv, "cdiLlRTu", longopts, NULL)) != EOF) {
    switch (c) {
    case 'c':
      cflag++;
      break;
    case 'd':
      dflag++;
      break;
    case 'i':
      iflag++;
      break;
    case 'L':
      Lflag++;
      break;
    case 'l':
      lflag++;
      break;
    case 'R':
      Rflag++;
      break;
    case 'T':
      Tflag++;
      break;
    case 'u':
      uflag++;
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
  (void) time (&current_time);

  if (Coptind >= argc && ! errflg) { /* no file specified */
    if ((p = getenv (CNS_HOME_ENV)) && strlen (p) <= CA_MAXPATHLEN) {
      strcpy (fullpath, p);
      if (procpath (fullpath)) {
        fprintf (stderr, "%s: %s\n", fullpath, sstrerror(serrno));
        exit (USERR);
      }
    } else
      errflg++;
  }
  if (errflg) {
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
    if (procpath (fullpath)) {
      fprintf (stderr, "%s: %s\n", path, sstrerror(serrno));
      errflg++;
    }
  }
  if (errflg)
    exit (USERR);
  exit (0);
}

int procpath(char *fullpath)
{
  int c;
  char comment[CA_MAXCOMMENTLEN+1];
  char slink[CA_MAXPATHLEN+1];
  struct Cns_filestatcs statbuf;

  comment[0] = '\0';
  if (Lflag) {
    if (Cns_statcs (fullpath, &statbuf) < 0)
      return (-1);
  } else {
    if (checksumflag && !Tflag) {
      if (Cns_statcs (fullpath, &statbuf) < 0)
        return (-1);
    } else if (Cns_lstat (fullpath, (struct Cns_filestat*) &statbuf) < 0)
      return (-1);
  }
  if (statbuf.filemode & S_IFDIR && ! dflag)
    return (listdir (fullpath));
  else if (Tflag)
    return (listsegs (fullpath));
  else {
    if (statbuf.status == 'D' && delflag == 0)
      return (0);
    if ((statbuf.filemode & S_IFLNK) == S_IFLNK && lflag) {
      if ((c = Cns_readlink (fullpath, slink, CA_MAXPATHLEN+1)) < 0)
        c = 0;
      slink[c] = '\0';
    }
    if (cmflag)
      (void) Cns_getcomment (fullpath, comment);
    return (listentry (fullpath, &statbuf, slink, comment));
  }
}

int listdir(char *dir)
{
  int c;
  char curdir[CA_MAXPATHLEN+1];
  struct Cns_direncomm *dcp;
  struct dirlist {
    char *d_name;
    struct dirlist *next;
  };
  Cns_DIR *dirp;
  struct dirlist *dlc;  /* pointer to current directory in the list */
  struct dirlist *dlf = NULL; /* pointer to first directory in the list */
  struct dirlist *dll;  /* pointer to last directory in the list */
  struct dirent *dp;
  struct Cns_direnstatc *dxcp;
  struct Cns_direnstatg *dxp;
  struct Cns_direntape *dxtp;
  char slink[CA_MAXPATHLEN+1];
  struct Cns_filestatcs statbuf;

  if ((dirp = Cns_opendir (dir)) == NULL)
    return (-1);

  if (Rflag)
    printf ("\n%s:\n", dir);
  if (! clflag && ! iflag && ! lflag && ! Rflag && ! Tflag) {
    if (! cmflag)
      /* dirent entries contain enough information */
      while ((dp = Cns_readdir (dirp)) != NULL) {
        if (Lflag) {
          sprintf (curdir, "%s/%s", dir, dp->d_name);
          if (Cns_access (curdir, F_OK)) {
            fprintf (stderr, "%s: %s\n", curdir,
                     sstrerror(serrno));
            continue;
          }
        }
        printf ("%s\n", dp->d_name);
      }
    else
      while ((dcp = Cns_readdirc (dirp)) != NULL) {
        printf ("%s %s\n", dcp->d_name, dcp->comment);
      }
  } else if (Tflag) {
    while ((dxtp = Cns_readdirxt (dirp)) != NULL) {
      listtpentry (dxtp->d_name, dxtp->copyno, dxtp->fsec,
                   dxtp->segsize, dxtp->compression, dxtp->vid,
                   dxtp->side, dxtp->fseq, dxtp->blockid, dxtp->s_status,
                   dxtp->checksum_name, dxtp->checksum);
    }
  } else {
    if (! cmflag)
      while ((dxp = Cns_readdirg (dirp)) != NULL) {
        if ((dxp->filemode & S_IFLNK) == S_IFLNK) {
          sprintf (curdir, "%s/%s", dir, dxp->d_name);
          if (Lflag) {
            if (Cns_statcs (curdir,&statbuf))
              fprintf (stderr, "%s: %s\n", curdir,
                       sstrerror(serrno));
            else
              listentry (dxp->d_name,
                         &statbuf, slink, NULL);
            continue;
          }
          if (lflag) {
            if ((c = Cns_readlink (curdir, slink,
                                   CA_MAXPATHLEN+1)) < 0)
              c = 0;
            slink[c] = '\0';
          }
        }
        direntstatg2filestatcs(&statbuf,dxp);
        listentry (dxp->d_name, &statbuf, slink, NULL);
        if (Rflag && (dxp->filemode & S_IFDIR)) {
          if ((dlc = (struct dirlist *)
               malloc (sizeof(struct dirlist))) == NULL ||
              (dlc->d_name = strdup (dxp->d_name)) == NULL) {
            serrno = errno;
            return (-1);
          }
          dlc->next = 0;
          if (dlf == NULL)
            dlf = dlc;
          else
            dll->next = dlc;
          dll = dlc;
        }
      }
    else
      while ((dxcp = Cns_readdirxc (dirp)) != NULL) {
        if ((dxcp->filemode & S_IFLNK) == S_IFLNK && lflag) {
          sprintf (curdir, "%s/%s", dir, dxcp->d_name);
          if ((c = Cns_readlink (curdir, slink, CA_MAXPATHLEN+1)) < 0)
            c = 0;
          slink[c] = '\0';
        }
        direntstatc2filestatcs(&statbuf,dxcp);
        listentry (dxcp->d_name, &statbuf, slink, dxcp->comment);
        if (Rflag && (dxcp->filemode & S_IFDIR)) {
          if ((dlc = (struct dirlist *)
               malloc (sizeof(struct dirlist))) == NULL ||
              (dlc->d_name = strdup (dxcp->d_name)) == NULL) {
            serrno = errno;
            return (-1);
          }
          dlc->next = 0;
          if (dlf == NULL)
            dlf = dlc;
          else
            dll->next = dlc;
          dll = dlc;
        }
      }
  }
  Cns_closedir (dirp);
  while (dlf) {
    if (strcmp (dir, "/"))
      sprintf (curdir, "%s/%s", dir, dlf->d_name);
    else
      sprintf (curdir, "/%s", dlf->d_name);
    if (listdir (curdir) < 0)
      fprintf (stderr, "%s: %s\n", curdir, sstrerror(serrno));
    free (dlf->d_name);
    dlc = dlf;
    dlf = dlf->next;
    free (dlc);
  }
  return (0);
}

int listentry(char *path,
              struct Cns_filestatcs *statbuf,
              char *slink,
              char *comment)
{
  char modestr[11];
  time_t ltime;
  char timestr[13];
  struct tm *tm;
  char tmpbuf[21];

  if (statbuf->status == 'D' && delflag == 0)
    return (0);
  if (iflag)
    printf ("%s ", u64tostr (statbuf->fileid, tmpbuf, 20));
  if (clflag)
    printf ("%3d ", statbuf->fileclass);
  if (lflag) {
    if (statbuf->filemode & S_IFDIR)
      modestr[0] = 'd';
    else if ((statbuf->filemode & S_IFLNK) == S_IFLNK)
      modestr[0] = 'l';
    else
      modestr[0] = statbuf->status;
    modestr[1] = (statbuf->filemode & S_IRUSR) ? 'r' : '-';
    modestr[2] = (statbuf->filemode & S_IWUSR) ? 'w' : '-';
    if (statbuf->filemode & S_IXUSR)
      if (statbuf->filemode & S_ISUID)
        modestr[3] = 's';
      else
        modestr[3] = 'x';
    else
      modestr[3] = '-';
    modestr[4] = (statbuf->filemode & S_IRGRP) ? 'r' : '-';
    modestr[5] = (statbuf->filemode & S_IWGRP) ? 'w' : '-';
    if (statbuf->filemode & S_IXGRP)
      if (statbuf->filemode & S_ISGID)
        modestr[6] = 's';
      else
        modestr[6] = 'x';
    else
      modestr[6] = '-';
    modestr[7] = (statbuf->filemode & S_IROTH) ? 'r' : '-';
    modestr[8] = (statbuf->filemode & S_IWOTH) ? 'w' : '-';
    if (statbuf->filemode & S_IXOTH)
      if (statbuf->filemode & S_ISVTX)
        modestr[9] = 't';
      else
        modestr[9] = 'x';
    else
      modestr[9] = '-';
    modestr[10] = '\0';
    if (cflag)
      ltime = statbuf->ctime;
    else if (uflag)
      ltime = statbuf->atime;
    else
      ltime = statbuf->mtime;
    tm = localtime (&ltime);
    if (ltime < current_time - SIXMONTHS ||
        ltime > current_time + 60)
      strftime (timestr, 13, "%b %d  %Y", tm);
    else
      strftime (timestr, 13, "%b %d %H:%M", tm);
    printf ("%s %3d %-8s %-8s %s %s ",
            modestr, statbuf->nlink, decode_user (statbuf->uid),
            decode_group (statbuf->gid),
            u64tostr (statbuf->filesize, tmpbuf, 18), timestr);
  }
  if (checksumflag) {
    printf ("%2s %8s ", statbuf->csumtype, statbuf->csumvalue);
  }
  printf ("%s", path);
  if (lflag && modestr[0] == 'l')
    printf (" -> %s", slink);
  if (cmflag)
    printf (" %s", comment);
  printf ("\n");
  return (0);
}

static char *
decode_group(gid_t gid)
{
  struct group *gr;
  static gid_t sav_gid = -1;
  static char sav_gidstr[256];

  if (gid != sav_gid) {
    sav_gid = gid;
    if ((gr = getgrgid (sav_gid))) {
      strncpy (sav_gidstr, gr->gr_name, sizeof(sav_gidstr) - 1);
      sav_gidstr[sizeof(sav_gidstr) - 1] = '\0';
    } else {
      sprintf (sav_gidstr, "%d", sav_gid);
    }
  }
  return (sav_gidstr);
}

static char *
decode_user(uid_t uid)
{
  struct passwd *pw;
  static uid_t sav_uid = -1;
  static char sav_uidstr[256];

  if (uid != sav_uid) {
    sav_uid = uid;
    if ((pw = getpwuid (sav_uid))) {
      strcpy (sav_uidstr, pw->pw_name);
    } else {
      sprintf (sav_uidstr, "%d", sav_uid);
    }
  }
  return (sav_uidstr);
}

int listsegs(char *path)
{
  int c;
  struct Cns_fileid file_uniqueid;
  int i;
  int nbseg;
  struct Cns_segattrs *segattrs;

  memset (&file_uniqueid, 0, sizeof(struct Cns_fileid));
  if ((c = Cns_getsegattrs (path, &file_uniqueid, &nbseg, &segattrs)))
    return (c);
  for (i = 0; i < nbseg; i++) {
    listtpentry (path, (segattrs+i)->copyno, (segattrs+i)->fsec,
                 (segattrs+i)->segsize, (segattrs+i)->compression,
                 (segattrs+i)->vid, (segattrs+i)->side, (segattrs+i)->fseq,
                 (segattrs+i)->blockid, (segattrs+i)->s_status,
                 (segattrs+i)->checksum_name, (segattrs+i)->checksum);
  }
  if (segattrs)
    free (segattrs);
  return (0);
}

int listtpentry(char *path, int copyno, int fsec, u_signed64 segsize, int compression,char *vid,int side, int fseq, unsigned char blockid[4],char status, char *checksum_name,unsigned long checksum)
{
  char tmpbuf[21];

  if (status == 'D' && delflag == 0)
    return (0);
  if (dsflag || side > 0)
    printf ("%c %d %3d %-6.6s/%d %5d %02x%02x%02x%02x %s %3d ",
            status, copyno, fsec, vid, side, fseq,
            blockid[0], blockid[1], blockid[2], blockid[3],
            u64tostr (segsize, tmpbuf, 20), compression);
  else
    printf ("%c %d %3d %-6.6s   %5d %02x%02x%02x%02x %s %3d ",
            status, copyno, fsec, vid, fseq,
            blockid[0], blockid[1], blockid[2], blockid[3],
            u64tostr (segsize, tmpbuf, 20), compression);

  if (checksumflag) {
    if (checksum_name != NULL && checksum_name[0] != '\0') {
      printf ("%*s %08lx ", CA_MAXCKSUMNAMELEN, checksum_name, checksum);
    } else {
      printf ("%*s %08x ", CA_MAXCKSUMNAMELEN, "-", 0);
    }
  }
  printf ("%s\n", path);
  return (0);
}
/* converting procedures */
void direntstatg2filestatcs(struct Cns_filestatcs* statbuf, struct Cns_direnstatg* dxp)
{
  statbuf->fileid=dxp->fileid;
  statbuf->filemode=dxp->filemode;
  statbuf->nlink=dxp->nlink;
  statbuf->uid=dxp->uid;
  statbuf->gid=dxp->gid;
  statbuf->filesize=dxp->filesize;
  statbuf->atime=dxp->atime;
  statbuf->mtime=dxp->mtime;
  statbuf->ctime=dxp->ctime;
  statbuf->fileclass=dxp->fileclass;
  statbuf->filemode=dxp->filemode;
  statbuf->status=dxp->status;
  strncpy(statbuf->csumtype,dxp->csumtype,2);statbuf->csumtype[2]='\0';
  strncpy(statbuf->csumvalue,dxp->csumvalue,CA_MAXCKSUMLEN);statbuf->csumvalue[CA_MAXCKSUMLEN]='\0';
  return;
}
void direntstatc2filestatcs(struct Cns_filestatcs* statbuf, struct Cns_direnstatc* dxp)
{
  statbuf->fileid=dxp->fileid;
  statbuf->filemode=dxp->filemode;
  statbuf->nlink=dxp->nlink;
  statbuf->uid=dxp->uid;
  statbuf->gid=dxp->gid;
  statbuf->filesize=dxp->filesize;
  statbuf->atime=dxp->atime;
  statbuf->mtime=dxp->mtime;
  statbuf->ctime=dxp->ctime;
  statbuf->fileclass=dxp->fileclass;
  statbuf->filemode=dxp->filemode;
  statbuf->status=dxp->status;
  statbuf->csumtype[0]='\0';
  statbuf->csumvalue[0]='\0';
  return;
}
