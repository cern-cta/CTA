/*
 * $Id: rfdir.c,v 1.10 2001/06/18 06:17:36 baud Exp $
 */

/*
 * Copyright (C) 1998-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rfdir.c,v $ $Revision: 1.10 $ $Date: 2001/06/18 06:17:36 $ CERN/IT/PDP/DM Olof Barring";
#endif /* not lint */
 
/*
 * List remote directory or file. Gives an ls -al type output
 */
#define RFIO_KERNEL 1
#include <limits.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#if !defined(_WIN32)
#include <dirent.h>
#endif
#include <pwd.h>
#include <grp.h>
#include <rfio.h>

#define SIXMONTHS (6*30*24*60*60)
#ifndef PATH_MAX
#include <Castor_limits.h>
#define PATH_MAX CA_MAXPATHLEN + 1 /* == 1024 == PATH_MAX on IRIX */
#endif

time_t current_time;
char ftype[7];
int ftype_v[7];
char fmode[9];
int fmode_v[9];
struct dirstack {
  char *dir;
  struct dirstack *prev;
};

static char *ckpath();
char *getconfent();

int main(argc, argv) 
int argc;
char *argv[];
{
  extern char * optarg;
  extern int optind;
  struct stat st;
  char *dir;
  int rc,i;
  struct dirent *de;
  char modestr[11];
  char owner[20];
  char t_creat[14];
  struct passwd *pw;
  struct group *grp;
  struct tm *t_tm;
  char uidstr[30];
  char gidstr[30];
  char path[PATH_MAX];
  int recursively = 0;
  int multiple = 0;
#if defined(_WIN32)
  WSADATA wsadata;
#endif

  strcpy(ftype,"pcdb-ls");
  ftype_v[0] = S_IFIFO; ftype_v[1] = S_IFCHR; ftype_v[2] = S_IFDIR; 
  ftype_v[3] = S_IFBLK; ftype_v[4] = S_IFREG; ftype_v[5] = S_IFLNK;
  ftype_v[6] = S_IFSOCK;
  strcpy(fmode,"rwxrwxrwx");
  fmode_v[0] = S_IRUSR; fmode_v[1] = S_IWUSR; fmode_v[2] = S_IXUSR;
  fmode_v[3] = S_IRGRP; fmode_v[4] = S_IWGRP; fmode_v[5] = S_IXGRP;
  fmode_v[6] = S_IROTH; fmode_v[7] = S_IWOTH; fmode_v[8] = S_IXOTH;
  if ( argc < 2 ) {
    fprintf(stderr,"Usage: %s [-R] [file | directory ...]\n",argv[0]);
    exit(0);
  }
  while ( (i = getopt(argc,argv,"R")) != EOF ) {
    switch(i) {
    case 'R' : 
      recursively++;
      break;
    case '?' :
      fprintf(stderr,"Usage: %s [-R] [file | directory ...]\n",argv[0]);
      exit(2);
    }
  }
  (void) time (&current_time);

#if defined(_WIN32)
  if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
    fprintf (stderr, "WSAStartup unsuccessful\n");
    exit (2);
  }
#endif

  multiple = argc - optind - 1;
  for (;optind<argc;optind++) {
    
    dir = ckpath(argv[optind]);
    
    rc = rfio_stat(dir,&st);
    
    if ( rc ) {
      fprintf(stderr,"%s: %s\n",dir,rfio_serror());
      exit(1);
    }
    
    if ( !S_ISDIR(st.st_mode) ) {
      strcpy(modestr,"----------");
      for (i=0; i<6; i++) if ( ftype_v[i] == ( S_IFMT & st.st_mode ) ) break;
      modestr[0] = ftype[i];
      for (i=0; i<9; i++) if (fmode_v[i] & st.st_mode) modestr[i+1] = fmode[i];
      if ( S_ISUID & st.st_mode ) modestr[3] = 's';
      if ( S_ISGID & st.st_mode ) modestr[6] = 's';
      pw = getpwuid(st.st_uid);
      if ( pw == NULL ) sprintf(uidstr,"%d",st.st_uid);
      else strcpy(uidstr,pw->pw_name);
      grp = getgrgid(st.st_gid);
      if ( grp == NULL ) sprintf(gidstr,"%d",st.st_gid);
      else strcpy(gidstr,grp->gr_name);
      t_tm = localtime(&st.st_mtime);
      if (st.st_mtime < current_time - SIXMONTHS || st.st_mtime > current_time + 60)
         strftime(t_creat,13,"%b %d  %Y",t_tm);
      else
         strftime(t_creat,13,"%b %d %H:%M",t_tm);
      fprintf(stdout,"%s %3d %-8.8s %-8.8s %8d %s %s\n",modestr,st.st_nlink,
         uidstr,gidstr,(int)st.st_size,t_creat,dir);
    } else {
      list_dir(dir,recursively,multiple);
    }
  }
  rfio_end();
  exit(0);
}

static char *ckpath(path)
char *path;
{
  char *cp;
  static char newpath[BUFSIZ];
 /* Special treatment for filenames starting with /scratch/... */
  if (!strncmp ("/scratch/", path, 9) &&
      (cp = getconfent ("SHIFT", "SCRATCH", 0)) != NULL) {
    strcpy (newpath, cp);
    strcat (newpath, path+9);
  } else 
 /* Special treatment for filenames starting with /hpss/... */
    if ( !strncmp("/hpss/",path,6) &&
	 (cp = getconfent("SHIFT","HPSS",0)) != NULL) {
      strcpy(newpath,cp);
      strcat(newpath,path+6);
    } else strcpy(newpath,path);
  return(newpath);
}

static int rfio_pushdir(ds,dir)
struct dirstack **ds;
char *dir;
{
  struct dirstack *tmp;
  if ( ds == NULL || dir == NULL ) return(0);
  tmp = (struct dirstack *)malloc(sizeof(struct dirstack));
  tmp->prev = *ds;
  tmp->dir = (char *)malloc((strlen(dir)+1)*sizeof(char));
  strcpy(tmp->dir,dir);
  *ds = tmp;
  return(0);
}
static struct dirstack *rfio_popdir(ds)
struct dirstack **ds;
{
   struct dirstack *tmp;
   if ( ds == NULL ) return(NULL);
   tmp = *ds;
   *ds = (*ds)->prev;
   free(tmp->dir);
   free(tmp);
   return(*ds);
}

int list_dir(dir,recursively,multiple)
char *dir;
int recursively,multiple;
{
  RDIR *dirp;
  struct stat st;
  int rc,i,fd;
  struct dirent *de;
  char modestr[11];
  char owner[20];
  char t_creat[14];
  struct passwd *pw;
  struct group *grp;
  struct tm *t_tm;
  char uidstr[30];
  char gidstr[30];
  char path[PATH_MAX];
  uid_t old_uid = -1;
  gid_t old_gid = -1;
  struct dirstack *ds = NULL;
  char *host,*filename;
  int reqtype = 0;
  int localdir = 0;
  int rootpathlen=0;

  if (rfio_parseln(dir,&host,&filename,RDLINKS)) {
    fd=rfio_connect(host,&i) ;
    reqtype = RQST_MSTAT_SEC;
    if ( fd >= 0 ) rc = rfio_smstat(fd,filename,&st,reqtype);
    if ( rc < 0 && serrno == SEPROTONOTSUP ) 
      reqtype = RQST_MSTAT;
    else netclose(fd);
  } else {
    localdir = 1;
  }
  
  dirp = rfio_opendir(dir);
  if ( dirp == NULL ) {
    rfio_perror("opendir()");
    exit(1);
  }

  if ( recursively || multiple ) fprintf(stdout,"\n%s:\n\n",dir);
  if ( !localdir ) {
    strcpy(path,host);
    strcat(path,":");
    strcat(path,filename);
  } else strcpy(path,dir);
  rootpathlen = strlen(path);
  path[rootpathlen] = '/';
  while ( ( de = rfio_readdir(dirp) ) != NULL ) {
    rc = 0;
    path[rootpathlen+1] = '\0';
    strcat(path,de->d_name);
    if (!rfio_parseln(path,&host,&filename,RDLINKS) && host == NULL ) {
      /* The file is local */
      rc = stat(filename,&st) ;
    } else {
      if ( localdir ) rc = rfio_stat(path,&st);
      else rc = rfio_smstat(dirp->s,filename,&st,reqtype);
    }
    if ( rc ) {
      fprintf(stderr,"%s: %s\n",path,rfio_serror());
      continue;
    }
    strcpy(modestr,"----------");
    for (i=0; i<6; i++) if ( ftype_v[i] == ( S_IFMT & st.st_mode ) ) break;
    modestr[0] = ftype[i];
    for (i=0; i<9; i++) if (fmode_v[i] & st.st_mode) modestr[i+1] = fmode[i];
    if ( S_ISUID & st.st_mode ) modestr[3] = 's';
    if ( S_ISGID & st.st_mode ) modestr[6] = 's';
    if ( st.st_uid != old_uid) {
      pw = getpwuid(st.st_uid);
      if ( pw == NULL ) sprintf(uidstr,"%d",st.st_uid);
      else strcpy(uidstr,pw->pw_name);
      old_uid = st.st_uid;
    }
    if ( st.st_gid != old_gid ) {
      grp = getgrgid(st.st_gid);
      if ( grp == NULL ) sprintf(gidstr,"%d",st.st_gid);
      else strcpy(gidstr,grp->gr_name);
      old_gid = st.st_gid;
    }
    t_tm = localtime(&st.st_mtime);
    if (st.st_mtime < current_time - SIXMONTHS || st.st_mtime > current_time + 60)
         strftime(t_creat,13,"%b %d  %Y",t_tm);
    else
         strftime(t_creat,13,"%b %d %H:%M",t_tm);
    fprintf(stdout,"%s %3d %-8.8s %-8.8s %8d %s %s\n",modestr,st.st_nlink,
         uidstr,gidstr,(int)st.st_size,t_creat,de->d_name);
    if ( strcmp(de->d_name,".") && strcmp(de->d_name,"..") && 
	 S_ISDIR(st.st_mode) && recursively ) rfio_pushdir(&ds,path);
  }
  rfio_closedir(dirp);
  while ( ds != NULL ) {
    list_dir(ds->dir,recursively,multiple);
    rfio_popdir(&ds);
  }
  return(0);
}
