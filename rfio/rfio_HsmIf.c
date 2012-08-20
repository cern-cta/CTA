/*
 * Copyright (C) 2000-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* rfio_HsmIf.c       Remote File I/O - generic HSM client interface         */

#include "Cmutex.h"
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#define RFIO_KERNEL 1
#include "Cglobals.h"
#include "Cns_api.h"
#include "serrno.h"
/*BC Added include for new stager */
#include "stager_api.h"
#define MAXPATH 1024
#include "rfio.h"
#include "osdep.h"
#include "stager_client_commandline.h"

extern int tStageHostKey;
extern int tStagePortKey;
extern int tSvcClassKey;

static int cwdserver_key = -1;
static int cwdtype_key = -1;
static int DIRcontext_key = -1;

typedef struct rfio_HsmIf_DIRcontext {
  DIR *dirp;
  char dirpath[CA_MAXPATHLEN+1];
  char *current_entry;
  struct Cns_filestat Cns_st;
  struct dirent *de;
  int HsmType;
  int GetStat;
} rfio_HsmIf_DIRcontext_t;

static rfio_HsmIf_DIRcontext_t *HsmDirs[MAXRFD];

#define FINDCNSFILES_WITH_SCAN     1
#define FINDCNSFILES_WITHOUT_SCAN  0

EXTERN_C char *getconfent ();   /* CASTOR /etc/castor.conf util */


void rfio_stglog(int type, char *msg) {
  (void)type;
  TRACE(3,"rfio","rfio_stglog: %s",msg);
  return;
}

int rfio_HsmIf_IsCnsFile(const char *path) {
  int rc = 0;
  char *q = (char *)path + strlen(CNS_ROOT);
  if ( strncmp(path,CNS_ROOT,strlen(CNS_ROOT)) == 0 &&
       ( *q == '/' || *q == '\0' ) ) rc = 1;
  else if ( *path != '/' && rfio_HsmIf_GetCwdType() == RFIO_HSM_CNS ) rc = 1;
  return(rc);
}

static int rfio_HsmIf_SwapHsmDirEntry(rfio_HsmIf_DIRcontext_t *OldHsmDir,
                                      rfio_HsmIf_DIRcontext_t *NewHsmDir) {
  int i;

  if ( OldHsmDir == NULL && NewHsmDir == NULL ) return(-1);
  if ( Cmutex_lock((void *) HsmDirs, -1) != 0 ) return(-1);
  for (i = 0; i < MAXRFD; i++) if ( HsmDirs[i] == OldHsmDir ) break;
  if ( i < MAXRFD && HsmDirs[i] != NewHsmDir )
    HsmDirs[i] = NewHsmDir;
  if ( Cmutex_unlock((void *)HsmDirs) != 0 ) return(-1);
  if ( i >= MAXRFD ) return(-1);
  return(i);
}

static int rfio_HsmIf_AddDirEntry(rfio_HsmIf_DIRcontext_t *HsmDir) {
  int rc;
  rc = rfio_HsmIf_SwapHsmDirEntry(NULL,HsmDir);
  TRACE(3,"rfio","rfio_HsmIf_AddDirEntry(0x%x) -> RC=%d\n",HsmDir,rc);
  return(rc);
}

static int rfio_HsmIf_DelDirEntry(rfio_HsmIf_DIRcontext_t *HsmDir) {
  int rc;
  rc = rfio_HsmIf_SwapHsmDirEntry(HsmDir,NULL);
  TRACE(3,"rfio","rfio_HsmIf_DelDirEntry(0x%x) -> RC=%d\n",HsmDir,rc);
  return(rc);
}

static int rfio_HsmIf_FindDirEntry(rfio_HsmIf_DIRcontext_t *HsmDir) {
  int rc;
  rc = rfio_HsmIf_SwapHsmDirEntry(HsmDir,HsmDir);
  TRACE(3,"rfio","rfio_HsmIf_FindDirEntry(0x%x) -> RC=%d\n",HsmDir,rc);
  return(rc);
}

int rfio_HsmIf_IsHsmDirEntry(DIR *HsmDir) {
  return(rfio_HsmIf_FindDirEntry((rfio_HsmIf_DIRcontext_t *)HsmDir));
}

int rfio_HsmIf_IsHsmFile(const char *path) {
  int rc = 0;
  if ( (rc = rfio_HsmIf_IsCnsFile(path)) == 1 ) return(rc);
  return(rc);
}

char *rfio_HsmIf_GetCwdServer() {
  char *cwd_server = NULL;

  Cglobals_get(&cwdserver_key, (void **)&cwd_server, CA_MAXHOSTNAMELEN+1);
  return(cwd_server);
}

int rfio_HsmIf_GetCwdType() {
  int *cwd_type = NULL;

  Cglobals_get(&cwdtype_key, (void **)&cwd_type, CA_MAXHOSTNAMELEN+1);
  return(*cwd_type);
}

int rfio_HsmIf_SetCwdServer(const char *path) {
  char *cwd_server = NULL;

  Cglobals_get(&cwdserver_key, (void **)&cwd_server, CA_MAXHOSTNAMELEN+1);
  strncpy(cwd_server, path, CA_MAXHOSTNAMELEN);
  cwd_server[CA_MAXHOSTNAMELEN] = '\0';
  return(0);
}

int rfio_HsmIf_SetCwdType(int n) {
  int *cwd_type = NULL;

  Cglobals_get(&cwdtype_key, (void **)&cwd_type, sizeof(int));
  *cwd_type = n;
  return(0);
}

int rfio_HsmIf_access(const char *path, int amode) {
  int rc = -1;

  if ( rfio_HsmIf_IsCnsFile(path) ) {
    rc = Cns_access(path,amode);
  }
  return(rc);
}

int rfio_HsmIf_chdir(const char *path) {
  int rc = -1;

  if ( rfio_HsmIf_IsCnsFile(path) ) {
    if ( (rc = Cns_chdir(path)) == 0)
      rfio_HsmIf_SetCwdType(RFIO_HSM_CNS);
  }
  return(rc);
}

int rfio_HsmIf_chmod(const char *path, mode_t mode) {
  int rc = -1;

  if ( rfio_HsmIf_IsCnsFile(path) ) {
    rc = Cns_chmod(path,mode);
  }
  return(rc);
}

int rfio_HsmIf_chown(const char *path, uid_t new_uid, gid_t new_gid) {
  int rc = -1;
  if ( rfio_HsmIf_IsCnsFile(path) ) {
    rc = Cns_chown(path,new_uid,new_gid);
  }
  return(rc);
}

int rfio_HsmIf_close(int fd) {
  (void)fd;
  return 0;
}

int rfio_HsmIf_closedir(DIR *dirp) {
  rfio_HsmIf_DIRcontext_t *tmp = NULL;
  rfio_HsmIf_DIRcontext_t **myDIRcontext = NULL;
  int rc = -1;

  tmp = (rfio_HsmIf_DIRcontext_t *)dirp;
  if ( tmp->HsmType == RFIO_HSM_CNS ) {
    rc = Cns_closedir((Cns_DIR *)(tmp->dirp));
    rfio_HsmIf_DelDirEntry(tmp);
    Cglobals_get(&DIRcontext_key,(void **)&myDIRcontext,
                 sizeof(rfio_HsmIf_DIRcontext_t *));
    if ( (myDIRcontext != NULL) && (*myDIRcontext == tmp) )
      *myDIRcontext = NULL;

    if (tmp->de != NULL) free(tmp->de);
    free(tmp);
  }
  return(rc);
}

int rfio_HsmIf_creat(const char *path, mode_t mode) {
  int rc = -1;
  if ( rfio_HsmIf_IsCnsFile(path) ) {
    rc = Cns_creat(path,mode);
  }
  return(rc);
}

char *rfio_HsmIf_getcwd(char *buf, int size) {
  char *cwd = NULL;
  if ( rfio_HsmIf_GetCwdType() == RFIO_HSM_CNS )
    cwd = Cns_getcwd(buf, size);
  return(cwd);
}

int rfio_HsmIf_mkdir(const char *path, mode_t mode) {
  int rc = -1;
  if ( rfio_HsmIf_IsCnsFile(path) ) {
    rc = Cns_mkdir(path,mode);
  }
  return(rc);
}

int rfio_HsmIf_open(const char *path, int flags, mode_t mode, int mode64, int streaming) {
  int rc = -1;
  int ret;
  int save_serrno, save_errno;
  struct Cns_filestat st;
  int* auxVal;
  char ** auxPoint;
  struct stage_options opts;
  opts.stage_host = NULL;
  opts.stage_port = 0;
  opts.service_class = NULL;

  ret = Cglobals_get(&tStageHostKey, (void**) &auxPoint, sizeof(void*));
  if (ret == 0) {
    if (*auxPoint != NULL) {
      opts.stage_host = strdup(*auxPoint);
      if (opts.stage_host == NULL) {
        serrno = ENOMEM;
        return -1;
      }
    }
  }
  ret = Cglobals_get(&tStagePortKey, (void**) &auxVal, sizeof(int));
  if (ret == 0) {
    opts.stage_port = *auxVal;
  }
  ret = Cglobals_get(&tSvcClassKey, (void**) &auxPoint, sizeof(void*));
  if (ret == 0) {
    if (*auxPoint != NULL) {
      opts.service_class = strdup(*auxPoint);
      if (opts.service_class == NULL) {
        if (opts.stage_host != NULL)
          free(opts.stage_host);
        serrno = ENOMEM;
        return -1;
      }
    }
  }
  
  ret = getDefaultForGlobal(&opts.stage_host, &opts.stage_port, &opts.service_class);

  if ( rfio_HsmIf_IsCnsFile(path) ) {
    char *mover_protocol_rfio = MOVER_PROTOCOL_RFIO;
    if (streaming) {
      mover_protocol_rfio = MOVER_PROTOCOL_RFIOV3;
    }

    /*
     * Check if an existing file is going to be updated
     */
    memset(&st,'\0',sizeof(st));
    rc = Cns_stat(path, &st);
    /*
     * Make sure that filesize is 0 if file doesn't exist (a
     * overdoing) or that we will create (stage_out) if O_TRUNC.
     */
    if ( rc == -1 || ((flags & O_TRUNC) != 0)) st.filesize = 0;

    /** NOW CALL THE NEW STAGER API */
    struct stage_io_fileresp *response = NULL;
    char *url = NULL;

    for (;;) {
      TRACE(3,"rfio","Calling stage_open with: %s %x %x",
            path, flags, mode);
      rc = stage_open(NULL,
                      mover_protocol_rfio,
                      path,
                      flags,
                      mode,
                      0,
                      &response,
                      NULL,
                      &opts);
      save_errno = errno;
      save_serrno = serrno;
      if (rc < 0) {
        if ( (save_serrno == SECOMERR)     ||
             (save_serrno == SETIMEDOUT)   ||
             (save_serrno == ECONNREFUSED) ||
             (save_errno  == ECONNREFUSED)
             ) {
          int retrySleepTime;
          retrySleepTime = 1 + (int)(10.0*rand()/(RAND_MAX+1.0));
          sleep(retrySleepTime);
          continue;
        }

        /* Free resources */
        rc = -1;
        goto end;
      } else {
        break;
      }
    }

    if (response == NULL) {
      TRACE(3,"rfio","Received NULL response");
      serrno = SEINTERNAL;
      rc = -1;
      goto end;
    }

    if (response->errorCode != 0) {
      TRACE(3,"rfio","stage_open error: %d/%s",
            response->errorCode, response->errorMessage);
      serrno = response->errorCode;
      rc = -1;
      goto end;
    }

    url = stage_geturl(response);
    if (url == NULL) {
      rc = -1;
      goto end;
    }

    /*
     * Warning, this is a special case for castor2 when a file is recreated
     * with O_TRUNC but without O_CREAT: the correct behaviour is to
     * recreate the castorfile and schedule access to the new diskcopy.
     * However, because of there is no O_CREAT the mover open will fail
     * to create the new diskcopy. We must therefore add O_CREAT but
     * only after stage_open() has processed the request and recreated
     * the castorfile.
     */
    if ( (flags & O_TRUNC) == O_TRUNC ) flags |= O_CREAT;

    if (mode64) {
      rc = rfio_open64(url, flags, mode);
    } else {
      rc = rfio_open(url, flags, mode);
    }

  end:

    /* Free response structure. Note: This logic should really as an API
     * of the stager client library
     */
    if (response != NULL) {
      if (response->castor_filename != NULL)
        free(response->castor_filename);
      if (response->protocol != NULL)
        free(response->protocol);
      if (response->server != NULL)
        free(response->server);
      if (response->filename != NULL)
        free(response->filename);
      if (response->errorMessage != NULL)
        free(response->errorMessage);
      free(response);
    }

    /* Free url */
    if (url != NULL)
      free(url);
  }

  /* Free stager options */
  if (opts.stage_host != NULL)
    free(opts.stage_host);
  if (opts.service_class != NULL)
    free(opts.service_class);

  return(rc);
}

DIR *rfio_HsmIf_opendir(const char *path) {
  rfio_HsmIf_DIRcontext_t *tmp = NULL;
  char *p;
  if ( rfio_HsmIf_IsCnsFile(path) ) {
    tmp=(rfio_HsmIf_DIRcontext_t *)malloc(sizeof(rfio_HsmIf_DIRcontext_t));
    if ( tmp == NULL ) return(NULL);
    tmp->HsmType = RFIO_HSM_CNS;
    tmp->dirp = (DIR *)Cns_opendir(path);
    if ( tmp->dirp == NULL) {
      free(tmp);
      return(NULL);
    }
    tmp->de = (struct dirent *)malloc(sizeof(struct dirent) + CA_MAXPATHLEN);
    memset(&tmp->Cns_st,'\0',sizeof(struct Cns_filestat));
    memset(tmp->de,'\0',sizeof(struct dirent) + CA_MAXPATHLEN);
    /*
     * Remember the directory path but remove trailing slashes.
     */
    strcpy(tmp->dirpath,path);
    while ( *(p = &tmp->dirpath[strlen(tmp->dirpath)-1]) == '/' ) *p = '\0';
    tmp->current_entry = NULL;
    /*
     * Cns does not allow to intermix Cns_readdir() and Cns_readdirx()
     * thus we always force the latter
     */
    tmp->GetStat = 1;
  }
  if ( tmp != NULL && rfio_HsmIf_AddDirEntry(tmp) == -1 ) {
    (void) Cns_closedir((Cns_DIR *)(tmp->dirp));
    free(tmp->de);
    free(tmp);
    return(NULL);
  }
  return((DIR *)tmp);
}

int rfio_HsmIf_stat(const char *path, struct stat *st) {
  rfio_HsmIf_DIRcontext_t **myDIRcontext = NULL;
  struct Cns_filestat statbuf;
  char *current_entry = NULL;
  char *dirpath = NULL;
  char *p = NULL;
  int rc = -1;
  if ( rfio_HsmIf_IsCnsFile(path) ) {
    Cglobals_get(&DIRcontext_key,(void **)&myDIRcontext,
                 sizeof(rfio_HsmIf_DIRcontext_t));
    if ( myDIRcontext != NULL && *myDIRcontext != NULL && (*myDIRcontext)->current_entry != NULL) {
      current_entry = (*myDIRcontext)->current_entry;
      dirpath = (*myDIRcontext)->dirpath;
      p = strrchr(path,'/');
      if ( p != NULL ) p++;
      if ( p != NULL && strcmp(p,current_entry) == 0 &&
           strncmp(path,dirpath,strlen(dirpath)) == 0 ) {
        statbuf = (*myDIRcontext)->Cns_st;
        rc = 0;
      } else (*myDIRcontext)->GetStat = 1;
    }
    if ( current_entry == NULL || p == NULL || dirpath == NULL ||
         strcmp(p,current_entry) != 0 ||
         strncmp(path,dirpath,strlen(dirpath)) != 0 )
      rc = Cns_stat(path,&statbuf);

    if ( st != NULL ) {
      memset(st,0,sizeof(struct stat));
      st->st_ino = (ino_t)statbuf.fileid;
      st->st_nlink = statbuf.nlink;
      st->st_uid = statbuf.uid;
      st->st_gid = statbuf.gid;
      st->st_mode = statbuf.filemode;
      st->st_size = (off_t)statbuf.filesize;
      st->st_atime = statbuf.atime;
      st->st_mtime = statbuf.mtime;
      st->st_ctime = statbuf.ctime;
    }
  }
  return(rc);
}

int rfio_HsmIf_stat64(const char *path, struct stat64 *st) {
  rfio_HsmIf_DIRcontext_t **myDIRcontext = NULL;
  struct Cns_filestat statbuf;
  char *current_entry = NULL;
  char *dirpath = NULL;
  char *p = NULL;
  int rc = -1;
  if ( rfio_HsmIf_IsCnsFile(path) ) {
    Cglobals_get(&DIRcontext_key,(void **)&myDIRcontext,
                 sizeof(rfio_HsmIf_DIRcontext_t));
    if ( myDIRcontext != NULL && *myDIRcontext != NULL && (*myDIRcontext)->current_entry != NULL) {
      current_entry = (*myDIRcontext)->current_entry;
      dirpath = (*myDIRcontext)->dirpath;
      p = strrchr(path,'/');
      if ( p != NULL ) p++;
      if ( p != NULL && strcmp(p,current_entry) == 0 &&
           strncmp(path,dirpath,strlen(dirpath)) == 0 ) {
        statbuf = (*myDIRcontext)->Cns_st;
        rc = 0;
      } else (*myDIRcontext)->GetStat = 1;
    }
    if ( current_entry == NULL || p == NULL || dirpath == NULL ||
         strcmp(p,current_entry) != 0 ||
         strncmp(path,dirpath,strlen(dirpath)) != 0 )
      rc = Cns_stat(path,&statbuf);

    if ( st != NULL ) {
      memset(st,0,sizeof(struct stat));
      st->st_ino = statbuf.fileid;
      st->st_nlink = statbuf.nlink;
      st->st_uid = statbuf.uid;
      st->st_gid = statbuf.gid;
      st->st_mode = statbuf.filemode;
      st->st_size = statbuf.filesize;
      st->st_atime = statbuf.atime;
      st->st_mtime = statbuf.mtime;
      st->st_ctime = statbuf.ctime;
    }
  }
  return(rc);
}

int rfio_HsmIf_read(int fd, void *buffer, int size) {
  (void)fd;
  (void)buffer;
  int rc = -1;
  /* For CASTOR HSM this is a no-op. */
  rc = size;
  return(rc);
}

struct dirent *rfio_HsmIf_readdir(DIR *dirp) {
  rfio_HsmIf_DIRcontext_t *tmp = NULL;
  rfio_HsmIf_DIRcontext_t **myDIRcontext = NULL;
  struct dirent *tmpdirent = NULL;
  struct Cns_direnstat *tmp_ds;

  tmp = (rfio_HsmIf_DIRcontext_t *)dirp;
  if ( tmp->HsmType == RFIO_HSM_CNS ) {
    if ( tmp->GetStat == 0 )
      tmpdirent = Cns_readdir((Cns_DIR *)(tmp->dirp));
    else {
      tmp->current_entry = NULL;
      tmp_ds = Cns_readdirx((Cns_DIR *)(tmp->dirp));
      if ( tmp_ds != NULL ) {
        tmp->Cns_st.fileid    = tmp_ds->fileid;
        tmp->Cns_st.filemode  = tmp_ds->filemode;
        tmp->Cns_st.nlink     = tmp_ds->nlink;
        tmp->Cns_st.uid       = tmp_ds->uid;
        tmp->Cns_st.gid       = tmp_ds->gid;
        tmp->Cns_st.filesize  = tmp_ds->filesize;
        tmp->Cns_st.atime     = tmp_ds->atime;
        tmp->Cns_st.mtime     = tmp_ds->mtime;
        tmp->Cns_st.ctime     = tmp_ds->ctime;
        tmp->Cns_st.fileclass = tmp_ds->fileclass;
        tmp->Cns_st.status    = tmp_ds->status;

        tmpdirent = tmp->de;
        tmpdirent->d_ino = tmp_ds->fileid;
        tmpdirent->d_reclen = tmp_ds->d_reclen;
        strcpy(tmpdirent->d_name,tmp_ds->d_name);
        tmp->current_entry = tmpdirent->d_name;
      }
    }
    Cglobals_get(&DIRcontext_key,(void **)&myDIRcontext,
                 sizeof(rfio_HsmIf_DIRcontext_t *));
    if ( myDIRcontext != NULL ) *myDIRcontext = tmp;
  } else serrno = EBADF;
  return(tmpdirent);
}

int rfio_HsmIf_rename(const char *opath, const char *npath) {
  int rc = -1;
  if ( rfio_HsmIf_IsCnsFile(opath) &&
       rfio_HsmIf_IsCnsFile(npath) ) {
    rc =Cns_rename(opath,npath);
  }
  return(rc);
}

void rfio_HsmIf_rewinddir(DIR *dirp) {
  rfio_HsmIf_DIRcontext_t *tmp = NULL;
  tmp = (rfio_HsmIf_DIRcontext_t *)dirp;
  if ( tmp->HsmType == RFIO_HSM_CNS )
    Cns_rewinddir((Cns_DIR *)(tmp->dirp));
  return;
}

int rfio_HsmIf_rmdir(const char *path) {
  int rc = -1;
  if ( rfio_HsmIf_IsCnsFile(path) ) {
    rc = Cns_rmdir(path);
  }
  return(rc);
}

int rfio_HsmIf_unlink(const char *path) {
  int rc = -1;
  if ( rfio_HsmIf_IsCnsFile(path) ) {
    rc = Cns_unlink(path);
  }
  return(rc);
}

int rfio_HsmIf_write(int fd, void *buffer, int size) {
  (void)fd;
  (void)buffer;
  int rc = -1;
  /* For CASTOR HSM this is a no-op. */
  rc = size;
  return(rc);
}

int rfio_HsmIf_FirstWrite(int fd, void *buffer, int size) {
  (void)fd;
  (void)buffer;
  if ( size < 0 ) {
    serrno = EINVAL;
    return (-1);
  }
  return(0);
}

int rfio_HsmIf_IOError(int fd, int errorcode) {
  (void)fd;
  (void)errorcode;
  int rc = -1;
  /* Should handle ENOSPC on write here */
  rc = 0;
  return(rc);
}
