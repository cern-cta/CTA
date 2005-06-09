/*
 * Copyright (C) 2000-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)rfio_HsmIf.c,v 1.58 2004/02/10 15:05:17 CERN/IT/PDP/DM Olof Barring";
#endif /* not lint */

/* rfio_HsmIf.c       Remote File I/O - generic HSM client interface         */

#define RFIO_USE_CASTOR_V2 "RFIO_USE_CASTOR_V2"

#include "Cmutex.h"
#include <stdlib.h>
#if !defined(_WIN32)
#include <unistd.h>
#endif /* _WIN32 */
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#if defined(_WIN32)
#define R_OK 4
#define W_OK 2
#define X_OK 1
#define F_OK 0
#endif /* _WIN32 */
#include <dirent.h>
#include <sys/stat.h>
#define RFIO_KERNEL 1
#include "Cglobals.h"
#include "Cns_api.h"
#include "serrno.h"
/*BC Added include for new stager */
#include "stager_api.h"
#define MAXPATH 1024
#include "stage_api.h"
#include "rfio.h"
#include "osdep.h"
#if defined(CASTOR_ON_GLOBAL_FILESYSTEM)
#include "rfio_lcastorfdt.h"
#endif

#if defined(CNS_ROOT)
typedef struct CnsFiles {
    int s;
    int mode;
    int written_to;
    stage_hsm_t *hsmfile;
} CnsFiles_t;
static CnsFiles_t *CnsFilesfdt[MAXRFD];
static CnsFiles_t dummyCnsFile;
#endif /* CNS_ROOT */

static int cwdserver_key = -1;
static int cwdtype_key = -1;
static int DIRcontext_key = -1;

typedef struct rfio_HsmIf_DIRcontext {
    DIR *dirp;
    char dirpath[CA_MAXPATHLEN+1];
    char *current_entry;
#if defined(CNS_ROOT)
    struct Cns_filestat Cns_st;
#endif /* CNS_ROOT */
    struct dirent *de;
    int HsmType;
    int GetStat;
} rfio_HsmIf_DIRcontext_t;

static rfio_HsmIf_DIRcontext_t *HsmDirs[MAXRFD];

#if defined(CNS_ROOT)
#define FINDCNSFILES_WITH_SCAN     1
#define FINDCNSFILES_WITHOUT_SCAN  0
static int rfio_CnsFilesfdt_allocentry _PROTO((int));
static int rfio_CnsFilesfdt_findentry _PROTO((int,int));
static int rfio_CnsFilesfdt_freeentry _PROTO((int));

EXTERN_C char *getconfent _PROTO(());   /* CASTOR /etc/castor.conf util */
int DLL_DECL use_castor2_api _PROTO(());

/*
 * Internal file info. table. Note that those routines do not
 * guarantee atomicity. Concurrent writer threads to a single file
 * may have problems (they would anyway!).
 */
static int AddCnsFileDescriptor(int fd, int mode, stage_hsm_t *hsmfile) {
    int s_index;
    CnsFiles_t *thisCnsFile;

    if ((s_index = rfio_CnsFilesfdt_allocentry(fd)) < 0) {
        serrno = SEINTERNAL;
        return(-1);
    }
    if ((thisCnsFile = malloc(sizeof(CnsFiles_t))) == NULL) {
        rfio_CnsFilesfdt_freeentry(s_index);
        serrno = SEINTERNAL;
        return(-1);
    }
    CnsFilesfdt[s_index]          = thisCnsFile;
    CnsFilesfdt[s_index]->s       = fd;
    CnsFilesfdt[s_index]->hsmfile = hsmfile;
    CnsFilesfdt[s_index]->mode    = mode;
    CnsFilesfdt[s_index]->written_to = 0;
    return(0);
}
static int SetCnsWrittenTo(int fd) {
    int s_index;

    if ((s_index = rfio_CnsFilesfdt_findentry(fd,FINDCNSFILES_WITHOUT_SCAN)) < 0) {
        serrno = SEINTERNAL;
        return(-1);
    }
    if ( CnsFilesfdt[s_index]->hsmfile == NULL ) {
        serrno = ENOENT;
        return(-1);
    }
    CnsFilesfdt[s_index]->written_to = 1;
    return(0);
}

static int GetCnsFileDescriptor(int fd, int *mode, stage_hsm_t **hsmfile,
                                int *written_to) {
    int s_index;

    if ((s_index = rfio_CnsFilesfdt_findentry(fd,FINDCNSFILES_WITHOUT_SCAN)) < 0) {
        serrno = SEINTERNAL;
        return(-1);
    }
    if ( CnsFilesfdt[s_index]->hsmfile == NULL ) {
        serrno = ENOENT;
        return(-1);
    }
    if ( hsmfile != NULL ) *hsmfile = CnsFilesfdt[s_index]->hsmfile;
    if ( mode != NULL ) *mode = CnsFilesfdt[s_index]->mode;
    if ( written_to ) *written_to = CnsFilesfdt[s_index]->written_to;
    return(0);
}

static int FindCnsPhysicalPath(char *Cns_path, char **physical_path) {
    int fd;

    if ( Cns_path == NULL || physical_path == NULL ) return(-1);
    for ( fd = 0; fd < MAXRFD; fd++ ) {
        if ( (CnsFilesfdt[fd] == NULL) || (CnsFilesfdt[fd] == &dummyCnsFile) ) continue;
        if ( (CnsFilesfdt[fd]->hsmfile != NULL) &&
             strcmp(CnsFilesfdt[fd]->hsmfile->xfile,Cns_path) == 0 ) {
            *physical_path = CnsFilesfdt[fd]->hsmfile->upath;
            return(1);
        }
    }
    return(0);
}

static int DelCnsFileDescriptor(int fd) {
    int s_index;

    if ((s_index = rfio_CnsFilesfdt_findentry(fd,FINDCNSFILES_WITHOUT_SCAN)) < 0) {
        serrno = SEINTERNAL;
        return(-1);
    }
    rfio_CnsFilesfdt_freeentry(s_index);
    return(0);
}
static int CnsCleanup(stage_hsm_t **hsmfile) {
    if ( hsmfile == NULL || *hsmfile == NULL) return(0);
    if ( (*hsmfile)->xfile != NULL ) free((*hsmfile)->xfile);
    if ( (*hsmfile)->upath != NULL ) free((*hsmfile)->upath);
    free(*hsmfile);
    return(0);
}
void rfio_stglog(int type, char *msg) {
    TRACE(3,"rfio","rfio_stglog: %s",msg);
    return;
}
#endif /* CNS_ROOT */

int rfio_HsmIf_IsCnsFile(const char *path) {
    int rc = 0;
#if defined(CNS_ROOT)
    char *q = (char *)path + strlen(CNS_ROOT);
    if ( strncmp(path,CNS_ROOT,strlen(CNS_ROOT)) == 0 &&
	( *q == '/' || *q == '\0' ) ) rc = 1;
    else if ( *path != '/' && rfio_HsmIf_GetCwdType() == RFIO_HSM_CNS ) rc = 1;
#endif /* CNS_ROOT */
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

int DLL_DECL rfio_HsmIf_IsHsmDirEntry(DIR *HsmDir) {
    return(rfio_HsmIf_FindDirEntry((rfio_HsmIf_DIRcontext_t *)HsmDir));
}

int DLL_DECL rfio_HsmIf_IsHsmFile(const char *path) {
    int rc = 0;

    if ( (rc = rfio_HsmIf_IsCnsFile(path)) == 1 ) return(rc); 
    return(rc);
}

char DLL_DECL *rfio_HsmIf_GetCwdServer() {
    char *cwd_server = NULL;

    Cglobals_get(&cwdserver_key, (void **)&cwd_server, CA_MAXHOSTNAMELEN+1);
    return(cwd_server);
}

int DLL_DECL rfio_HsmIf_GetCwdType() {
    int *cwd_type = NULL;

    Cglobals_get(&cwdtype_key, (void **)&cwd_type, CA_MAXHOSTNAMELEN+1);
    return(*cwd_type);
}

int DLL_DECL rfio_HsmIf_SetCwdServer(const char *path) {
    char *cwd_server = NULL;

    Cglobals_get(&cwdserver_key, (void **)&cwd_server, CA_MAXHOSTNAMELEN+1);
    strncpy(cwd_server, path, CA_MAXHOSTNAMELEN);
	cwd_server[CA_MAXHOSTNAMELEN] = '\0';
    return(0);
}

int DLL_DECL rfio_HsmIf_SetCwdType(int n) {
    int *cwd_type = NULL;

    Cglobals_get(&cwdtype_key, (void **)&cwd_type, sizeof(int));
    *cwd_type = n;
    return(0);
}

int DLL_DECL rfio_HsmIf_GetHsmType(int fd, int *WrittenTo) {
    int rc = -1;

#if defined(CNS_ROOT)
    {
        int s_index;
        if ((s_index = rfio_CnsFilesfdt_findentry(fd,FINDCNSFILES_WITHOUT_SCAN)) >= 0) {
          if ( CnsFilesfdt[s_index]->hsmfile != NULL ) {
            rc = RFIO_HSM_CNS;
            if ( WrittenTo != NULL ) *WrittenTo = CnsFilesfdt[s_index]->written_to;
          }
        }
    }
#endif /* CNS_ROOT */
    return(rc);
}

int DLL_DECL rfio_HsmIf_FindPhysicalPath(char *hsm_path, char **physical_path) {
    int rc = -1;
#if defined(CNS_ROOT)
    rc = FindCnsPhysicalPath(hsm_path,physical_path);
#endif /* CNS_ROOT */

    return(rc);
}

int DLL_DECL rfio_HsmIf_access(const char *path, int amode) {
    int rc = -1;

#if defined(CNS_ROOT)
    if ( rfio_HsmIf_IsCnsFile(path) ) {
        rc = Cns_access(path,amode);
    }
#endif /* CNS_ROOT */
    return(rc);
}

int DLL_DECL rfio_HsmIf_chdir(const char *path) {
    int rc = -1;

#if defined(CNS_ROOT)
    if ( rfio_HsmIf_IsCnsFile(path) ) {
        if ( (rc = Cns_chdir(path)) == 0)
             rfio_HsmIf_SetCwdType(RFIO_HSM_CNS);
    }
#endif /* CNS_ROOT */
    return(rc);
}

int DLL_DECL rfio_HsmIf_chmod(const char *path, mode_t mode) {
    int rc = -1;

#if defined(CNS_ROOT)
    if ( rfio_HsmIf_IsCnsFile(path) ) {
        rc = Cns_chmod(path,mode);
    }
#endif /* CNS_ROOT */
    return(rc);
}

int DLL_DECL rfio_HsmIf_chown(const char *path, uid_t new_uid, gid_t new_gid) {
    int rc = -1;

#if defined(CNS_ROOT)
    if ( rfio_HsmIf_IsCnsFile(path) ) {
        rc = Cns_chown(path,new_uid,new_gid);
    }
#endif /* CNS_ROOT */
    return(rc);
}

int DLL_DECL rfio_HsmIf_close(int fd) {
    int rc = -1;
#if defined(CNS_ROOT)
    char upath[CA_MAXHOSTNAMELEN+MAXPATH+2];

    rc = rfio_HsmIf_getipath(fd,upath);
    if (rc == 1)
         rc = rfio_HsmIf_reqtoput(upath);
#endif /* CNS_ROOT */
   return(rc);
}

int DLL_DECL rfio_HsmIf_closedir(DIR *dirp) {
    rfio_HsmIf_DIRcontext_t *tmp = NULL; 
    rfio_HsmIf_DIRcontext_t **myDIRcontext = NULL;
    int rc = -1;

    tmp = (rfio_HsmIf_DIRcontext_t *)dirp;
#if defined(CNS_ROOT)
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
#endif /* CNS_ROOT */
    return(rc);
}

int DLL_DECL rfio_HsmIf_creat(const char *path, mode_t mode) {
    int rc = -1;

#if defined(CNS_ROOT)
    if ( rfio_HsmIf_IsCnsFile(path) ) {
        rc = Cns_creat(path,mode);
    }
#endif /* CNS_ROOT */
    return(rc);
}

char DLL_DECL *rfio_HsmIf_getcwd(char *buf, int size) {
    char *cwd = NULL;

#if defined(CNS_ROOT)
    if ( rfio_HsmIf_GetCwdType() == RFIO_HSM_CNS )
        cwd = Cns_getcwd(buf, size);
#endif /* CNS_ROOT */
    return(cwd);
}

/* Output is: -1 if error */
/*          :  0 if rfio_HsmIf_reqtoput(name) is NOT to be done */
/*          :  1 if rfio_HsmIf_reqtoput(name) IS to be done */
/* Note: name should point to a buffer of size CA_MAXHOSTNAMELEN+MAXPATH+2 */
int DLL_DECL rfio_HsmIf_getipath(int fd, char *name) {
    int rc = -1;
#if defined(CNS_ROOT)
    int flags, written_to;
    stage_hsm_t *hsmfile = NULL;
#endif /* CNS_ROOT */

#if defined(CNS_ROOT)
    /*
     * No specific close needed
     */
    rc = GetCnsFileDescriptor(fd,&flags,&hsmfile,&written_to);
    (void)DelCnsFileDescriptor(fd);
    if ( rc == -1 ) return(-1);
    if ( (flags & (O_RDONLY|O_RDWR|O_WRONLY)) != O_RDONLY && written_to != 0 )
        rc = 1;
    else
        rc = 0;
    /* Before cleanup of hsmfile we copy content of name */
    if (name != NULL) strcpy(name, hsmfile->upath);
    (void)CnsCleanup(&hsmfile);
#endif /* CNS_ROOT */
   return(rc);
}

int DLL_DECL rfio_HsmIf_mkdir(const char *path, mode_t mode) {
    int rc = -1;

#if defined(CNS_ROOT)
    if ( rfio_HsmIf_IsCnsFile(path) ) {
        rc = Cns_mkdir(path,mode);
    }
#endif /* CNS_ROOT */
    return(rc);
}





int DLL_DECL rfio_HsmIf_open(const char *path, int flags, mode_t mode, int mode64) {
    int rc = -1;
    int save_serrno, save_errno;
#if defined(CNS_ROOT)
    stage_hsm_t *hsmfile = NULL;
    struct Cns_filestat st;
    void (*this_stglog) _PROTO((int, char *)) = NULL;
#endif /* CNS_ROOT */

#if defined(CNS_ROOT)
    if ( rfio_HsmIf_IsCnsFile(path) ) {
        char *mover_protocol_rfio = MOVER_PROTOCOL_RFIO;
#if defined(CASTOR_ON_GLOBAL_FILESYSTEM)
	char *mover_protocol_alt;
	char *got_protocol;
        /*
         * Get the protocol used, default is MOVER_PROTOCOL_RFIO, i.e. "rfio"
         */
	if ((mover_protocol_alt = getenv("MOVER_PROTOCOL_RFIO")) != NULL) {
	  mover_protocol_rfio = mover_protocol_alt;
	}
#endif
        /*
         * Check if an existing file is going to be updated
         */
        memset(&st,'\0',sizeof(st));
        rc = Cns_stat(path,&st);
        /*
         * Make sure that filesize is 0 if file doesn't exist (a
         * overdoing) or that we will create (stage_out) if O_TRUNC.
         */
        if ( rc == -1 || ((flags & O_TRUNC) != 0)) st.filesize = 0;
        /*
         * Do the work
         */
        hsmfile = (stage_hsm_t *)calloc(1,sizeof(stage_hsm_t));
        hsmfile->xfile = (char *)malloc(strlen(path)+1);
        hsmfile->upath = (char *)malloc(CA_MAXHOSTNAMELEN+CA_MAXPATHLEN+2);
        strcpy(hsmfile->xfile,path);
        hsmfile->size = st.filesize;
        hsmfile->next = NULL;
	/** NOW CALL THE NEW STAGER API */
       
        if ((stage_getlog((void (**) _PROTO((int,char *))) &this_stglog) == 0) && 
            (this_stglog == NULL)) {
          stage_setlog((void (*) _PROTO((int,char *)))&rfio_stglog); 
        } 
        if (use_castor2_api()) {
          struct stage_io_fileresp *response;
          char *requestId, *url;

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
                            &requestId,
                            NULL);
            save_errno = errno;
            save_serrno = serrno;
            if (rc < 0) {
              if ( (save_serrno == SECOMERR) ||
                   (save_serrno == SETIMEDOUT) ||
                   (save_serrno == ECONNREFUSED) ||
                   (save_errno == ECONNREFUSED) ) {
                int retrySleepTime;
                retrySleepTime = 1 + (int)(10.0*rand()/(RAND_MAX+1.0));
                sleep(retrySleepTime);
                continue;
              }
              return -1;
            } else break;
          }

          if (response == NULL) {
            TRACE(3,"rfio","Received NULL response");
            serrno = SEINTERNAL;
            return -1;
          }
          
          if (response->errorCode != 0) {
            TRACE(3,"rfio","stage_open error: %d/%s",
                  response->errorCode, response->errorMessage);
            serrno = response->errorCode;
            return -1;
          }

          url = stage_geturl(response);
          if (url == 0) {
            free(response);
            return -1;
          }

#if defined(CASTOR_ON_GLOBAL_FILESYSTEM)
	  got_protocol = response->protocol;
	  if (strcmp(response->protocol,"file") == 0) {
	    char *got_server;
	    int got_port;
	    char *got_filename;
	    int internal_fd;
	    char *rfio_path;
	    int index;

	    /* We are access a CASTOR file using a global filesystem */

	    if ((rfio_path = (char *) malloc(strlen("rfio://") +
					     strlen(response->server) +
					     strlen(":") +
					     strlen("2147483647") +
					     strlen("/") +
					     strlen(response->filename) + 1)) == NULL) {
	      serrno = errno;
	      free(response);
	      return -1;
	    }
	    sprintf(rfio_path,"rfio://%s:%d/%s",response->server,response->port,response->filename);

	    /* Instanciate the wrapper by opening internal remote connection */
	    if (mode64) {
	      internal_fd = rfio_open64(rfio_path, flags, mode);
	    } else {
	      internal_fd = rfio_open(rfio_path, flags, mode);
	    }

	    free(rfio_path);

	    if (internal_fd < 0) {
	      free(response);
	      return -1;
	    }

	    /* Try to open the local file */
	    if (mode64) {
	      rc = rfio_open64(response->filename, flags, mode);
	    } else {
	      rc = rfio_open(response->filename, flags, mode);
	    }

	    if (rc < 0) {
	      /* Oups */
	      rfio_close(internal_fd);
	    } else {
	      /* Try to remember the tuple {user_fd=rc,internal_fd=internal_fd} */
	      if ((index = rfio_lcastorfdt_allocentry(rc)) < 0) {
		/* Oups */
		rfio_close(internal_fd);
		rfio_close(rc);
		serrno = SEINTERNAL;
		rc = -1;
	      } else {
		/* Fill the tuple in our internal table */
		if (rfio_lcastorfdt_fillentry(index,rc,internal_fd) < 0) {
		  /* Oups */
		  rfio_close(internal_fd);
		  rfio_close(rc);
		  serrno = SEINTERNAL;
		  rc = -1;
		}
	      }
	    }
	  } else {
#endif /* defined(CASTOR_ON_GLOBAL_FILESYSTEM) */
	    if (mode64) {
	      rc = rfio_open64(url, flags, mode);
	    } else {
	      rc = rfio_open(url, flags, mode);
	    }
#if defined(CASTOR_ON_GLOBAL_FILESYSTEM)
	  }
#endif /* defined(CASTOR_ON_GLOBAL_FILESYSTEM) */
    free(response);
    free(url);
  } else {
	  TRACE(3,"rfio","Using OLD stage API\n");
	  {
	    struct stgcat_entry stcp_input;
	    int nstcp_output;
	    struct stgcat_entry *stcp_output = NULL;
	    
	    memset(&stcp_input, 0, sizeof(struct stgcat_entry));
	    if (strlen(path) > STAGE_MAX_HSMLENGTH) { 
	      /* Oupsss... Stager api limitation */
	      if (this_stglog == NULL) stage_setlog((void (*) _PROTO((int,char *))) NULL); 
	      (void)CnsCleanup(&hsmfile); 
	      serrno = SENAMETOOLONG; 
	      return(-1); 
	    } 
	    strcpy(stcp_input.u1.h.xfile,path);
	    if ( (flags & (O_RDONLY|O_RDWR|O_WRONLY)) == O_RDONLY || st.filesize > 0 ) 
	      rc = stage_in_hsm((u_signed64) 0,          /* Ebusy is possible... */
				(int) flags,             /* open flags */
				(char *) NULL,           /* hostname */
				(char *) NULL,           /* pooluser */
				(int) 1,                 /* nstcp_input */
				(struct stgcat_entry *) &stcp_input, /* stcp_input */
				(int *) &nstcp_output,   /* nstcp_output */
				(struct stgcat_entry **) &stcp_output, /* stcp_output */
				(int) 0,                 /* nstpp_input */
				(struct stgpath_entry *) NULL /* stpp_input */
				);
	    else
	      rc = stage_out_hsm((u_signed64) 0,          /* Ebusy is possible... */
				 (int) flags,             /* open flags */
				 (mode_t) mode,           /* open mode (c.f. also umask) */
				 (char *) NULL,           /* hostname */
				 (char *) NULL,           /* pooluser */
				 (int) 1,                 /* nstcp_input */
				 (struct stgcat_entry *) &stcp_input, /* stcp_input */
				 (int *) &nstcp_output,   /* nstcp_output */
				 (struct stgcat_entry **) &stcp_output, /* stcp_output */
				 (int) 0,                       /* nstpp_input */
				 (struct stgpath_entry *) NULL  /* stpp_input */
				 );
	    if ( rc == -1 ) {
	      save_serrno = serrno;
	      if (this_stglog == NULL) stage_setlog((void (*) _PROTO((int,char *))) NULL);
	      (void)CnsCleanup(&hsmfile);
	      serrno = save_serrno;
	      return(-1);
	    }
	    if ((nstcp_output != 1) || (stcp_output == NULL) || (*(stcp_output->ipath) == '\0')) {
	      /* Impossible */
	      save_serrno = SEINTERNAL;
	      if (this_stglog == NULL) stage_setlog((void (*) _PROTO((int,char *))) NULL);
	      (void)CnsCleanup(&hsmfile);
	      serrno = save_serrno;
	      if (stcp_output != NULL) free(stcp_output);
	      return(-1);
	    }
	    strcpy(hsmfile->upath,stcp_output->ipath);
	    free(stcp_output);
	  }
	  if (mode64)
            rc = rfio_open64(hsmfile->upath,flags,mode);
	  else
            rc = rfio_open(hsmfile->upath,flags,mode);
	  if ( rc == -1 ) {
            save_serrno = (rfio_errno > 0 ? rfio_errno : serrno);
            if (this_stglog == NULL) stage_setlog((void (*) _PROTO((int,char *))) NULL);
            (void)CnsCleanup(&hsmfile);
            serrno = save_serrno;
            return(-1);
	  }
	  if ( AddCnsFileDescriptor(rc,flags,hsmfile) == -1 ) {
            save_serrno = serrno;
            if (this_stglog == NULL) stage_setlog((void (*) _PROTO((int,char *))) NULL);
            (void)CnsCleanup(&hsmfile);
            rfio_close(rc);
            serrno = save_serrno;
            return(-1);
	  }
	  if ( st.filesize == 0 ) SetCnsWrittenTo(rc);
	  if (this_stglog == NULL) stage_setlog((void (*) _PROTO((int,char *))) NULL);
	}
    }
#endif /* CNS_ROOT */
    return(rc);
}

int DLL_DECL rfio_HsmIf_open_limbysz(const char *path, int flags, mode_t mode, u_signed64 maxsize, int mode64) {
    int rc = -1;
    int save_serrno;
#if defined(CNS_ROOT)
    stage_hsm_t *hsmfile = NULL;
    struct Cns_filestat st;
    void (*this_stglog) _PROTO((int, char *)) = NULL;
#endif /* CNS_ROOT */

#if defined(CNS_ROOT)
    if ( rfio_HsmIf_IsCnsFile(path) ) {
        char *mover_protocol_rfio = MOVER_PROTOCOL_RFIO;
#if defined(CASTOR_ON_GLOBAL_FILESYSTEM)
	char *mover_protocol_alt;
	char *got_protocol;
        /*
         * Get the protocol used, default is MOVER_PROTOCOL_RFIO, i.e. "rfio"
         */
	if ((mover_protocol_alt = getenv("MOVER_PROTOCOL_RFIO")) != NULL) {
	  mover_protocol_rfio = mover_protocol_alt;
	}
#endif
        /*
         * Check if an existing file is going to be updated
         */
        memset(&st,'\0',sizeof(st));
        rc = Cns_stat(path,&st);
        /*
         * Make sure that filesize is 0 if file doesn't exist (a
         * overdoing) or that we will create (stage_out) if O_TRUNC.
         */
        if ( rc == -1 || ((flags & O_TRUNC) != 0)) st.filesize = 0;
        /*
         * Do the work
         */
        hsmfile = (stage_hsm_t *)calloc(1,sizeof(stage_hsm_t));
        hsmfile->xfile = (char *)malloc(strlen(path)+1);
        hsmfile->upath = (char *)malloc(CA_MAXHOSTNAMELEN+CA_MAXPATHLEN+2);
        strcpy(hsmfile->xfile,path);
        hsmfile->size = st.filesize;
        hsmfile->next = NULL;


	/* New stager API call */
	if (use_castor2_api()) {
          struct stage_io_fileresp *response;
          char *requestId, *url;

          TRACE(3,"rfio","Calling stage_open with: %s %x %x",
                path, flags, mode);
          rc = stage_open(NULL,
                          mover_protocol_rfio,
                          path,
                          flags,
                          mode,
			  maxsize,
                          &response,
                          &requestId,
                          NULL);
          if (rc < 0) {
            return -1;
          }

          if (response == NULL) {
            TRACE(3,"rfio","Received NULL response");
            serrno = SEINTERNAL;
            return -1;
          }
          
          if (response->errorCode != 0) {
            TRACE(3,"rfio","stage_open error: %d/%s",
                  response->errorCode, response->errorMessage);
            serrno = response->errorCode;
            return -1;
          }

          url = stage_geturl(response);
          if (url == 0) {
            free(response);
            return -1;
          }

#if defined(CASTOR_ON_GLOBAL_FILESYSTEM)
	  got_protocol = response->protocol;
	  if (strcmp(response->protocol,"file") == 0) {
	    char *got_server;
	    int got_port;
	    char *got_filename;
	    int internal_fd;
	    char *rfio_path;
	    int index;

	    /* We are access a CASTOR file using a global filesystem */

	    if ((rfio_path = (char *) malloc(strlen("rfio://") +
					     strlen(response->server) +
					     strlen(":") +
					     strlen("2147483647") +
					     strlen("/") +
					     strlen(response->filename) + 1)) == NULL) {
	      serrno = errno;
	      free(response);
	      return -1;
	    }
	    sprintf(rfio_path,"rfio://%s:%d/%s",response->server,response->port,response->filename);

	    /* Instanciate the wrapper by opening internal remote connection */
	    if (mode64) {
	      internal_fd = rfio_open64(rfio_path, flags, mode);
	    } else {
	      internal_fd = rfio_open(rfio_path, flags, mode);
	    }

	    free(rfio_path);

	    if (internal_fd < 0) {
	      free(response);
	      return -1;
	    }

	    /* Try to open the local file */
	    if (mode64) {
	      rc = rfio_open64(response->filename, flags, mode);
	    } else {
	      rc = rfio_open(response->filename, flags, mode);
	    }

	    if (rc < 0) {
	      /* Oups */
	      rfio_close(internal_fd);
	    } else {
	      /* Try to remember the tuple {user_fd=rc,internal_fd=internal_fd} */
	      if ((index = rfio_lcastorfdt_allocentry(rc)) < 0) {
		/* Oups */
		rfio_close(internal_fd);
		rfio_close(rc);
		serrno = SEINTERNAL;
		rc = -1;
	      } else {
		/* Fill the tuple in our internal table */
		if (rfio_lcastorfdt_fillentry(index,rc,internal_fd) < 0) {
		  /* Oups */
		  rfio_close(internal_fd);
		  rfio_close(rc);
		  serrno = SEINTERNAL;
		  rc = -1;
		}
	      }
	    }
	  } else {
#endif /* defined(CASTOR_ON_GLOBAL_FILESYSTEM) */
	    if (mode64) {
	      rc = rfio_open64(url, flags, mode);
	    } else {
	      rc = rfio_open(url, flags, mode);
	    }
#if defined(CASTOR_ON_GLOBAL_FILESYSTEM)
	  }
#endif /* defined(CASTOR_ON_GLOBAL_FILESYSTEM) */
          free(response);
          free(url); 


	} else {
	  /* Old stager copatibility mode */
	  if ((stage_getlog((void (**) _PROTO((int,char *))) &this_stglog) == 0) && (this_stglog == NULL)) {
	    stage_setlog((void (*) _PROTO((int,char *)))&rfio_stglog);
	  }
	  {
	    struct stgcat_entry stcp_input;
	    int nstcp_output;
	    struct stgcat_entry *stcp_output = NULL;

	    memset(&stcp_input, 0, sizeof(struct stgcat_entry));
	    if (strlen(path) > STAGE_MAX_HSMLENGTH) {
	      /* Oupsss... Stager api limitation */
	      if (this_stglog == NULL) stage_setlog((void (*) _PROTO((int,char *))) NULL);
	      (void)CnsCleanup(&hsmfile);
	      serrno = SENAMETOOLONG;
	      return(-1);
	    }
	    strcpy(stcp_input.u1.h.xfile,path);
	    if (maxsize > 0) stcp_input.size = maxsize;
	    if ( (flags & (O_RDONLY|O_RDWR|O_WRONLY)) == O_RDONLY || st.filesize > 0 ) {
	      rc = stage_in_hsm((u_signed64) 0,          /* Ebusy is possible... */
				(int) flags,             /* open flags */
				(char *) NULL,           /* hostname */
				(char *) NULL,           /* pooluser */
				(int) 1,                 /* nstcp_input */
				(struct stgcat_entry *) &stcp_input, /* stcp_input */
				(int *) &nstcp_output,   /* nstcp_output */
				(struct stgcat_entry **) &stcp_output, /* stcp_output */
				(int) 0,                 /* nstpp_input */
				(struct stgpath_entry *) NULL /* stpp_input */
				);
	      if ((rc != 0) && (serrno == ERTLIMBYSZ)) {
		/* It is by definition possible to have 'File limited by size' error on a recall */
		/* where we specified the maximum number of files to transfer */
		/* This is called by rfcp.c only btw */
		rc = 0;
	      }
	    } else {
	      rc = stage_out_hsm((u_signed64) 0,          /* Ebusy is possible... */
				 (int) flags,             /* open flags */
				 (mode_t) mode,           /* open mode (c.f. also umask) */
				 (char *) NULL,           /* hostname */
				 (char *) NULL,           /* pooluser */
				 (int) 1,                 /* nstcp_input */
				 (struct stgcat_entry *) &stcp_input, /* stcp_input */
				 (int *) &nstcp_output,   /* nstcp_output */
				 (struct stgcat_entry **) &stcp_output, /* stcp_output */
				 (int) 0,                       /* nstpp_input */
				 (struct stgpath_entry *) NULL  /* stpp_input */
				 );
	    }
	    if ( rc == -1 ) {
	      save_serrno = serrno;
	      if (this_stglog == NULL) stage_setlog((void (*) _PROTO((int,char *))) NULL);
	      (void)CnsCleanup(&hsmfile);
	      serrno = save_serrno;
	      return(-1);
	    }
	    if ((nstcp_output != 1) || (stcp_output == NULL) || (*(stcp_output->ipath) == '\0')) {
	      /* Impossible */
	      save_serrno = SEINTERNAL;
	      if (this_stglog == NULL) stage_setlog((void (*) _PROTO((int,char *))) NULL);
	      (void)CnsCleanup(&hsmfile);
	      serrno = save_serrno;
	      if (stcp_output != NULL) free(stcp_output);
	      return(-1);
	    }
	    strcpy(hsmfile->upath,stcp_output->ipath);
	    free(stcp_output);
	  }
	  if (mode64)
            rc = rfio_open64(hsmfile->upath,flags,mode);
	  else
            rc = rfio_open(hsmfile->upath,flags,mode);
	  if ( rc == -1 ) {
            save_serrno = (rfio_errno > 0 ? rfio_errno : serrno);
            if (this_stglog == NULL) stage_setlog((void (*) _PROTO((int,char *))) NULL);
            (void)CnsCleanup(&hsmfile);
            serrno = save_serrno;
            return(-1);
	  }
	  if ( AddCnsFileDescriptor(rc,flags,hsmfile) == -1 ) {
            save_serrno = serrno;
            if (this_stglog == NULL) stage_setlog((void (*) _PROTO((int,char *))) NULL);
            (void)CnsCleanup(&hsmfile);
            rfio_close(rc);
            serrno = save_serrno;
            return(-1);
	  }
	  if ( st.filesize == 0 ) SetCnsWrittenTo(rc);
	  if (this_stglog == NULL) stage_setlog((void (*) _PROTO((int,char *))) NULL);

	}
    }
#endif /* CNS_ROOT */
    return(rc);
}

DIR DLL_DECL *rfio_HsmIf_opendir(const char *path) {
    rfio_HsmIf_DIRcontext_t *tmp = NULL;
    char *p;

#if defined(CNS_ROOT)
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
#endif /* CNS_ROOT */
    if ( tmp != NULL && rfio_HsmIf_AddDirEntry(tmp) == -1 ) {
#if defined(CNS_ROOT)
        (void) Cns_closedir((Cns_DIR *)(tmp->dirp));
        free(tmp->de);
        free(tmp);
#endif /* CNS_ROOT */
        return(NULL);
    }
    return((DIR *)tmp);
}

int DLL_DECL rfio_HsmIf_reqtoput(char *name) {
    int rc = -1;
    int save_serrno;
#if defined(CNS_ROOT)
    

    if (use_castor2_api()) {
      rc = 0;
    } else {
      stage_hsm_t hsmfile; 
      void (*this_stglog) _PROTO((int, char *)) = NULL; 
      if ((stage_getlog((void (**) _PROTO((int,char *))) &this_stglog) == 0) && (this_stglog == NULL)) { 
	stage_setlog((void (*) _PROTO((int,char *)))&rfio_stglog); 
      } 
      memset(&hsmfile, 0, sizeof(stage_hsm_t)); 
      hsmfile.upath = name; 
      rc = stage_updc_user((char *) NULL, (stage_hsm_t *) &hsmfile); 
      save_serrno = serrno; 
     if (this_stglog == NULL) stage_setlog((void (*) _PROTO((int,char *))) NULL); 
     serrno = save_serrno; 
    }
#endif
    return(rc);
}

int DLL_DECL rfio_HsmIf_stat(const char *path, struct stat *st) {
    rfio_HsmIf_DIRcontext_t **myDIRcontext = NULL;
#if defined(CNS_ROOT)
    struct Cns_filestat statbuf;
#endif /* CNS_ROOT */
    char *current_entry = NULL;
    char *dirpath = NULL;
    char *p = NULL;
    int rc = -1;

#if defined(CNS_ROOT)
    if ( rfio_HsmIf_IsCnsFile(path) ) {
        Cglobals_get(&DIRcontext_key,(void **)&myDIRcontext,
                    sizeof(rfio_HsmIf_DIRcontext_t));
        if ( myDIRcontext != NULL && *myDIRcontext != NULL && (*myDIRcontext)->current_entry != NULL && (*myDIRcontext)->dirpath != NULL) {
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
#endif /* CNS_ROOT */
    return(rc);
}

int DLL_DECL rfio_HsmIf_stat64(const char *path, struct stat64 *st) {
    rfio_HsmIf_DIRcontext_t **myDIRcontext = NULL;
#if defined(CNS_ROOT)
    struct Cns_filestat statbuf;
#endif /* CNS_ROOT */
    char *current_entry = NULL;
    char *dirpath = NULL;
    char *p = NULL;
    int rc = -1;

#if defined(CNS_ROOT)
    if ( rfio_HsmIf_IsCnsFile(path) ) {
        Cglobals_get(&DIRcontext_key,(void **)&myDIRcontext,
                    sizeof(rfio_HsmIf_DIRcontext_t));
        if ( myDIRcontext != NULL && *myDIRcontext != NULL && (*myDIRcontext)->current_entry != NULL && (*myDIRcontext)->dirpath != NULL) {
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
#endif /* CNS_ROOT */
    return(rc);
}

int DLL_DECL rfio_HsmIf_read(int fd, void *buffer, int size) {
    int rc = -1;

#if defined(CNS_ROOT)
    /* 
     * For CASTOR HSM this is a no-op.
     */
    rc = size;
#endif /* CNS_ROOT */
    return(rc);
}

struct dirent DLL_DECL *rfio_HsmIf_readdir(DIR *dirp) {
    rfio_HsmIf_DIRcontext_t *tmp = NULL;
    rfio_HsmIf_DIRcontext_t **myDIRcontext = NULL;
    struct dirent *tmpdirent = NULL;
#if defined(CNS_ROOT)
    struct Cns_direnstat *tmp_ds;
#endif /* CNS_ROOT */

    tmp = (rfio_HsmIf_DIRcontext_t *)dirp;
#if defined(CNS_ROOT)
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
#endif /* CNS_ROOT */
    return(tmpdirent);
}

int DLL_DECL rfio_HsmIf_rename(const char *opath, const char *npath) {
    int rc = -1;

#if defined(CNS_ROOT)
    if ( rfio_HsmIf_IsCnsFile(opath) &&
         rfio_HsmIf_IsCnsFile(npath) ) {
        rc =Cns_rename(opath,npath);
    }
#endif /* CNS_ROOT */
    return(rc);
}

void DLL_DECL rfio_HsmIf_rewinddir(DIR *dirp) {
    rfio_HsmIf_DIRcontext_t *tmp = NULL;

    tmp = (rfio_HsmIf_DIRcontext_t *)dirp;
#if defined(CNS_ROOT)
    if ( tmp->HsmType == RFIO_HSM_CNS )
        Cns_rewinddir((Cns_DIR *)(tmp->dirp));
#endif /* CNS_ROOT */
    return;
}

int DLL_DECL rfio_HsmIf_rmdir(const char *path) {
    int rc = -1;

#if defined(CNS_ROOT)
    if ( rfio_HsmIf_IsCnsFile(path) ) {
        rc = Cns_rmdir(path);
    }
#endif /* CNS_ROOT */
    return(rc);
}

int DLL_DECL rfio_HsmIf_unlink(const char *path) {
    int rc = -1;

#if defined(CNS_ROOT)
    if ( rfio_HsmIf_IsCnsFile(path) ) {
        rc = Cns_unlink(path);
    }
#endif /* CNS_ROOT */
    return(rc);
}

int DLL_DECL rfio_HsmIf_write(int fd, void *buffer, int size) {
    int rc = -1;

#if defined(CNS_ROOT)
    /*
     * For CASTOR HSM this is a no-op.
     */
    rc = size;
#endif /* CNS_ROOT */
    return(rc);
}

int DLL_DECL rfio_HsmIf_FirstWrite(int fd, void *buffer, int size) {
    int rc = 0;
    int flags, written_to;
    int save_serrno;
#if defined(CNS_ROOT)
    stage_hsm_t *hsmfile;
    void (*this_stglog) _PROTO((int, char *)) = NULL;
#endif /* CNS_ROOT */

    if ( size == 0 )
        return (0);
    if ( size < 0 ) {
        serrno = EINVAL;
        return (-1);
    }
#if defined(CNS_ROOT)

    if (use_castor2_api()) {
      rc = 0;
    } else {

      /*BC Not needed by the new stager anymore */
      if ( GetCnsFileDescriptor(fd,&flags,&hsmfile,&written_to) < 0 ) 
	return (-1); 
      if ( (flags & (O_WRONLY|O_RDWR|O_APPEND)) == 0 ) { 
	serrno = EBADF; 
	return (-1); 
      } 
      if ( written_to ) 
	return (0); 
      if ((stage_getlog((void (**) _PROTO((int,char *))) &this_stglog) == 0) && (this_stglog == NULL)) { 
	stage_setlog((void (*) _PROTO((int,char *)))&rfio_stglog); 
      } 
      rc = stage_updc_filchg((char *) NULL,(stage_hsm_t *) hsmfile); 
      save_serrno = serrno; 
      if (this_stglog == NULL) stage_setlog((void (*) _PROTO((int,char *))) NULL); 
      serrno = save_serrno; 
      if ( rc == -1 ) return(-1); 
      rc = SetCnsWrittenTo(fd); 
      rc = 0;
    }
#endif /* CNS_ROOT */
    return(rc);
}

int DLL_DECL rfio_HsmIf_IOError(int fd, int errorcode) {
    int rc = -1;

#if defined(CNS_ROOT)
    /*
     * Should handle ENOSPC on write here
     */
    rc = 0;
#endif /* CNS_ROOT */
    return(rc);
}

#if defined(CNS_ROOT)
/*
 * Seach for a free index in the CnsFilesfdt table
 */
static int rfio_CnsFilesfdt_allocentry(s)
     int s;
{
#ifdef _WIN32
  int i;
  int rc;

  if (Cmutex_lock((void *) CnsFilesfdt,-1) != 0) {
    return(-1);
  }
  /* Scan it */

  for (i = 0; i < MAXRFD; i++) {
    if (CnsFilesfdt[i] == NULL) {
      rc = i;
      CnsFilesfdt[i] = &dummyCnsFile;
      goto _rfio_CnsFilesfdt_allocentry_return;
    }
  }

  serrno = ENOENT;
  rc = -1;

 _rfio_CnsFilesfdt_allocentry_return:
  if (Cmutex_unlock((void *) CnsFilesfdt) != 0) {
    return(-1);
  }
  return(rc);
#else /* _WIN32 */
  return(((s >= 0) && (s < MAXRFD)) ? s : -1);
#endif /* _WIN32 */
}

/*
 * Seach for a given index in the CnsFilesfdt table
 * On UNIX, if scanflag is FINDCNSFILES_WITH_SCAN,
 * a scan of table content is performed, otherwise
 * only boundary and content within the boundary
 * is performed.
 */
static int rfio_CnsFilesfdt_findentry(s,scanflag)
     int s;
     int scanflag;
{
  int i;
#ifdef _WIN32
  int rc;

  if (Cmutex_lock((void *) CnsFilesfdt,-1) != 0) {
    return(-1);
  }
  /* Scan it */

  for (i = 0; i < MAXRFD; i++) {
    if (CnsFilesfdt[i] != NULL) {
      if (CnsFilesfdt[i]->s == s) {
        rc = i;
        goto _rfio_CnsFilesfdt_findentry_return;
      }
    }
  }

  serrno = ENOENT;
  rc = -1;

 _rfio_CnsFilesfdt_findentry_return:
  if (Cmutex_unlock((void *) CnsFilesfdt) != 0) {
    return(-1);
  }
  return(rc);
#else /* _WIN32 */
  if (scanflag == FINDCNSFILES_WITH_SCAN) {
    for (i = 0; i < MAXRFD; i++) {
      if (CnsFilesfdt[i] != NULL) {
        if (CnsFilesfdt[i]->s == s) {
          return(i);
        }
      }
    }
    return(-1);
  } else {
    return(((s >= 0) && (s < MAXRFD) && (CnsFilesfdt[s] != NULL)) ? s : -1);
  }
#endif /* _WIN32 */
}


/*
 * Free a given index in the CnsFilesfdt table
 * Warning : the argument is REALLY an index
 */
static int rfio_CnsFilesfdt_freeentry(s)
     int s;
{
#ifdef _WIN32
  if (Cmutex_lock((void *) CnsFilesfdt,-1) != 0) {
    return(-1);
  }
  if (CnsFilesfdt[s] != NULL) {
    if (CnsFilesfdt[s] != &dummyCnsFile) free(CnsFilesfdt[s]);
    CnsFilesfdt[s] = NULL;
  }
  if (Cmutex_unlock((void *) CnsFilesfdt) != 0) {
    return(-1);
  }
#else /* _WIN32 */
  if ((s >= 0) && (s < MAXRFD) && (CnsFilesfdt[s] != NULL)) {
    if (CnsFilesfdt[s] != &dummyCnsFile) free((char *)CnsFilesfdt[s]);
    CnsFilesfdt[s] = NULL;
  }
#endif /* _WIN32 */
  return(0);
}
#endif /* CNS_ROOT */

/* Function that is saying if we work in old CASTOR1 compat mode (default) or CASTOR2 mode */
int DLL_DECL use_castor2_api() {
  char *p;

  if (((p = getenv(RFIO_USE_CASTOR_V2)) == NULL) &&
      ((p = getconfent("RFIO","USE_CASTOR_V2",0)) == NULL)) {
    /* Variable not set: compat mode */
    return(0);
  }
  if ((strcmp(p,"YES") == 0) || (strcmp(p,"yes") == 0) || (atoi(p) == 1)) {
    /* Variable set to yes or 1 : new mode */
    return(1);
  }
  /* Variable set but not to 1 : compat mode */
  return(0);
}
