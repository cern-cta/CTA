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
#else
#include <winsock2.h>
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

extern int tStageHostKey;
extern int tStagePortKey;
extern int tSvcClassKey;
extern int tCastorVersionKey;



#if defined(CNS_ROOT)

typedef struct CnsFiles {
  int s;
  int mode;
  int written_to;
  stage_hsm_t *hsmfile;
} CnsFiles_t;

#endif /* CNS_ROOT */



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


#if defined(CNS_ROOT)
#define FINDCNSFILES_WITH_SCAN     1
#define FINDCNSFILES_WITHOUT_SCAN  0



EXTERN_C char *getconfent _PROTO(());   /* CASTOR to access /etc/castor.conf util */
int DLL_DECL use_castor2_api _PROTO(()); 

/* getDefaultForGlobal defined in the parse.c file */
int getDefaultForGlobal(char** host,int* port,char** svc,int* version);

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


/************************************************/
/*                                              */
/*        CnsCleanup for castor                 */
/*                                              */ 
/************************************************/

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

/************************************************/
/*                                              */
/*        rfio_HsmIf_getipath for castor        */
/*                                              */ 
/************************************************/

int DLL_DECL rfio_HsmIf_getipath(int fd, char *name) {

/* Output is: -1 if error */
/*          :  0 if rfio_HsmIf_reqtoput(name) is NOT to be done */
/*          :  1 if rfio_HsmIf_reqtoput(name) IS to be done */
/* Note: name should point to a buffer of size CA_MAXHOSTNAMELEN+MAXPATH+2 */

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


/********************************************/
/*                                          */
/*        rfio_HsmIf_open for castor        */
/*                                          */ 
/********************************************/

int DLL_DECL rfio_HsmIf_open(const char *path, int flags, mode_t mode, int mode64) {
  int rc = -1;
  int ret;
  int save_serrno, save_errno;
  int* auxVal;
  char ** auxPoint;
#if defined(CNS_ROOT)
  stage_hsm_t *hsmfile = NULL;
  struct Cns_filestat st;
  void (*this_stglog) _PROTO((int, char *)) = NULL;
#endif /* CNS_ROOT */
  struct stage_options opts;
  opts.stage_host=NULL;
  opts.stage_port=0;
  opts.service_class=NULL;
  opts.stage_version=0;

  ret=Cglobals_get(& tStageHostKey, (void**) &auxPoint,sizeof(void*));
  if(ret==0){
    opts.stage_host=*auxPoint;
  }
  ret=Cglobals_get(& tStagePortKey, (void**) &auxVal,sizeof(int));
  if(ret==0){
    opts.stage_port=*auxVal;
  }
  ret=Cglobals_get(& tCastorVersionKey, (void**) &auxVal,sizeof(int));
  if(ret==0){
    opts.stage_version=*auxVal;
  }
  ret=Cglobals_get(& tSvcClassKey, (void**) &auxPoint,sizeof(void*));
  if (ret==0){
    opts.service_class=*auxPoint;
  }

  ret= getDefaultForGlobal(&opts.stage_host,&opts.stage_port,&opts.service_class,&opts.stage_version);
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

    if ((stage_getlog((void (**) _PROTO((int,char *))) &this_stglog) == 0) &&
        (this_stglog == NULL)) {
      stage_setlog((void (*) _PROTO((int,char *)))&rfio_stglog);
    }
    if (use_castor2_api()) {
      /** NOW CALL THE NEW STAGER API */
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
                        &opts);
        save_errno = errno;
        save_serrno = serrno;
        if (rc < 0) {
          if ( (save_serrno == SECOMERR) ||
               (save_serrno == SETIMEDOUT) ||
#ifdef _WIN32
               (save_serrno == WSAECONNREFUSED) ||
#else
               (save_serrno == ECONNREFUSED) ||
#endif
#ifdef _WIN32
               (save_errno == WSAECONNREFUSED)
#else
               (save_errno == ECONNREFUSED)
#endif
               ) {
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
      /*
       * Do the work
       */
      hsmfile = (stage_hsm_t *)calloc(1,sizeof(stage_hsm_t));
      hsmfile->xfile = (char *)malloc(strlen(path)+1);
      hsmfile->upath = (char *)malloc(CA_MAXHOSTNAMELEN+CA_MAXPATHLEN+2);
      strcpy(hsmfile->xfile,path);
      hsmfile->size = st.filesize;
      hsmfile->next = NULL;
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


/********************************************/
/*                                          */
/*        rfio_HsmIf_close for castor       */
/*                                          */ 
/********************************************/


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



/********************************************/
/*                                          */
/*        rfio_HsmIf_unlink for castor      */
/*                                          */ 
/********************************************/


int DLL_DECL rfio_HsmIf_unlink(const char *path) {
  int rc = -1;

#if defined(CNS_ROOT)
  if ( rfio_HsmIf_IsCnsFile(path) ) {
    rc = Cns_unlink(path);
  }
#endif /* CNS_ROOT */
  return(rc);
}


/***************************************************/
/*                                                 */
/*        rfio_HsmIf_open_limbyszs for castor      */
/*                                                 */ 
/***************************************************/


int DLL_DECL rfio_HsmIf_open_limbysz(const char *path, int flags, mode_t mode, u_signed64 maxsize, int mode64) {
  int rc = -1;
  int ret;
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
    int* auxVal;
    char ** auxPoint;
    struct stage_options opts;
    opts.stage_host=NULL;
    opts.stage_port=0;
    opts.service_class=NULL;
    opts.stage_version=0;

    ret=Cglobals_get(& tStageHostKey, (void**) &auxPoint,sizeof(void*));
    if(ret==1){
      opts.stage_host=*auxPoint;
    }
    ret=Cglobals_get(& tStagePortKey, (void**) &auxVal,sizeof(int));
    if(ret==1){
      opts.stage_port=*auxVal;
    }
    ret=Cglobals_get(& tCastorVersionKey, (void**) &auxVal,sizeof(int));
    if(ret==1){
      opts.stage_version=*auxVal;
    }
    ret=Cglobals_get(& tSvcClassKey, (void**) &auxPoint,sizeof(void*));
    if (ret==1){
      opts.service_class=*auxPoint;
    }

    ret= getDefaultForGlobal(&opts.stage_host,&opts.stage_port,&opts.service_class,&opts.stage_version);

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
                      &opts);
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


/**********************************************/
/*                                            */
/*        rfio_HsmIf_reqtoput for castor      */
/*                                            */ 
/**********************************************/



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


/**************************************************/
/*                                                */
/*       rfio_HsmIf_FirstWrite for castor         */
/*                                                */ 
/**************************************************/


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

/**********************************************/
/*                                            */
/*       use_castor2_api() for castor         */
/*                                            */ 
/**********************************************/

int DLL_DECL use_castor2_api() {

/* Function that is saying if we work in old CASTOR1 compat mode (default) or CASTOR2 mode */
  char *p;
  int ret=0;
  char** globalHost;
  char** globalSvc;
  int* globalVersion;
  int* globalPort;

  globalHost=globalSvc=0;
  globalVersion=globalPort=0;

  ret=Cglobals_get(& tCastorVersionKey, (void**) &globalVersion,sizeof(int));
  if(ret==0 && (*globalVersion==1 ||*globalVersion==2)){
    return (*globalVersion-1);

  }
  if (ret<0) return 0;

/* Let's now find the global variable thread specific */

  ret=Cglobals_get(&tStageHostKey,(void **)&globalHost,sizeof(void*));
  if (ret<0) return 0; 

  ret=Cglobals_get(&tSvcClassKey,(void **)&globalSvc,sizeof(void*));
  if (ret<0) return 0;

  ret=Cglobals_get(&tStagePortKey,(void **)&globalPort,sizeof(int));
  if (ret<0) return 0;

  ret=getDefaultForGlobal(globalHost,globalPort,globalSvc,globalVersion);
  if (ret<0) return 0;
  if (globalVersion) return (*globalVersion)-1;

///////////
/*
  if (((p = getenv(RFIO_USE_CASTOR_V2)) == NULL) &&
      ((p = getconfent("RFIO","USE_CASTOR_V2",0)) == NULL)) {*/
    /* Variable not set: compat mode */
 /*   return(0);
  }
  if ((strcasecmp(p,"YES") == 0) || (atoi(p) == 1)) {*/
    /* Variable set to yes or 1 : new mode */
  /*  return(1);
  }*/
  /* Variable set but not to 1 : compat mode */
  /*return(0);*/
}




