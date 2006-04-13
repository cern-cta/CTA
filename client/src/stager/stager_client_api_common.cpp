/*
 * stager_client_api_common.cpp,v 1.20 2006/02/03 11:40:17 sponcec3 Exp
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)stager_client_api_common.cpp,v 1.20 2006/02/03 11:40:17 CERN IT-ADC/CA Benjamin COuturier";
#endif

/* ============== */
/* System headers */
/* ============== */
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>
#if !defined(_WIN32)
#include <unistd.h>
#else
#include "pwd.h"	// For getuid(), getgid()
#endif
#include <sys/types.h>
#include <sstream>

/* ============= */
/* Local headers */
/* ============= */
#include "stager_client_api_common.h"
#include "serrno.h"
#include "trace.h"
#include "Cglobals.h"
#include "Csnprintf.h"
#include "stager_api.h"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/client/BaseClient.hpp"
#include "castor/stager/DiskServerStatusCode.hpp"
#include "castor/stager/FileSystemStatusCodes.hpp"
#include "stager_client_api_authid.hpp"

EXTERN_C char DLL_DECL *getconfent _PROTO((char *, char *, int));

/* ================= */
/* Internal routines */
/* ================= */

/* Routines to free the structures */

int _free_prepareToGet_filereq (struct stage_prepareToGet_filereq  *ptr) { 
  if (ptr->protocol != NULL) free (ptr->protocol);
  if (ptr->filename != NULL) free (ptr->filename);
  return 0; 
}

int _free_prepareToGet_fileresp (struct stage_prepareToGet_fileresp  *ptr) { 
  if (ptr->filename != NULL) free (ptr->filename);
  if (ptr->errorMessage != NULL) free (ptr->errorMessage);
  return 0; 
}

int _free_io_fileresp (struct stage_io_fileresp  *ptr){ 
  if (ptr->castor_filename != NULL) free (ptr->castor_filename);
  if (ptr->protocol != NULL) free (ptr->protocol);
  if (ptr->server != NULL) free (ptr->server);
  if (ptr->filename != NULL) free (ptr->filename);
  if (ptr->errorMessage != NULL) free (ptr->errorMessage);
  return 0; 
}
int _free_prepareToPut_filereq (struct stage_prepareToPut_filereq  *ptr){ 
  if (ptr->protocol != NULL) free (ptr->protocol);
  if (ptr->filename != NULL) free (ptr->filename);
  return 0; 
}
int _free_prepareToPut_fileresp (struct stage_prepareToPut_fileresp  *ptr){ 
  if (ptr->filename != NULL) free (ptr->filename);
  if (ptr->errorMessage != NULL) free (ptr->errorMessage);
  return 0; 
}

int _free_prepareToUpdate_filereq (struct stage_prepareToUpdate_filereq  *ptr) { 
  if (ptr->protocol != NULL) free (ptr->protocol);
  if (ptr->filename != NULL) free (ptr->filename);
  return 0; 
}

int _free_prepareToUpdate_fileresp (struct stage_prepareToUpdate_fileresp  *ptr) { 
  if (ptr->filename != NULL) free (ptr->filename);
  if (ptr->errorMessage != NULL) free (ptr->errorMessage);
  return 0; 
}



int _free_filereq (struct stage_filereq  *ptr){ 
  if (ptr->filename != NULL) free (ptr->filename);
  return 0; 
}

int _free_fileresp (struct stage_fileresp  *ptr){ 
  if (ptr->filename != NULL) free (ptr->filename);
  if (ptr->errorMessage != NULL) free (ptr->errorMessage);
  return 0; 
}

int _free_query_req (struct stage_query_req  *ptr){ 
  if (ptr->param != NULL) free (ptr->param);
  return 0; 
}

int _free_filequery_resp (struct stage_filequery_resp  *ptr){ 
  if (ptr->filename != NULL) free (ptr->filename);
  if (ptr->poolname != NULL) free (ptr->poolname);
  return 0; 
}

int _free_requestquery_resp (struct stage_requestquery_resp  *ptr){ 
  if (ptr->requestId != NULL) free (ptr->requestId);
  return 0; 
}

int _free_subrequestquery_resp (struct stage_subrequestquery_resp  *ptr){ 
  return 0; 
}

int _free_findrequest_resp (struct stage_findrequest_resp  *ptr){ 
  if (ptr->requestId != NULL) free (ptr->requestId);
  return 0; 
}

/* ================= */
/* External routines */
/* ================= */

/* Utility Routines to allocate/delete the lists 
   of structures taken as input by the API functions */

ALLOC_STRUCT_LIST(prepareToGet_filereq)
ALLOC_STRUCT_LIST(prepareToGet_fileresp)
ALLOC_STRUCT_LIST(io_fileresp)
ALLOC_STRUCT_LIST(prepareToPut_filereq)
ALLOC_STRUCT_LIST(prepareToPut_fileresp)
ALLOC_STRUCT_LIST(prepareToUpdate_filereq)
ALLOC_STRUCT_LIST(prepareToUpdate_fileresp)
ALLOC_STRUCT_LIST(filereq)
ALLOC_STRUCT_LIST(fileresp)
ALLOC_STRUCT_LIST(query_req)
ALLOC_STRUCT_LIST(filequery_resp)
ALLOC_STRUCT_LIST(requestquery_resp)
ALLOC_STRUCT_LIST(subrequestquery_resp)
ALLOC_STRUCT_LIST(findrequest_resp)


FREE_STRUCT_LIST(prepareToGet_filereq)
FREE_STRUCT_LIST(prepareToGet_fileresp)
FREE_STRUCT_LIST(io_fileresp)
FREE_STRUCT_LIST(prepareToPut_filereq)
FREE_STRUCT_LIST(prepareToPut_fileresp)
FREE_STRUCT_LIST(prepareToUpdate_filereq)
FREE_STRUCT_LIST(prepareToUpdate_fileresp)
FREE_STRUCT_LIST(filereq)
FREE_STRUCT_LIST(fileresp)
FREE_STRUCT_LIST(query_req)
FREE_STRUCT_LIST(filequery_resp)
FREE_STRUCT_LIST(requestquery_resp)
FREE_STRUCT_LIST(subrequestquery_resp)
FREE_STRUCT_LIST(findrequest_resp)


#define STATUS_NA "NA"

EXTERN_C char *stage_statusName(int statusCode) {
  return (char *)castor::stager::SubRequestStatusCodesStrings[statusCode];
}


EXTERN_C char *stage_requestStatusName(int statusCode) {
  char *ret = STATUS_NA;
  if (statusCode >= 0 
      && statusCode < (sizeof(castor::stager::SubRequestStatusCodesStrings)/
		       sizeof(castor::stager::SubRequestStatusCodesStrings[0]))) {
    char *tmp = strchr((char *)(castor::stager::SubRequestStatusCodesStrings[statusCode]), '_');
    ret = tmp+1;
  }
  return ret;
}

#define NB_FILE_STATUS 8
static char* stage_fileStatusNameStr[NB_FILE_STATUS] = {
  "INVALID_STATUS",
  "STAGEOUT",
  "STAGEIN",
  "STAGED",
  "CANBEMIGR",
  "WAITINGMIGR",
  "BEINGMIGR",
  "PUT_FAILED"};

EXTERN_C char *stage_fileStatusName(int statusCode) {
  char *ret = STATUS_NA;
  if (statusCode >= 0 
      && statusCode < NB_FILE_STATUS) {
    ret = stage_fileStatusNameStr[statusCode];
  }
  return ret;
}

EXTERN_C char *stage_diskServerStatusName(int statusCode) {
  return (char*)castor::stager::DiskServerStatusCodeStrings[statusCode];
}

EXTERN_C char *stage_fileSystemStatusName(int statusCode) {
  return (char*)castor::stager::FileSystemStatusCodesStrings[statusCode];
}

EXTERN_C char* DLL_DECL stage_geturl(struct stage_io_fileresp *io) {
  
  char *func = "stage_geturl";
  
  if (io == NULL) {
    serrno = EINVAL;
    stage_errmsg(func, "io is NULL");
    return NULL;
  }
  
  std::stringstream sst;
  
  if (io->protocol != NULL) {
    sst << io->protocol << "://";
  }

  if (io->server != NULL) {
    sst << io->server;
    if (io->port > 0) {
      sst << ":" << std::dec << io->port;;
    }    
    sst << "/";
  }
  
  if (io->filename != NULL) {
    sst << io->filename;
  }
  
  return strdup(sst.str().c_str());
}


EXTERN_C int DLL_DECL stage_getClientTimeout() {
  char *p;
  int stager_timeout = STAGER_TIMEOUT_DEFAULT;
  if ((p = getenv ("STAGER_TIMEOUT")) != NULL ||
      (p = getconfent("STAGER", "TIMEOUT",0)) != NULL) {
    char* dp = p;
    int itimeout = strtol(p, &dp, 0);
    if (*dp == 0) {
      stager_timeout = itimeout;
    }
  }
  return stager_timeout;
}



static int stager_client_api_key = -1;

EXTERN_C int DLL_DECL
stage_apiInit(struct stager_client_api_thread_info **thip) {
  Cglobals_get (&stager_client_api_key,
		(void **) thip, 
		sizeof(struct stager_client_api_thread_info));
  if (*thip == NULL) {
    serrno = ENOMEM;
    return (-1);
  }
  if(! (*thip)->initialized) {
    init_trace_r(&((*thip)->trace), STAGER_TRACE_NAME); 
    (*thip)->initialized = 1;
    (*thip)->authorization_id_specified = 0;
  }
  return (0);
}



#define STBUFSIZE 200
EXTERN_C void DLL_DECL stage_trace(int level, char *format, ...) {
  va_list args;           /* arguments */
  struct stager_client_api_thread_info *thip;
  char *label = "stager";
  char buffer[STBUFSIZE+1];

  va_start(args, format);
  if(stage_apiInit(&thip)) {
    va_end(args);
    return;
  }
  buffer[STBUFSIZE] = '\0';
  Cvsnprintf(buffer, STBUFSIZE, format, args);
  va_end(args);
  print_trace_r(thip->trace, level, label, "%s", buffer);

}


EXTERN_C int DLL_DECL stage_setid(uid_t uid, gid_t gid) {

  struct stager_client_api_thread_info *thip;
  if(stage_apiInit(&thip)) {
    return -1;
  }

  thip->uid = uid;
  thip->gid = gid;
  thip->authorization_id_specified = 1;
  return 0;
}


EXTERN_C int DLL_DECL stage_getid(uid_t *uid, gid_t *gid) {

  struct stager_client_api_thread_info *thip;
  if(stage_apiInit(&thip)) {
    return -1;
  }

  if(thip->authorization_id_specified == 1) {
    if (uid != NULL) *uid = thip->uid;
    if (gid != NULL) *gid = thip->gid;
  } else {
    if (uid != NULL) *uid = getuid();
    if (gid != NULL) *gid = getgid();
  }
  return 0;
}


EXTERN_C int DLL_DECL stage_resetid() {

  struct stager_client_api_thread_info *thip;
  if(stage_apiInit(&thip)) {
    return -1;
  }
  thip->authorization_id_specified = 0;
  return 0;
}


void DLL_DECL castor::client::setClientAuthorizationId
(castor::client::BaseClient &client)
  throw(castor::exception::Exception) {

  uid_t authUid;
  gid_t authGid;

  if (stage_getid(&authUid, &authGid) < 0) {
    castor::exception::Exception e(serrno);
    e.getMessage()
      << "Error in stage_getid" << std::endl;
    throw e;
  }
  client.setAuthorizationId(authUid, authGid);
}
