/*
 * $Id: stager_client_api_common.h,v 1.4 2006/04/20 10:04:09 gtaur Exp $
 */

#ifndef stager_client_api_common_h
#define stager_client_api_common_h

#include <sys/types.h>
#include "osdep.h"

#define STAGER_TRACE_NAME  "STAGER_TRACE"

struct stager_client_api_thread_info {
  bool initialized;
  void *trace;
  int authorization_id_specified;
  uid_t uid;
  gid_t gid;
};

// Forward declaration of a C++ object
struct Cclient_BaseClient_t;

EXTERN_C int DLL_DECL
stage_apiInit(struct stager_client_api_thread_info **thip);

EXTERN_C void DLL_DECL
stage_trace(int level, char *format, ...);

EXTERN_C int DLL_DECL
stage_setid(uid_t uid, gid_t gid);

EXTERN_C int DLL_DECL
stage_getid(uid_t *uid, gid_t *gid);

EXTERN_C int DLL_DECL
stage_resetid();

EXTERN_C int DLL_DECL 
setDefaultOption(struct stage_options* opts);

#endif /* stager_client_api_common_h */
