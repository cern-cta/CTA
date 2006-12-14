/*
 * $Id: stager_client_api_common.hpp,v 1.1 2006/12/14 14:01:14 itglp Exp $
 *
 * Header file for internal stager client functions.
 * Note that this is only included from C++ code and it *is* C++
 * only because of the bool field in stager_client_api_thread_info.
 */

#ifndef STAGER_CLIENT_API_COMMON_HPP
#define STAGER_CLIENT_API_COMMON_HPP

#include <sys/types.h>
#include "osdep.h"

#define DEFAULT_HOST "stagepublic"
#define DEFAULT_PORT1 5007
#define DEFAULT_PORT2 9002
#define DEFAULT_SVCCLASS ""  
#define DEFAULT_VERSION 1

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

#endif /* STAGER_CLIENT_API_COMMON_HPP */
