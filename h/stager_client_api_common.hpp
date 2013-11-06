/*
 * $Id: stager_client_api_common.hpp,v 1.6 2009/01/14 17:38:07 sponcec3 Exp $
 *
 * Header file for internal stager client functions.
 * Note that this is only included from C++ code and it *is* C++
 * only because of the bool field in stager_client_api_thread_info.
 */

#ifndef STAGER_CLIENT_API_COMMON_HPP
#define STAGER_CLIENT_API_COMMON_HPP

#include <sys/types.h>
#include "osdep.h"
#include "stager_errmsg.h"
#include "castor/exception/Exception.hpp"

#define DEFAULT_PORT 9002
#define DEFAULT_SEC_PORT 9007
#define DEFAULT_SVCCLASS ""  

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

EXTERN_C int
stage_apiInit(struct stager_client_api_thread_info **thip);

EXTERN_C void
stage_trace(int level, const char *format, ...);

EXTERN_C int
stage_setid(uid_t uid, gid_t gid);

EXTERN_C int
stage_getid(uid_t *uid, gid_t *gid);

EXTERN_C int
stage_resetid();

int setDefaultOption(struct stage_options* opts)
  throw(castor::exception::Exception);

#endif /* STAGER_CLIENT_API_COMMON_HPP */
