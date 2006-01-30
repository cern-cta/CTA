/*
 * $Id: stager_client_api_common.h,v 1.2 2006/01/30 20:57:16 bcouturi Exp $
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


EXTERN_C int DLL_DECL
stage_apiInit(struct stager_client_api_thread_info **thip);

EXTERN_C void DLL_DECL
stage_trace(int level, char *format, ...);


#endif /* stager_client_api_common_h */
