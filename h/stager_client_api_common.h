/*
 * $Id: stager_client_api_common.h,v 1.1 2005/02/14 17:20:44 bcouturi Exp $
 */

#ifndef stager_client_api_common_h
#define stager_client_api_common_h

#include <sys/types.h>
#include "osdep.h"

#define STAGER_TRACE_NAME  "STAGER_TRACE"

struct stager_client_api_thread_info {
  bool initialized;
  void *trace;
};


EXTERN_C int DLL_DECL
stage_apiInit(struct stager_client_api_thread_info **thip);

EXTERN_C void DLL_DECL
stage_trace(int level, char *format, ...);


#endif /* stager_client_api_common_h */
