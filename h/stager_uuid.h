/*
 * $Id: stager_uuid.h,v 1.1 2004/11/08 10:46:12 jdurand Exp $
 */

#ifndef __stager_uuid_h
#define __stager_uuid_h

#include "osdep.h"
#include "Cuuid.h"

EXTERN_C Cuuid_t DLL_DECL *C__stager_request_uuid _PROTO(());
#define stager_request_uuid (*C__stager_request_uuid())

EXTERN_C Cuuid_t DLL_DECL *C__stager_subrequest_uuid _PROTO(());
#define stager_subrequest_uuid (*C__stager_subrequest_uuid())

#endif /* __stager_uuid_h */
