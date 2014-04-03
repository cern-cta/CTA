/*
 * $Id: stager_uuid.h,v 1.1 2004/11/08 10:46:12 jdurand Exp $
 */

#pragma once

#include "osdep.h"
#include "Cuuid.h"

EXTERN_C Cuuid_t *C__stager_request_uuid ();
#define stager_request_uuid (*C__stager_request_uuid())

EXTERN_C Cuuid_t *C__stager_subrequest_uuid ();
#define stager_subrequest_uuid (*C__stager_subrequest_uuid())

