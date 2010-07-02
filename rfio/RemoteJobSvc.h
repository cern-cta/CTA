/*
 * $Id: RemoteJobSvc.h,v 1.1 2008/07/28 16:23:57 waldron Exp $
 */

#ifndef _RemoteJobSvc_h
#define _RemoteJobSvc_h

#include "castor/stager/IJobSvc.h"
#include "castor/Services.h"
#include "castor/Constants.h"
#include "castor/BaseAddress.h"
#include "castor/IObject.h"
#include "castor/IAddress.h"

EXTERN_C int stager_getRemJobAndDbSvc _PROTO((struct Cstager_IJobSvc_t ***, struct C_Services_t ***));

#endif /* _RemoteJobSvc_h */
