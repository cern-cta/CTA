/*
 * $Id: stager_catalogInterface.h,v 1.5 2004/11/17 18:50:41 jdurand Exp $
 */

#ifndef __stager_catalogInterface_h
#define __stager_catalogInterface_h

#include "osdep.h"
#include "castor/stager/CastorFile.h"
#include "castor/stager/IStagerSvc.h"
#include "castor/stager/Request.h"
#include "castor/stager/SubRequest.h"
#include "castor/stager/SubRequestStatusCodes.h"
#include "castor/stager/FileRequest.h"
#include "castor/stager/SvcClass.h"
#include "castor/Services.h"
#include "castor/Constants.h"
#include "castor/BaseAddress.h"

EXTERN_C int DLL_DECL stager_getDbSvc _PROTO((struct C_Services_t ***));
EXTERN_C int DLL_DECL stager_getStgAndDbSvc _PROTO((struct Cstager_IStagerSvc_t ***, struct C_Services_t ***));

#endif /* __stager_catalogInterface_h */
