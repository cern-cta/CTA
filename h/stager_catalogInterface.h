/*
 * $Id: stager_catalogInterface.h,v 1.2 2004/11/06 08:41:10 jdurand Exp $
 */

#ifndef __stager_catalogInterface_h
#define __stager_catalogInterface_h

#include "osdep.h"
#include "castor/stager/CastorFile.h"
#include "castor/stager/IStagerSvc.h"
#include "castor/stager/Request.h"
#include "castor/stager/SubRequest.h"
#include "castor/stager/SubRequestStatusCodes.h"
#include "castor/Services.h"
#include "castor/Constants.h"

EXTERN_C int DLL_DECL stager_getDbSvc _PROTO((struct C_Services_t ***));
EXTERN_C int DLL_DECL stager_getStgAndDbSvc _PROTO((struct Cstager_IStagerSvc_t ***, struct C_Services_t ***));

#endif /* __stager_catalogInterface_h */
