/*
 * $Id: stager_catalogInterface.h,v 1.1 2004/11/04 15:02:49 jdurand Exp $
 */

#ifndef __stager_catalogInterface_h
#define __stager_catalogInterface_h

#include "osdep.h"
#include "castor/stager/Tape.h"
#include "castor/stager/Segment.h"
#include "castor/stager/Stream.h"
#include "castor/stager/TapeCopy.h"
#include "castor/stager/TapeCopyForMigration.h"
#include "castor/stager/DiskCopy.h"
#include "castor/stager/DiskCopyStatusCodes.h"
#include "castor/stager/CastorFile.h"
#include "castor/stager/TapePool.h"
#include "castor/stager/TapeStatusCodes.h"
#include "castor/stager/SegmentStatusCodes.h"
#include "castor/stager/IStagerSvc.h"
#include "castor/Services.h"
#include "castor/BaseAddress.h"
#include "castor/db/DbAddress.h"
#include "castor/IAddress.h"
#include "castor/IObject.h"
#include "castor/IClient.h"
#include "castor/Constants.h"

EXTERN_C int DLL_DECL stager_getDbSvc _PROTO((struct C_Services_t ***));
EXTERN_C int DLL_DECL stager_getStgAndDbSvc _PROTO((struct Cstager_IStagerSvc_t ***, struct C_Services_t ***));

#endif /* __stager_catalogInterface_h */
