/*
 * $Id: Cstage_ifce.h,v 1.1 1999/11/29 15:27:44 jdurand Exp $
 */

#ifndef __Cstage_ifce_h
#define __Cstage_ifce_h

#include "osdep.h"

EXTERN_C int DLL_DECL stcp2Cdb _PROTO((struct stgcat_entry *,
                                       struct Cstgcat_common *,
                                       struct Cstgcat_tape *,
                                       struct Cstgcat_disk *,
                                       struct Cstgcat_hsm *));
EXTERN_C int DLL_DECL stpp2Cdb _PROTO((struct stgpath_entry *,
                                       struct Cstgcat_link *));

#endif /* __Cstage_ifce_h */
