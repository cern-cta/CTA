/*
 * $Id: Cstage_ifce.h,v 1.2 1999/12/02 14:38:42 jdurand Exp $
 */

#ifndef __Cstage_ifce_h
#define __Cstage_ifce_h

#include "osdep.h"

EXTERN_C int DLL_DECL stcp2Cdb _PROTO((struct stgcat_entry *,
                                       struct stgcat_tape *,
                                       struct stgcat_disk *,
                                       struct stgcat_hsm *,
                                       struct stgcat_alloc *));
EXTERN_C int DLL_DECL stpp2Cdb _PROTO((struct stgpath_entry *,
                                       struct stgcat_link *));
EXTERN_C int DLL_DECL Cdb2stcp _PROTO((struct stgcat_entry *,
                                       struct stgcat_tape *,
                                       struct stgcat_disk *,
                                       struct stgcat_hsm *,
                                       struct stgcat_alloc *));
EXTERN_C int DLL_DECL Cdb2stpp _PROTO((struct stgpath_entry *,
                                       struct stgcat_link *));

#endif /* __Cstage_ifce_h */
