/*
 * $RCSfile: Csched_api.h,v $ $Revision: 1.1 $ $Date: 2000/06/19 09:48:34 $ CERN IT-PDP/DM Jean-Damien Durand
 */

/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */


#ifndef __Csched_api_h
#define __Csched_api_h

#include <Csched_flags.h>
#include <Cglobals.h>
#include <osdep.h>

#define  Csched_getschedparam(a,b,c)  Csched_Getschedparam(__FILE__,__LINE__,a,b,c)
#define  Csched_setschedparam(a,b,c)  Csched_Setschedparam(__FILE__,__LINE__,a,b,c)
#define  Csched_get_priority_min(a)   Csched_Get_priority_min(__FILE__,__LINE__,a)
#define  Csched_get_priority_mid(a)   Csched_Get_priority_mid(__FILE__,__LINE__,a)
#define  Csched_get_priority_max(a)   Csched_Get_priority_max(__FILE__,__LINE__,a)

EXTERN_C int   DLL_DECL  Csched_Getschedparam _PROTO((char *, int, int, int *, Csched_param_t *));
EXTERN_C int   DLL_DECL  Csched_Setschedparam _PROTO((char *, int, int, int, Csched_param_t *));
EXTERN_C int   DLL_DECL  Csched_Get_priority_min _PROTO((char *, int, int));
EXTERN_C int   DLL_DECL  Csched_Get_priority_mid _PROTO((char *, int, int));
EXTERN_C int   DLL_DECL  Csched_Get_priority_max _PROTO((char *, int, int));

#endif /* __Csched_api_h */




