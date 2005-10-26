/*
 * $Id: Cdlopen_api.h,v 1.2 2005/10/26 08:53:50 jdurand Exp $
 */

/*
 * $RCSfile: Cdlopen_api.h,v $ $Revision: 1.2 $ $Date: 2005/10/26 08:53:50 $ CERN IT-ADC/CA Jean-Damien Durand
 */


#ifndef __Cdlopen_api_h
#define __Cdlopen_api_h

#include "osdep.h"

#ifdef _MSC_VER
/* On window RT_* are not defined */
#define RTLD_LAZY       0x00001 /* Lazy function call binding.  */
#define RTLD_NOW        0x00002 /* Immediate function call binding.  */
#define RTLD_BINDING_MASK   0x3 /* Mask of binding time value.  */
#define RTLD_NOLOAD     0x00004 /* Do not load the object.  */
#define RTLD_DEEPBIND   0x00008 /* Use deep binding.  */
#define RTLD_GLOBAL     0x00100
#define RTLD_LOCAL      0
#define RTLD_NODELETE   0x01000
#else
#include <dlfcn.h>
#endif

EXTERN_C void DLL_DECL *Cdlopen _PROTO((CONST char *, int));
EXTERN_C void DLL_DECL *Cdlsym _PROTO((void *, CONST char *));
EXTERN_C int  DLL_DECL  Cdlclose _PROTO((void *));
EXTERN_C char DLL_DECL *Cdlerror _PROTO((void));

#endif /* __Cdlopen_api_h */
