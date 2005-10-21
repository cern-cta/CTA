/*
 * $Id: Cdlopen_api.h,v 1.1 2005/10/21 13:54:27 jdurand Exp $
 */

/*
 * $RCSfile: Cdlopen_api.h,v $ $Revision: 1.1 $ $Date: 2005/10/21 13:54:27 $ CERN IT-ADC/CA Jean-Damien Durand
 */


#ifndef __Cdlopen_api_h
#define __Cdlopen_api_h

#include "osdep.h"

EXTERN_C void DLL_DECL *Cdlopen _PROTO((CONST char *, int));
EXTERN_C void DLL_DECL *Cdlsym _PROTO((void *, CONST char *));
EXTERN_C int  DLL_DECL  Cdlclose _PROTO((void *));
EXTERN_C char DLL_DECL *Cdlerror _PROTO((void));

#endif /* __Cdlopen_api_h */
