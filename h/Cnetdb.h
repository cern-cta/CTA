/*
 * $Id: Cnetdb.h,v 1.4 1999/11/21 14:22:39 obarring Exp $
 * $Log: Cnetdb.h,v $
 * Revision 1.4  1999/11/21 14:22:39  obarring
 * Include osdep.h to get EXTERN_C/DLL_DECL/_PROTO macros
 *
 * Revision 1.3  1999/07/30 15:59:39  obarring
 * Add copyright notice
 *
 * Revision 1.2  1999/07/30 15:58:06  obarring
 * Add __STDC__ compilation branch for __h_errno() declaration
 *
 * Revision 1.1  1999/07/30 15:02:29  obarring
 * First version
 *
 */

/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */


#ifndef _CNETDB_H
#define _CNETDB_H

#include <osdep.h>

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#if defined(hpux) || defined(HPUX10) || defined(sgi) || defined(SOLARIS)
EXTERN_C int DLL_DECL *__h_errno _PROTO((void));
#define h_errno (*__h_errno())
#endif /* hpux || HPUX10 || sgi || SOLARIS */
#endif /* _REENTRANT || _THREAD_SAFE */


EXTERN_C struct hostent DLL_DECL *Cgethostbyname _PROTO((const char *));
EXTERN_C struct hostent DLL_DECL *Cgethostbyaddr _PROTO((const void *, size_t, int));

#endif /* _CNETDB_H */
