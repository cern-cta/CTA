/*
 * $Id: stage_api.h,v 1.1 2000/01/26 08:39:16 jdurand Exp $
 */

#ifndef __stage_api_h
#define __stage_api_h

#include <osdep.h>

EXTERN_C int DLL_DECL send2stgd _PROTO((char *, char *, int, int, char *, int));
EXTERN_C int DLL_DECL stage_seterrbuf _PROTO((char *, int));
EXTERN_C int DLL_DECL stage_setoutbuf _PROTO((char *, int));
EXTERN_C int DLL_DECL stage_geterrbuf _PROTO((char **, int *));
EXTERN_C int DLL_DECL stage_getoutbuf _PROTO((char **, int *));
EXTERN_C int DLL_DECL stage_errmsg _PROTO(());
EXTERN_C int DLL_DECL stage_outmsg _PROTO(());
EXTERN_C int DLL_DECL stage_updc_filcp _PROTO((char *, int, char *, u_signed64, int, int, int, char *, char *, int, int, char *, char *, int));
EXTERN_C int DLL_DECL stage_updc_tppos _PROTO((char *, int, int, char *, char *, int, int, char *, char *, int));

#endif /* __stage_api_h */

