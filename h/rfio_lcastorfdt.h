/*
 * $Id: rfio_lcastorfdt.h,v 1.1 2005/04/11 15:35:11 jdurand Exp $
 */

#ifndef __rfio_lcastorfdt_h
#define __rfio_lcastorfdt_h

EXTERN_C int DLL_DECL rfio_lcastorfdt_allocentry _PROTO((int));
#define FINDLCASTOR_WITH_SCAN     1
#define FINDLCASTOR_WITHOUT_SCAN  0
EXTERN_C int DLL_DECL rfio_lcastorfdt_findentry _PROTO((int, int *, int));
EXTERN_C int DLL_DECL rfio_lcastorfdt_freeentry _PROTO((int));
EXTERN_C int DLL_DECL rfio_lcastorfdt_fillentry _PROTO((int, int, int));

#endif /* __rfio_lcastorfdt_h */
