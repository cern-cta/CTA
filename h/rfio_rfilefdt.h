/*
 * $Id: rfio_rfilefdt.h,v 1.1 2000/10/02 08:02:22 jdurand Exp $
 */

#ifndef __rfio_rfilefdt_h
#define __rfio_rfilefdt_h

EXTERN_C int DLL_DECL rfio_rfilefdt_allocentry _PROTO((int));
#define FINDRFILE_WITH_SCAN     1
#define FINDRFILE_WITHOUT_SCAN  0
EXTERN_C int DLL_DECL rfio_rfilefdt_findentry _PROTO((int, int));
EXTERN_C int DLL_DECL rfio_rfilefdt_freeentry _PROTO((int));

#endif /* __rfio_rfilefdt_h */
