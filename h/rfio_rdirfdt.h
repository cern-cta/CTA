/*
 * $Id: rfio_rdirfdt.h,v 1.2 2000/11/20 14:58:03 jdurand Exp $
 */

#ifndef __rfio_rdirfdt_h
#define __rfio_rdirfdt_h

EXTERN_C int rfio_rdirfdt_allocentry (int);
#define FINDRDIR_WITH_SCAN     1
#define FINDRDIR_WITHOUT_SCAN  0
EXTERN_C int rfio_rdirfdt_findentry (int, int);
EXTERN_C int rfio_rdirfdt_findptr (RDIR *, int);
EXTERN_C int rfio_rdirfdt_freeentry (int);

#endif /* __rfio_rdirfdt_h */
