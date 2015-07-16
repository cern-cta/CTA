/*
 * $Id: rfio_lcastorfdt.h,v 1.1 2005/04/11 15:35:11 jdurand Exp $
 */

#pragma once

EXTERN_C int rfio_lcastorfdt_allocentry (int);
#define FINDLCASTOR_WITH_SCAN     1
#define FINDLCASTOR_WITHOUT_SCAN  0
EXTERN_C int rfio_lcastorfdt_findentry (int, int *, int);
EXTERN_C int rfio_lcastorfdt_freeentry (int);
EXTERN_C int rfio_lcastorfdt_fillentry (int, int, int);

