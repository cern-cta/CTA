/*
 * Copyright (C) 1995-2000 by CERN CN-PDP/CS
 * All rights reserved
 */

/*
 * @(#)$RCSfile: ypgetacctent.h,v $ $Revision: 1.2 $ $Date: 2001/09/21 04:29:14 $ CERN CN-PDP/CS Antony Simmins
 */


#ifndef YPGETACCTENT_H
#define YPGETACCTENT_H
#include <osdep.h>

#define ACCT_MAP_NAME	"account"

#define DFLT_SEQ_STR	"0"
#define MAX_SEQ_NUM	8

#define NAME_LEN	10
#define ACCT_LEN	9

#ifdef FALSE
#undef FALSE
#endif
#define FALSE		0
#ifdef TRUE
#undef TRUE
#endif
#define TRUE		1

#define NEWLINE_CHR	'\n'
#define ENDSTR_CHR	'\0'
#define STAR_CHR	'*'

#define STAR_STR	"*"
#define COLON_STR	":"

EXTERN_C char DLL_DECL *ypgetacctent _PROTO((struct passwd *, char *, char *, int));

#endif /* YPGETACCTENT_H */
