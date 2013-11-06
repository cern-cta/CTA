/*
 * Copyright (C) 1995-2000 by CERN CN-PDP/CS
 * All rights reserved
 */

/*
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

EXTERN_C char *ypgetacctent (struct passwd *, char *, char *, int);

#endif /* YPGETACCTENT_H */
