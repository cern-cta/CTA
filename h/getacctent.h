/*
 * Copyright (C) 1995-2001 by CERN CN-PDP/CS
 * All rights reserved
 */

/*
 * @(#)$RCSfile: getacctent.h,v $ $Revision: 1.2 $ $Date: 2001/05/04 05:18:16 $ CERN CN-PDP/CS Antony Simmins
 */


#ifndef GETACCTENT_H
#define GETACCTENT_H
#include <osdep.h>

#ifndef ACCT_FILE
#if ! defined(_WIN32)
#define ACCT_FILE	"/etc/account"
#endif
#define ACCT_FILE_VAR	"ACCT_FILE"

#define DFLT_SEQ_NUM	0

#define NEWLINE	'\n'
#define ENDSTR	'\0'

#define PLUS_STR	"+"
#define COLON_STR	":"

EXTERN_C char DLL_DECL *getacctent _PROTO((struct passwd *, char *, char *, int));

#endif /* GETACCTENT_H */
