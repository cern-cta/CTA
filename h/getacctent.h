/*
 * Copyright (C) 1995-2000 by CERN CN-PDP/CS
 * All rights reserved
 */

/*
 * @(#)$RCSfile: getacctent.h,v $ $Revision: 1.1 $ $Date: 2000/05/31 10:14:23 $ CERN CN-PDP/CS Antony Simmins
 */


#ifndef GETACCTENT_H
#define GETACCTENT_H
#include <osdep.h>

#ifndef ACCT_FILE
#if defined(_WIN32)
#define ACCT_FILE "%SystemRoot%\\system32\\drivers\\etc\\account"
#else
#define ACCT_FILE	"/etc/account"
#endif
#endif
#define ACCT_FILE_VAR	"ACCT_FILE"

#define DFLT_SEQ_NUM	0

#define NEWLINE	'\n'
#define ENDSTR	'\0'

#define PLUS_STR	"+"
#define COLON_STR	":"

EXTERN_C char DLL_DECL *getacctent _PROTO((struct passwd *, char *, char *, int));

#endif /* GETACCTENT_H */
