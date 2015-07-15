/*
 * Copyright (C) 1995-2001 by CERN CN-PDP/CS
 * All rights reserved
 */

/*
 */


#pragma once
#include <osdep.h>

#ifndef ACCT_FILE
#define ACCT_FILE	"/etc/account"
#endif
#define ACCT_FILE_VAR	"ACCT_FILE"

#define DFLT_SEQ_NUM	0

#define NEWLINE	'\n'
#define ENDSTR	'\0'

#define PLUS_STR	"+"
#define COLON_STR	":"

EXTERN_C char *getacctent (struct passwd *, char *, char *, int);

