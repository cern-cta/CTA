/*
 * Copyright (C) 1995-2000 by CERN CN-PDP/CS
 * All rights reserved
 */

/*
 */


#ifndef GETACCT_H
#define GETACCT_H

#define ACCOUNT_VAR	"ACCOUNT"

#define EMPTY_STR	""
#define COLON_STR	":"

#include <osdep.h>

EXTERN_C char *getacct_r (char *);
EXTERN_C char *getacct (void);

#endif /* GETACCT_H */
