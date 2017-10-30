/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 */


#pragma once

#include <osdep.h>
#include <netdb.h>

EXTERN_C struct hostent *Cgethostbyname (const char *);
EXTERN_C struct hostent *Cgethostbyaddr (const void *, size_t, int);
EXTERN_C struct servent *Cgetservbyname (const char *, const char *);

