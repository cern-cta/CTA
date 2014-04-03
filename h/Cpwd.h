/*
 * $Id: Cpwd.h,v 1.4 2000/03/14 14:40:27 baud Exp $
 */

/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 */


#pragma once

#include <osdep.h>
#include <pwd.h>

EXTERN_C struct passwd *Cgetpwnam (const char *);
EXTERN_C struct passwd *Cgetpwuid (uid_t);


