
/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * $RCSfile: Cpwd.h,v $ $Revision: 1.1 $ $Date: 1999/11/22 17:54:36 $ CERN IT-PDP/DM Olof Barring
 */


#ifndef _CPWD_H
#define _CPWD_H

#include <osdep.h>

EXTERN_C struct passwd DLL_DECL *Cgetpwnam _PROTO((const char *));
EXTERN_C struct passwd DLL_DECL *Cgetpwuid _PROTO((uid_t));

#endif /* _CPWD_H */

