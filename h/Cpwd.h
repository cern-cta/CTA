/*
 * $Id: Cpwd.h,v 1.4 2000/03/14 14:40:27 baud Exp $
 */

/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * $RCSfile: Cpwd.h,v $ $Revision: 1.4 $ $Date: 2000/03/14 14:40:27 $ CERN IT-PDP/DM Olof Barring
 */


#ifndef _CPWD_H
#define _CPWD_H

#include <osdep.h>
#include <pwd.h>

EXTERN_C struct passwd DLL_DECL *Cgetpwnam _PROTO((CONST char *));
EXTERN_C struct passwd DLL_DECL *Cgetpwuid _PROTO((uid_t));

#endif /* _CPWD_H */

