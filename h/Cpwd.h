/*
 * $Id: Cpwd.h,v 1.3 2000/03/14 09:49:07 jdurand Exp $
 */

/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * $RCSfile: Cpwd.h,v $ $Revision: 1.3 $ $Date: 2000/03/14 09:49:07 $ CERN IT-PDP/DM Olof Barring
 */


#ifndef _CPWD_H
#define _CPWD_H

#include <osdep.h>
#include <pwd.h>

EXTERN_C struct passwd DLL_DECL *Cgetpwnam _PROTO((CONST char *));
EXTERN_C struct passwd DLL_DECL *Cgetpwuid _PROTO((uid_t));

#endif /* _CPWD_H */

