/*
 * $Id: Cgrp.h,v 1.1 2000/10/27 13:55:55 jdurand Exp $
 */

/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * $RCSfile: Cgrp.h,v $ $Revision: 1.1 $ $Date: 2000/10/27 13:55:55 $ CERN IT-PDP/DM Jean-Damien Durand
 */


#ifndef _CGRP_H
#define _CGRP_H

#include <osdep.h>
#include <grp.h>
#include <sys/types.h>

EXTERN_C struct group DLL_DECL *Cgetgrnam _PROTO((CONST char *));
EXTERN_C struct group DLL_DECL *Cgetgrgid _PROTO((gid_t));

#endif /* _CGRP_H */

