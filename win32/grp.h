/*
 * $Id: grp.h,v 1.4 2001/05/21 11:00:27 baud Exp $
 */

/*
 * @(#)$RCSfile: grp.h,v $ $Revision: 1.4 $ $Date: 2001/05/21 11:00:27 $ CERN IT-PDP/DC Frederic Hemmer
 */

/*
 * Copyright (C) 1993-2001 by CERN/IT/PDP/DC
 * All rights reserved
 */


#ifndef __win32_grp_h
#define __win32_grp_h

#include <osdep.h>

struct  group {
        char    *gr_name;
        char    *gr_passwd;
        int     gr_gid;
        char    **gr_mem;
};

EXTERN_C struct group  DLL_DECL *fillgrpent _PROTO((char *));
EXTERN_C struct group  DLL_DECL *getgrgid _PROTO((gid_t));
EXTERN_C struct group  DLL_DECL *getgrnam _PROTO((char *));

#endif /* __win32_grp_h */
