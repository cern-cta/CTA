/*
 * $Id: grp.h,v 1.3 2000/11/03 13:34:11 baud Exp $
 */

/*
 * @(#)$RCSfile: grp.h,v $ $Revision: 1.3 $ $Date: 2000/11/03 13:34:11 $ CERN IT-PDP/DC Frederic Hemmer
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DC
 * All rights reserved
 */


#ifndef __win32_grp_h
#define __win32_grp_h

struct  group {
        char    *gr_name;
        char    *gr_passwd;
        int     gr_gid;
        char    **gr_mem;
};

struct group *getgrgid();
struct group *getgrnam();

#endif /* __win32_grp_h */
