/*
 * $Id: grp.h,v 1.2 1999/12/09 13:47:51 jdurand Exp $
 */

/*
 * @(#)grp.h	1.2 02/16/99 CERN CN-SW/DC Frederic Hemmer
 */

/*
 * Copyright (C) 1993-1999 by CERN/CN/SW/DC
 * All rights reserved
 */


struct  group {
        char    *gr_name;
        char    *gr_passwd;
        int     gr_gid;
        char    **gr_mem;
};

struct group *getgrgid();
struct group *getgrnam();
