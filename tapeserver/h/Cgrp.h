/*
 * $Id: Cgrp.h,v 1.1 2000/10/27 13:55:55 jdurand Exp $
 */

/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 */


#pragma once

#include <osdep.h>
#include <grp.h>
#include <sys/types.h>

EXTERN_C struct group *Cgetgrnam (const char *);
EXTERN_C struct group *Cgetgrgid (gid_t);


