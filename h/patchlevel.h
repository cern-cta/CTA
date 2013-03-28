/*
 * $Id: patchlevel.h,v 1.8 2007/12/07 17:19:31 itglp Exp $
 */

/*
 */

/*
 * Copyright (C) 1990-2000 by CERN IT-PDP/CS
 * All rights reserved
 */

/* patchlevel.h                 Patch level                             */

#ifndef _PATCHLEVEL_H_INCLUDED_
#define _PATCHLEVEL_H_INCLUDED_

#define MAJORVERSION    __MAJORVERSION__
#define MINORVERSION    __MINORVERSION__
#define MAJORRELEASE    __MAJORRELEASE__
#define MINORRELEASE    __MINORRELEASE__
#define BASEVERSION     __BASEVERSION__

/* For untagged builds (e.g. from CVS) we redefine the replacement macros */
/* warning: there must be ONLY ONE space between define and the macro here to work! */
#define __MAJORVERSION__ 0
#define __MINORVERSION__ 0
#define __MAJORRELEASE__ 0
#define __MINORRELEASE__ 0

#define PATCHLEVEL      MINORRELEASE

#endif /* _PATCHLEVEL_H_INCLUDED_ */
