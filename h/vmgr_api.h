/*
 * $Id: vmgr_api.h,v 1.1 1999/12/15 10:04:44 baud Exp $
 */

/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: vmgr_api.h,v $ $Revision: 1.1 $ $Date: 1999/12/15 10:04:44 $ CERN IT-PDP/DM Jean-Philippe Baud
 */

#ifndef _VMGR_API_H
#define _VMGR_API_H
#include "vmgr_constants.h"
#include "osdep.h"

int *__vmgr_errno();
#define vmgr_errno (*__vmgr_errno())

struct vmgr_api_thread_info {
	char *		errbufp;
	int		errbuflen;
	int		initialized;
	int		vm_errno;
};

			/* function prototypes */

#if defined(__STDC__)
extern int vmgr_deletetape(const char *);
extern int vmgr_entertape(const char *);
extern int vmgr_gettape(const char *, uid_t, gid_t, int, const char *, int *, char *, char *, char *, char *, int *, unsigned int *);
extern int vmgr_modifytape(const char *);
extern int vmgr_querytape(const char *, char *, char *, char *, char *);
extern int vmgr_updatetape(int, u_signed64, int, int, int);
#else
extern int vmgr_deletetape();
extern int vmgr_entertape();
extern int vmgr_gettape();
extern int vmgr_modifytape();
extern int vmgr_querytape();
extern int vmgr_updatetape();
#endif
#endif
