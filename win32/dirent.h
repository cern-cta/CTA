/*
 * $Id: dirent.h,v 1.3 2001/05/21 10:59:20 baud Exp $
 */

/*
 * @(#)$RCSfile: dirent.h,v $ $Revision: 1.3 $ $Date: 2001/05/21 10:59:20 $ CERN IT-PDP/DM Jean-Philippe Baud
 */

/*
 * Copyright (C) 1999-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef _DIRENT_WIN32_H
#define _DIRENT_WIN32_H
#include <sys/types.h>
#include "Castor_limits.h"
struct dirent {
	ino_t d_ino;
	off_t d_reclen;
	unsigned short d_namlen;
	char d_name[CA_MAXNAMELEN+1];
};
#endif
