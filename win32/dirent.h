/*
 * @(#)$RCSfile: dirent.h,v $ $Revision: 1.1 $ $Date: 1999/10/14 09:48:44 $ CERN IT-PDP/DM Jean-Philippe Baud
 */

/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef _DIRENT_H
#define _DIRENT_H
#include <sys/types.h>
#include "Castor_limits.h"
struct dirent {
	ino_t d_ino;
	off_t d_reclen;
	unsigned short d_namlen;
	char d_name[CA_MAXNAMELEN+1];
};
#endif
