/*
 * $Id: Cinit.h,v 1.3 2001/06/21 11:41:19 jdurand Exp $
 */

/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: Cinit.h,v $ $Revision: 1.3 $ $Date: 2001/06/21 11:41:19 $ CERN IT-PDP/DM Jean-Philippe Baud
 */

#ifndef _CINIT_H
#define _CINIT_H

#include "osdep.h"

	/* structure to be used with Cinitdaemon()/Cinitservice() */

struct main_args {
	int	argc;
	char	**argv;
};

EXTERN_C int Cinitdaemon (char *, void (*) (int));

#endif
