/*
 * $Id: Cinit.h,v 1.3 2001/06/21 11:41:19 jdurand Exp $
 */

/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 */

#pragma once

#include "h/osdep.h"

	/* structure to be used with Cinitdaemon()/Cinitservice() */

struct main_args {
	int	argc;
	char	**argv;
};

EXTERN_C int Cinitdaemon (const char *const name, void (*const wait4child) (int));

