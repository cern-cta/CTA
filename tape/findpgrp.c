/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	findpgrp - finds the process group */

#include <unistd.h>
#include <sys/types.h>
#include "Ctape_api.h"

int findpgrp()
{
	return (getpgrp());
}
