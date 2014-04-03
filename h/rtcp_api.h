/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */


/*
 */

/*
 * rtcp_api.h - rtcopy client API prototypes
 */

#pragma once

#include <osdep.h>
#include <Cuuid.h>
#include <Ctape_constants.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <net.h>

EXTERN_C int rtcp_NewFileList (
			       tape_list_t **,
			       file_list_t **,
			       int
			       );


