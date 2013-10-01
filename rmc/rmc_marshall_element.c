/*
 * Copyright (C) 2001-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#include "h/marshall.h"
#include "h/rmc_marshall_element.h"
 
int rmc_marshall_element (
	char **const sbpp,
	const struct smc_element_info *const element_info)
{
	char *sbp = *sbpp;

	marshall_WORD (sbp, element_info->element_address);
	marshall_BYTE (sbp, element_info->element_type);
	marshall_BYTE (sbp, element_info->state);
	marshall_BYTE (sbp, element_info->asc);
	marshall_BYTE (sbp, element_info->ascq);
	marshall_BYTE (sbp, element_info->flags);
	marshall_WORD (sbp, element_info->source_address);
	marshall_STRING (sbp, element_info->name);
	*sbpp = sbp;
	return (0);
}
