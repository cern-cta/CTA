/*
 * Copyright (C) 1999-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef _VMGR_TAPE_DENMAP_TRUNK_R21843_H
#define _VMGR_TAPE_DENMAP_TRUNK_R21843_H
struct vmgr_tape_denmap {
	char		md_model[CA_MAXMODELLEN+1];
	char		md_media_letter[CA_MAXMLLEN+1];
	char		md_density[CA_MAXDENLEN+1];
	int		native_capacity;	/* in kbytes */
};
#endif
