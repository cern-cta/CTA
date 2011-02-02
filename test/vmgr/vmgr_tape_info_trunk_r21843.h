/*
 * Copyright (C) 1999-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef _VMGR_TAPE_INFO_TRUNK_R21843_H
#define _VMGR_TAPE_INFO_TRUNK_R21843_H
struct vmgr_tape_info {
	char		vid[CA_MAXVIDLEN+1];
	char		vsn[CA_MAXVSNLEN+1];
	char		library[CA_MAXTAPELIBLEN+1];
	char		density[CA_MAXDENLEN+1];
	char		lbltype[CA_MAXLBLTYPLEN+1];
	char		model[CA_MAXMODELLEN+1];
	char		media_letter[CA_MAXMLLEN+1];
	char		manufacturer[CA_MAXMANUFLEN+1];
	char		sn[CA_MAXSNLEN+1];	/* serial number */
	int		nbsides;
	time_t		etime;
	int		rcount;
	int		wcount;
	char		rhost[CA_MAXSHORTHOSTLEN+1];
	char		whost[CA_MAXSHORTHOSTLEN+1];
	int		rjid;
	int		wjid;
	time_t		rtime;		/* last access to tape in read mode */
	time_t		wtime;		/* last access to tape in write mode */
	int		side;
	char		poolname[CA_MAXPOOLNAMELEN+1];
	short		status;		/* TAPE_FULL, DISABLED, EXPORTED */
	int		estimated_free_space;	/* in kbytes */
	int		nbfiles;
};
#endif
