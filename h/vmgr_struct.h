/*
 * $Id: vmgr_struct.h,v 1.1 2005/03/17 10:12:17 obarring Exp $
 */

/*
 * Copyright (C) 1999-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
/*
 * @(#)$RCSfile: vmgr_struct.h,v $ $Revision: 1.1 $ $Date: 2005/03/17 10:12:17 $ CERN IT-PDP/DM Jean-Philippe Baud
 */
 
#ifndef _VMGR_STRUCT_H
#define _VMGR_STRUCT_H
 
                        /* structures common to volume manager client and server */

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

struct vmgr_tape_library {
	char		name[CA_MAXTAPELIBLEN+1];
	int		capacity;	/* number of slots */
	int		nb_free_slots;
	short		status;
};

struct vmgr_tape_media {
	char		m_model[CA_MAXMODELLEN+1];
	char		m_media_letter[CA_MAXMLLEN+1];
	int		media_cost;
};
 
struct vmgr_tape_denmap {
	char		md_model[CA_MAXMODELLEN+1];
	char		md_media_letter[CA_MAXMLLEN+1];
	char		md_density[CA_MAXDENLEN+1];
	int		native_capacity;	/* in kbytes */
};

struct vmgr_tape_dgnmap {
	char		dgn[CA_MAXDGNLEN+1];
	char		model[CA_MAXMODELLEN+1];
	char		library[CA_MAXTAPELIBLEN+1];
};

struct vmgr_tape_dgn {
	char		dgn[CA_MAXDGNLEN+1];
	int		weight;
	int		deltaweight;
};

struct vmgr_tape_pool {
	char		name[CA_MAXPOOLNAMELEN+1];
	uid_t		uid;
	gid_t		gid;
	u_signed64	capacity;
	u_signed64	tot_free_space;
};
#endif
