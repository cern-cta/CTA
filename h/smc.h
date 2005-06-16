/*
 * $Id: smc.h,v 1.8 2005/06/16 09:43:26 bcouturi Exp $
 */

/*
 * Copyright (C) 1998-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: smc.h,v $ $Revision: 1.8 $ $Date: 2005/06/16 09:43:26 $ CERN IT-PDP/DM   Jean-Philippe Baud
 */

#ifndef _SMC_H
#define _SMC_H

			/* error messages */

#define	SR001	"SR001 - drive ordinal must be a non negative integer\n"
#define	SR002	"SR002 - option -%c and -%c are mutually exclusive\n"
#define	SR003	"SR003 - invalid query type %c\n"
#define	SR004	"SR004 - vid %s must be at most 6 characters long\n"
#define	SR005	"SR005 - loader must be specified\n"
#define	SR006	"SR006 - drive ordinal is mandatory for demount operations\n"
#define	SR007	"SR007 - drive ordinal and vid are mandatory for mount operations\n"
#define	SR008	"SR008 - invalid device ordinal (must be < %d)\n"
#define	SR009	"SR009 - vid mismatch: %s on request, %s on drive\n"
#define	SR010	"SR010 - number of elements must be a positive integer\n"
#define	SR011	"SR011 - vid is mandatory for export operations\n"
#define	SR012	"SR012 - cannot allocate enough memory\n"
#define	SR013	"SR013 - export slots are full\n"
#define	SR014	"SR014 - slot ordinal must be a non negative integer\n"
#define	SR015	"SR015 - storage cells are full\n"
#define	SR016	"SR016 - invalid slot address (must be < %d)\n"
#define	SR017	"SR017 - %s %s failed : %s\n"
#define	SR018	"SR018 - %s of %s on drive %d failed : %s\n"
#define	SR019	"SR019 - %s : %s error : %s\n"
#define	SR020	"SR020 - %s failed : %s\n"
#define	SR021	"SR021 - specify source slot and target slot\n"

			/* smc structures */

struct robot_info {
	char inquiry[32];
	int transport_start;
	int transport_count;
	int slot_start;
	int slot_count;
	int port_start;
	int port_count;
	int device_start;
	int device_count;
};

struct smc_element_info {
	int element_address;
	int element_type;
	int state;
	unsigned char asc;
	unsigned char ascq;
	int flags;
	int source_address;
	char name[9];
};

struct smc_status {
	unsigned char asc;
	unsigned char ascq;
	int save_errno;
	int rc;		/* return code from send_scsi_cmd */
	unsigned char sensekey;
	int skvalid;	/* sense key is valid */
};
#endif
