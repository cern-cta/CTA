/*
 * Castor_limits.h,v 1.27 2004/02/12 15:38:08 obarring Exp
 */

/*
 * Copyright (C) 1999-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)Castor_limits.h,v 1.27 2004/02/12 15:38:08 CERN IT-PDP/DM Jean-Philippe Baud
 */

#ifndef _CASTOR_LIMITS_H
#define _CASTOR_LIMITS_H

	/* all maximum lengths defined below do not include the trailing null */
#define CA_MAXDENNUM      24    /*  ...  */

#define CA_MAXCLASNAMELEN 15	/* maximum length for a fileclass name */
#define	CA_MAXCOMMENTLEN 255	/* maximum length for user comments in metadata */
#define	CA_MAXDENFIELDS    9	/* maximum number of density values in devinfo */
#define	CA_MAXDENLEN       8	/* maximum length for a alphanumeric density */
#define	CA_MAXDGNLEN       6	/* maximum length for a device group name */
#define CA_MAXDVNLEN      63	/* maximum length for a device name */
#define	CA_MAXDVTLEN       8	/* maximum length for a device type */
#define	CA_MAXFIDLEN      17	/* maximum length for a fid (DSN) */
#define	CA_MAXFSEQLEN     14	/* maximum length for a fseq string */
#define	CA_MAXGRPNAMELEN   2	/* maximum length for a group name */
#define CA_MAXHOSTNAMELEN 63	/* maximum length for a hostname */
#define	CA_MAXLBLTYPLEN    3	/* maximum length for a label type */
#define CA_MAXLINELEN   1023	/* maximum length for a line in a log */
#define	CA_MAXMANUFLEN    12	/* maximum length for a cartridge manufacturer */
#define	CA_MAXMIGPNAMELEN 15	/* maximum length for a migration policy name */
#define	CA_MAXMIGRNAMELEN 15	/* maximum length for a migrator name */
#define	CA_MAXMLLEN        1	/* maximum length for a cartridge media_letter */
#define	CA_MAXMODELLEN     6	/* maximum length for a cartridge model */
#define	CA_MAXNAMELEN    231	/* maximum length for a pathname component */
#define	CA_MAXNBDRIVES    32	/* maximum number of tape drives per server */
#define	CA_MAXPATHLEN   1023	/* maximum length for a pathname */
#define CA_MAXPOOLNAMELEN 15	/* maximum length for a pool name */
#define	CA_MAXRBTNAMELEN  17	/* maximum length for a robot name */
#define	CA_MAXRECFMLEN     3	/* maximum length for a record format */
#define CA_MAXREGEXPLEN   63    /* Maximum length for a regular expression */
#define CA_MAXSHORTHOSTLEN 10	/* maximum length for a hostname without domain */
#define	CA_MAXSNLEN       24	/* maximum length for a cartridge serial nb */
#define	CA_MAXSTGRIDLEN   77	/* maximum length for a stager full request id */
				/* must be >= nb digits in CA_MAXSTGREQID +
				   CA_MAXHOSTNAMELEN + 8 */
#define	CA_MAXSTGREQID 999999	/* maximum value for a stager request id */
#define	CA_MAXTAGLEN     255	/* maximum length for a volume tag */
#define	CA_MAXTAPELIBLEN   8	/* maximum length for a tape library name */
#define	CA_MAXUNMLEN       8	/* maximum length for a drive name */
#define	CA_MAXUSRNAMELEN  14	/* maximum length for a login name */
#define	CA_MAXVIDLEN       6	/* maximum length for a VID */
#define	CA_MAXVSNLEN       6	/* maximum length for a VSN */
#define CA_MAXCKSUMNAMELEN 15    /* maximum length for a checksum algorithm name */
#define CA_MAXDMPROTNAMELEN 15    /* maximum length for Disk Mover protocol name */

/* Max allowed uid/gif */
#define CA_MAXUID    0x7FFFFFFF /* Maximum uid */
#define CA_MAXGID    0x7FFFFFFF /* Maximum gid */
#endif
