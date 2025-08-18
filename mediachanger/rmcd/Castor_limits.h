/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 1999-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

	/* all maximum lengths defined below do not include the trailing null */

#define CA_MAXACLENTRIES 300	/* maximum number of ACL entries for a file/dir */
#define CA_MAXCLASNAMELEN 15	/* maximum length for a fileclass name */
#define	CA_MAXCOMMENTLEN 255	/* maximum length for user comments in metadata */
#define	CA_MAXDENLEN       8	/* maximum length for a alphanumeric density */
#define	CA_MAXDGNLEN       6	/* maximum length for a device group name */
#define CA_MAXDVNLEN      63	/* maximum length for a device name */
#define	CA_MAXDVTLEN       8	/* maximum length for a device type */
#define	CA_MAXFIDLEN      17	/* maximum length for a fid (DSN) */
#define	CA_MAXFSEQLEN     14	/* maximum length for a fseq string */
#define	CA_MAXGRPNAMELEN   2	/* maximum length for a group name */
#define	CA_MAXGUIDLEN     36	/* maximum length for a guid */
#define CA_MAXHOSTNAMELEN 63	/* maximum length for a hostname */
#define CA_MAXSVCCLASSNAMELEN 63	/* maximum length for a svc class name */
#define	CA_MAXLBLTYPLEN    3	/* maximum length for a label type */
#define CA_MAXLINELEN   1023	/* maximum length for a line in a log */
#define	CA_MAXMANUFLEN    12	/* maximum length for a cartridge manufacturer */
#define	CA_MAXMIGPNAMELEN 15	/* maximum length for a migration policy name */
#define	CA_MAXMIGRNAMELEN 15	/* maximum length for a migrator name */
#define	CA_MAXMLLEN        1	/* maximum length for a cartridge media_letter */
#define	CA_MAXMODELLEN     6	/* maximum length for a cartridge model */
#define	CA_MAXNAMELEN    255	/* maximum length for a pathname component */
#define	CA_MAXNBDRIVES     4	/* maximum number of tape drives per server */
#define	CA_MAXPATHLEN   1023	/* maximum length for a pathname */
#define CA_MAXPOOLNAMELEN 15	/* maximum length for a pool name */
#define CA_MAXPROTOLEN     7	/* maximum length for a protocol name */
#define	CA_MAXRBTNAMELEN  17	/* maximum length for a robot name */
#define	CA_MAXRECFMLEN     3	/* maximum length for a record format */
#define CA_MAXREGEXPLEN   63    /* Maximum length for a regular expression */
#define CA_MAXCSECNAMELEN 512	/* Maximum length for a Csec authorization id */
#define CA_MAXCSECPROTOLEN 20	/* Maximum length for a Csec mechanism */
#define	CA_MAXSFNLEN    1103	/* maximum length for a replica */
#define CA_MAXSHORTHOSTLEN 10	/* maximum length for a hostname without domain */
#define	CA_MAXSNLEN       24	/* maximum length for a cartridge serial nb */
#define	CA_MAXSTGRIDLEN   77	/* maximum length for a stager full request id */
				/* must be >= nb digits in CA_MAXSTGREQID +
				   CA_MAXHOSTNAMELEN + 8 */
#define	CA_MAXSTGREQID 999999	/* maximum value for a stager request id */
#define	CA_MAXSYMLINKS     5	/* maximum number of symbolic links */
#define	CA_MAXTAGLEN     255	/* maximum length for a volume tag */
#define	CA_MAXTAPELIBLEN   8	/* maximum length for a tape library name */
#define	CA_MAXUNMLEN       8	/* maximum length for a drive name */
#define	CA_MAXUSRNAMELEN  14	/* maximum length for a login name */
#define	CA_MAXVIDLEN       6	/* maximum length for a VID */
#define	CA_MAXVSNLEN       6	/* maximum length for a VSN */
#define CA_MAXCKSUMNAMELEN 15   /* maximum length for a checksum algorithm name */
#define CA_MAXCKSUMLEN     32   /* maximum length for a checksum value in an asci form */
#define CA_MAXDMPROTNAMELEN 15  /* maximum length for Disk Mover protocol name */
#define CA_MAXJOBIDLEN     36   /* Maximum length for the representation of the Cuuid */
#define CA_MAXUSERTAGLEN    63  /* Maximum length for a user tag (stage request) */
#define CA_MAXRFSLINELEN 2047   /* maximum length for the requested filesystem string */

/* Max allowed uid/gif */
#define CA_MAXUID    0x7FFFFFFF /* Maximum uid */
#define CA_MAXGID    0x7FFFFFFF /* Maximum gid */
