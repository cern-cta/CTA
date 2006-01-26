/*
 * Copyright (C) 2003-2005 by CERN/IT/ADC/CA
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cns_acl.c,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:16 $ CERN IT-ADC/CA Jean-Philippe Baud";
#endif /* not lint */
 
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include "Cns.h"
#include "Cns_server.h"
#include "serrno.h"

/*	Cns_acl_chmod - propagate new mode to access ACL */

Cns_acl_chmod (struct Cns_file_metadata *fmd_entry)
{
	int entry_len;
	char *iacl;
	char *nacl;
	char oldacl[CA_MAXACLENTRIES*13];
	char *p;

	strcpy (oldacl, fmd_entry->acl);
	iacl = oldacl;
	nacl = fmd_entry->acl;
	while (1) {
		p = strchr (iacl, ',');
		if (nacl != fmd_entry->acl)
			*nacl++ = ',';
		*nacl++ = *iacl;
		switch (*iacl - '@') {
		case CNS_ACL_USER_OBJ:
			*nacl++ = (fmd_entry->filemode >> 6 & 07) + '0';
			break;
		case CNS_ACL_GROUP_OBJ:
		case CNS_ACL_MASK:
			*nacl++ = (fmd_entry->filemode >> 3 & 07) + '0';
			break;
		case CNS_ACL_OTHER:
			*nacl++ = (fmd_entry->filemode & 07) + '0';
			break;
		default:
			*nacl++ = *(iacl+1);
		}
		if (p)
			entry_len = p - iacl - 2;
		else
			entry_len = strlen (iacl) - 2;
		strncpy (nacl, iacl + 2, entry_len);
		nacl += entry_len;
		if (! p) break;
		iacl = p + 1;
	}
	*nacl = '\0';
	return (0);
}

/*	Cns_acl_chown - propagate new ownership to access ACL */

Cns_acl_chown (struct Cns_file_metadata *fmd_entry)
{
	int entry_len;
	char *iacl;
	char *nacl;
	char oldacl[CA_MAXACLENTRIES*13];
	char *p;

	strcpy (oldacl, fmd_entry->acl);
	iacl = oldacl;
	nacl = fmd_entry->acl;
	while (1) {
		p = strchr (iacl, ',');
		if (nacl != fmd_entry->acl)
			*nacl++ = ',';
		*nacl++ = *iacl;
		*nacl++ = *(iacl+1);
		switch (*iacl - '@') {
		case CNS_ACL_USER_OBJ:
			nacl += sprintf (nacl, "%d", fmd_entry->uid);
			break;
		case CNS_ACL_GROUP_OBJ:
			nacl += sprintf (nacl, "%d", fmd_entry->gid);
			break;
		default:
			if (p)
				entry_len = p - iacl - 2;
			else
				entry_len = strlen (iacl) - 2;
			strncpy (nacl, iacl + 2, entry_len);
			nacl += entry_len;
		}
		if (! p) break;
		iacl = p + 1;
	}
	*nacl = '\0';
	return (0);
}

/*	Cns_acl_compare - routine used by qsort to order ACL entries */

Cns_acl_compare (const void *acl1, const void *acl2)
{
	if (((struct Cns_acl *)acl1)->a_type < ((struct Cns_acl *)acl2)->a_type)
		return (-1);
	if (((struct Cns_acl *)acl1)->a_type > ((struct Cns_acl *)acl2)->a_type)
		return (1);
	if (((struct Cns_acl *)acl1)->a_id < ((struct Cns_acl *)acl2)->a_id)
		return (-1);
	if (((struct Cns_acl *)acl1)->a_id > ((struct Cns_acl *)acl2)->a_id)
		return (1);
	return (0);
}

/*      Cns_acl_inherit - inherit ACLs from parent default ACL entries */

Cns_acl_inherit (struct Cns_file_metadata *parent_dir, struct Cns_file_metadata *fmd_entry, mode_t mode)
{
	char acl_mask = 0x7F;
	int entry_len;
	char *iacl;
	char *nacl;		/* ACL for new CNS entry */
	char *p;
	char *pacl;		/* parent default ACL */

	pacl = strchr (parent_dir->acl, CNS_ACL_DEFAULT|CNS_ACL_USER_OBJ|'@');
	if (! pacl)
		return (0);	/* no default acl */
	nacl = fmd_entry->acl;

	/* Get CNS_ACL_MASK if any */

	if (iacl = strchr (pacl, CNS_ACL_DEFAULT|CNS_ACL_MASK|'@'))
		acl_mask = *(iacl + 1) - '0';

	/* Build access ACL */

	iacl = pacl;
	if (acl_mask == 0x7F &&		/* no default mask */
	    (fmd_entry->filemode & S_IFDIR) == 0) {	/* not a directory */
		/* no need to build access ACLs, just update filemode */
		while (1) {
			p = strchr (iacl, ',');
			switch (*iacl - '@') {
			case CNS_ACL_DEFAULT | CNS_ACL_USER_OBJ:
				fmd_entry->filemode = fmd_entry->filemode & 0177077 |
					(mode & (*(iacl+1) - '0') << 6);
				break;
			case CNS_ACL_DEFAULT | CNS_ACL_GROUP_OBJ:
				fmd_entry->filemode = fmd_entry->filemode & 0177707 |
					(mode & (*(iacl+1) - '0') << 3);
				break;
			case CNS_ACL_DEFAULT | CNS_ACL_OTHER:
				fmd_entry->filemode = fmd_entry->filemode & 0177770 |
					(mode & (*(iacl+1) - '0'));
				break;
			}
			if (! p) break;
			iacl = p + 1;
		}
	} else {
		while (1) {
			p = strchr (iacl, ',');
			if (nacl != fmd_entry->acl)
				*nacl++ = ',';
			*nacl++ = *iacl & ~ CNS_ACL_DEFAULT;
			switch (*iacl - '@') {
			case CNS_ACL_DEFAULT | CNS_ACL_USER_OBJ:
				*nacl++ = (*(iacl+1) & (mode >> 6 & 7)) + '0';
				fmd_entry->filemode = fmd_entry->filemode & 0177077 |
					(mode & (*(iacl+1) - '0') << 6);
				nacl += sprintf (nacl, "%d", fmd_entry->uid);
				break;
			case CNS_ACL_DEFAULT | CNS_ACL_GROUP_OBJ:
				*nacl++ = (*(iacl+1) & (mode >> 3 & 7)) + '0';
				fmd_entry->filemode = fmd_entry->filemode & 0177707 |
					(mode & (*(iacl+1) - '0') << 3);
				nacl += sprintf (nacl, "%d", fmd_entry->gid);
				break;
			case CNS_ACL_DEFAULT | CNS_ACL_MASK:
				*nacl++ = (*(iacl+1) & (mode >> 3 & 7)) + '0';
				fmd_entry->filemode = fmd_entry->filemode & 0177707 |
					(mode & (*(iacl+1) - '0') << 3);
				*nacl++ = '0';
				break;
			case CNS_ACL_DEFAULT | CNS_ACL_OTHER:
				*nacl++ = (*(iacl+1) & (mode & 7)) + '0';
				fmd_entry->filemode = fmd_entry->filemode & 0177770 |
					(mode & (*(iacl+1) - '0'));
				*nacl++ = '0';
				break;
			default:
				if (p)
					entry_len = p - iacl - 1;
				else
					entry_len = strlen (iacl) - 1;
				strncpy (nacl, iacl + 1, entry_len);
				nacl += entry_len;
			}
			if (! p) break;
			iacl = p + 1;
		}
	}

	/* if new CNS entry is a directory, copy default ACL */

	if (fmd_entry->filemode & S_IFDIR) {
		if (nacl != fmd_entry->acl)
			*nacl++ = ',';
		strcpy (nacl, pacl);
	} else
		*nacl = '\0';
	return (0);
}

/*      Cns_acl_validate - validate set of ACL entries */

Cns_acl_validate (struct Cns_acl *acl, int nentries)
{
	struct Cns_acl *aclp;
	int i;
	int ndefs = 0;
	int ndg = 0;
	int ndgo = 0;
	int ndm = 0;
	int ndo = 0;
	int ndu = 0;
	int nduo = 0;
	int ng = 0;
	int ngo = 0;
	int nm = 0;
	int no = 0;
	int nu = 0;
	int nuo = 0;

	for (i = 0, aclp = acl; i < nentries; i++, aclp++) {
		switch (aclp->a_type) {
		case CNS_ACL_USER_OBJ:
			nuo++;
			break;
		case CNS_ACL_USER:
			nu++;
			break;
		case CNS_ACL_GROUP_OBJ:
			ngo++;
			break;
		case CNS_ACL_GROUP:
			ng++;
			break;
		case CNS_ACL_MASK:
			nm++;
			break;
		case CNS_ACL_OTHER:
			no++;
			break;
		case CNS_ACL_DEFAULT | CNS_ACL_USER_OBJ:
			ndefs++;
			nduo++;
			break;
		case CNS_ACL_DEFAULT | CNS_ACL_USER:
			ndefs++;
			ndu++;
			break;
		case CNS_ACL_DEFAULT | CNS_ACL_GROUP_OBJ:
			ndefs++;
			ndgo++;
			break;
		case CNS_ACL_DEFAULT | CNS_ACL_GROUP:
			ndefs++;
			ndg++;
			break;
		case CNS_ACL_DEFAULT | CNS_ACL_MASK:
			ndefs++;
			ndm++;
			break;
		case CNS_ACL_DEFAULT | CNS_ACL_OTHER:
			ndefs++;
			ndo++;
			break;
		default:
			return (-1);
		}
		if (aclp->a_perm > 7)
			return (-1);
	}
	/* There must be one and only one of each type USER_OBJ, GROUP_OBJ, OTHER */
	if (nuo != 1 || ngo != 1 || no != 1)
		return (-1);
	/* If there is any USER or GROUP entry, there must be a MASK entry */
	if ((nu || ng) && nm != 1)
		return (-1);
	/* If there are any default ACL entries, there must be one and only one
	   entry of each type DEF_USER_OBJ, DEF_GROUP_OBJ, DEF_OTHER */
	if (ndefs && (nduo != 1 || ndgo != 1 || ndo != 1))
		return (-1);
	if ((ndu || ndg) && ndm != 1)
		return (-1);

	/* check for duplicate entries USER or GROUP */

	for (i = 1, aclp = acl + 1; i < nentries; i++, aclp++) {
		if ((aclp->a_type == (aclp-1)->a_type &&
		    aclp->a_id == (aclp-1)->a_id))
			return (-1);
	}
	return (0);
}

/*	Cns_chkaclperm - check access permissions */

Cns_chkaclperm (struct Cns_file_metadata *fmd_entry, int mode, uid_t uid, gid_t gid)
{
	char acl_mask = 0x7F;
	int acl_id;
	char *iacl;
	char *p;

	/* check USER */

	if (fmd_entry->uid == uid)
		return ((fmd_entry->filemode & mode) != mode);

	/* Get CNS_ACL_MASK if any */

	if (iacl = strchr (fmd_entry->acl, CNS_ACL_MASK|'@'))
		acl_mask = *(iacl + 1) - '0';
	mode >>= 6;

	/* check CNS_ACL_USER entries if any */

	for (iacl = fmd_entry->acl; iacl; iacl = p) {
		if (*iacl - '@' > CNS_ACL_USER) break;
		p = strchr (iacl, ',');
		p++;
		if (*iacl - '@' < CNS_ACL_USER) continue;
		acl_id = atoi (iacl + 2);
		if (uid == acl_id)
			return ((*(iacl + 1) & acl_mask & mode) != mode);
		if (uid < acl_id) break;
	}

	/* check GROUP */

	iacl = strchr (iacl, CNS_ACL_GROUP_OBJ|'@');
	if (fmd_entry->gid == gid)
		return ((*(iacl + 1) & acl_mask & mode) != mode);

	/* check CNS_ACL_GROUP entries if any */

	for ( ; iacl; iacl = p) {
		if (*iacl - '@' > CNS_ACL_GROUP) break;
		p = strchr (iacl, ',');
		p++;
		if (*iacl - '@' < CNS_ACL_GROUP) continue;
		acl_id = atoi (iacl + 2);
		if (gid == acl_id)
			return ((*(iacl + 1) & acl_mask & mode) != mode);
		if (gid < acl_id) break;
	}

	/* OTHER */

	return ((fmd_entry->filemode & mode) != mode);
}
