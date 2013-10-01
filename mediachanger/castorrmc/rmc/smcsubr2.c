/*
 * Copyright (C) 1998-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */
 

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include "h/Ctape.h"
#include "h/Ctape_api.h"
#include "h/getconfent.h"
#include "h/rmc_smcsubr.h"
#include "h/rmc_smcsubr2.h"
#include "h/serrno.h"
#include "h/smc_constants.h"
#include <stdarg.h>

static int rmc_usrmsg(const int rpfd, const char *func, const char *const msg, ...)
{
	va_list args;
	char prtbuf[PRTBUFSZ];
	const int save_errno = errno;

	va_start (args, msg);
	snprintf (prtbuf, sizeof(prtbuf), "%s: ", func);
	prtbuf[sizeof(prtbuf) - 1] = '\0';
	{
		const size_t nbBytesUsed = strlen (prtbuf);

		/* If there is still space in the print buffer */
		if(nbBytesUsed < (sizeof(prtbuf))) {
			const size_t nbBytesRemaining = sizeof(prtbuf) -
				nbBytesUsed;
			char *const p = prtbuf + nbBytesUsed;
			vsnprintf (p, nbBytesRemaining, msg, args);
			prtbuf[sizeof(prtbuf) - 1] = '\0';
		}
	}
	sendrep (rpfd, MSG_ERR, "%s", prtbuf);
	va_end (args);
	errno = save_errno;
	return (0);
}

int smc_dismount (
	const int rpfd,
	const int fd,
	const char *const loader,
	struct robot_info *const robot_info,
	const int drvord,
	const char *const vid)
{
	const unsigned int max_element_status_reads = 20;
	const unsigned int dismount_status_read_delay = 1; /* In seconds */
	unsigned int nb_element_status_reads = 0;
	int drive_not_unloaded = 1;
	struct smc_element_info drive_element_info;
	char func[16];
	char *msgaddr = 0;
	struct smc_status smc_status;
 
	strncpy (func, "smc_dismount", sizeof(func));
	func[sizeof(func) - 1] = '\0';

	memset(&smc_status, '\0', sizeof(smc_status));

	/* IBM libraries sometimes disagree with the eject of their drives. */
	/* Sometimes the access bit of the result of Read Element Status    */
	/* (XB8) indicates the gripper cannot access the tape even though   */
	/* the eject was successful.  Reading the element status at a later */
	/* point in time eventually indicates the tape is accessible.       */
	while(drive_not_unloaded && nb_element_status_reads < max_element_status_reads) {
		if (0 > smc_read_elem_status (fd, loader, 4, robot_info->device_start+drvord,
		    	1, &drive_element_info)) {
			const int smc_error = smc_lasterror (&smc_status, &msgaddr);
			rmc_usrmsg ( rpfd, func, SR020, "read_elem_status", msgaddr);
			return (smc_error);
		}
		if (0 == (drive_element_info.state & 0x1)) {
			rmc_usrmsg ( rpfd, func, SR018, "demount", vid, drvord, "Medium Not Present");
			return (RBT_OK);
		}

		drive_not_unloaded = (0 == (drive_element_info.state & 0x8));
		if (drive_not_unloaded) {
			rmc_usrmsg ( rpfd, func, "read_elem_status of %s on drive %d detected Drive Not Unloaded\n", vid, drvord);
		}

		nb_element_status_reads++;

		if(nb_element_status_reads < max_element_status_reads) {
			sleep(dismount_status_read_delay);
		}
	}
	if(drive_not_unloaded) {
		rmc_usrmsg ( rpfd, func, SR018, "demount", vid, drvord, "Drive Not Unloaded");
		return (RBT_UNLD_DMNT);
	}

	if (*vid && strcmp (drive_element_info.name, vid)) {
		rmc_usrmsg ( rpfd, func, SR009, vid, drive_element_info.name);
		return (RBT_NORETRY);
	}
	if (0 > smc_move_medium (fd, loader, robot_info->device_start+drvord,
	    drive_element_info.source_address, (drive_element_info.flags & 0x40) ? 1 : 0)) {
		const int smc_error = smc_lasterror (&smc_status, &msgaddr);
		rmc_usrmsg ( rpfd, func, SR018, "demount", vid, drvord, msgaddr);
		return (smc_error);
	}
    /* check that the vid is in a slot before returning */
    while (1) {   
          struct smc_element_info vol_element_info;
          if (0 > smc_find_cartridge (fd, loader, drive_element_info.name, 0, 0, 1, &vol_element_info)) {
              const int smc_error = smc_lasterror (&smc_status, &msgaddr);
              rmc_usrmsg ( rpfd, func, SR017, "find_cartridge", drive_element_info.name, msgaddr);
              return (smc_error);
          }
         
          /* vid is in a storage slot */  
          if (vol_element_info.element_type == 2) break; 
          /* give time for the tape enter the slot */
          sleep (2);
    }

	return (0);
}

int smc_export (
	const int rpfd,
	const int fd,
	const char *const loader,
	struct robot_info *const robot_info,
	const char *const vid)
{
        int c;
        struct smc_element_info element_info;
	char func[16];
	int i;
        struct smc_element_info *impexp_info;
	char *msgaddr;
	int nbelem;
	struct smc_status smc_status;
 
	strncpy (func, "smc_export", sizeof(func));
	func[sizeof(func) - 1] = '\0';

	if ((c = smc_find_cartridge (fd, loader, vid, 0, 0, 1, &element_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		rmc_usrmsg ( rpfd, func, SR017, "find_cartridge", vid, msgaddr);
		return (c);
	}
	if (c == 0) {
		rmc_usrmsg ( rpfd, func, SR017, "export", vid, "volume not in library");
		return (RBT_NORETRY);
	}
	if (element_info.element_type != 2) {
		rmc_usrmsg ( rpfd, func, SR017, "export", vid, "volume in use");
		return (RBT_SLOW_RETRY);
	}
	/* look for a free export slot */

	nbelem = robot_info->port_count;
	if ((impexp_info = malloc (nbelem * sizeof(struct smc_element_info))) == NULL) {
		rmc_usrmsg ( rpfd, func, SR012);
		return (RBT_NORETRY);
	}

	if ((c = smc_read_elem_status (fd, loader, 3, robot_info->port_start,
	    nbelem, impexp_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		rmc_usrmsg ( rpfd, func, SR020, "read_elem_status", msgaddr);
		free (impexp_info);
		return (c);
	}
	for (i = 0; i < nbelem; i++) {
		if (((impexp_info+i)->state & 0x1) == 0)	/* element free */
			break;
	}
	if (i >= nbelem) {	/* export slots are full */
		rmc_usrmsg ( rpfd, func, SR013);
		free (impexp_info);
		return (RBT_NORETRY);
	}

	if ((c = smc_move_medium (fd, loader, element_info.element_address,
	    (impexp_info+i)->element_address, 0)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		rmc_usrmsg ( rpfd, func, SR017, "export", vid, msgaddr);
		free (impexp_info);
		return (c);
	}
	free (impexp_info);
	return (0);
}

int smc_import (
	const int rpfd,
	const int fd,
	const char *const loader,
	struct robot_info *const robot_info,
	const char *const vid)
{
        int c;
	int device_start;
        struct smc_element_info *element_info;
	char func[16];
	int i, j;
	char *msgaddr;
	int nbelem;
	int port_start;
	int slot_start;
	struct smc_status smc_status;
 
	strncpy (func, "smc_import", sizeof(func));
	func[sizeof(func) - 1] = '\0';

	nbelem = robot_info->transport_count + robot_info->slot_count +
		 robot_info->port_count + robot_info->device_count;
	if ((element_info = malloc (nbelem * sizeof(struct smc_element_info))) == NULL) {
		rmc_usrmsg ( rpfd, func, SR012);
		return (RBT_NORETRY);
	}

	/* get inventory */

	if ((c = smc_read_elem_status (fd, loader, 0, 0, nbelem, element_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		rmc_usrmsg ( rpfd, func, SR020, "read_elem_status", msgaddr);
		free (element_info);
		return (c);
	}
	for (i = 0; i < c; i++)
		if ((element_info+i)->element_type == 2) break;
	slot_start = i;
	for (i = 0; i < c; i++)
		if ((element_info+i)->element_type == 3) break;
	port_start = i;
	for (i = 0; i < c; i++)
		if ((element_info+i)->element_type == 4) break;
	device_start = i;

	/* mark home slots of cartridges currently on drives as non free */

	for (i = device_start; i < device_start+robot_info->device_count; i++) {
		if (((element_info+i)->state & 0x1) == 0) continue;
		for (j = slot_start; j < slot_start+robot_info->slot_count; j++)
			if ((element_info+i)->source_address ==
				(element_info+j)->element_address) break;
		(element_info+j)->state |= 1;
	}

	/* loop on all import slots */

	for (i = port_start; i < port_start+robot_info->port_count; i++) {
		if (*vid && strcmp (vid, (element_info+i)->name)) continue;
		if (*vid || (*vid == '\0' && ((element_info+i)->state & 2))) {

			/* find a free storage slot */

			for (j = slot_start; j < slot_start+robot_info->slot_count; j++)
				if (((element_info+j)->state & 0x1) == 0) break;
			if (j >= slot_start+robot_info->slot_count) {
				rmc_usrmsg ( rpfd, func, SR015);
				free (element_info);
				return (RBT_NORETRY);
			}

			if ((c = smc_move_medium (fd, loader, (element_info+i)->element_address,
			    (element_info+j)->element_address, 0)) < 0) {
				c = smc_lasterror (&smc_status, &msgaddr);
				rmc_usrmsg ( rpfd, func, SR017, "import",
				    (element_info+i)->name, msgaddr);
				free (element_info);
				return (c);
			}
			if (*vid || c) break;
			(element_info+j)->state |= 1;	/* dest slot is now full */
		}
	}
	free (element_info);
	return (c);
}

int smc_mount (
	const int rpfd,
	const int fd,
	const char *const loader,
	struct robot_info *const robot_info,
	const int drvord,
	const char *const vid,
	const int invert)
{
    int c;
    struct smc_element_info element_info;
	char func[16];
	char *msgaddr;
	struct smc_status smc_status;
 
	strncpy (func, "smc_mount", sizeof(func));
	func[sizeof(func) - 1] = '\0';

	if ((c = smc_find_cartridge (fd, loader, vid, 0, 0, 1, &element_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		rmc_usrmsg ( rpfd, func, SR017, "find_cartridge", vid, msgaddr);
		return (c);
	}
	if (c == 0) {
		rmc_usrmsg ( rpfd, func, SR018, "mount", vid, drvord, "volume not in library");
		return (RBT_NORETRY);
	}
	if (element_info.element_type != 2) {

                /* compare requested and replied vid   */
                rmc_usrmsg ( rpfd, func, "Asked for %s, got reply for %s\n", 
                        vid, element_info.name );

                /* detail on a tape's current location */
                switch (element_info.element_type) {

                case 1:
                        rmc_usrmsg ( rpfd, func, "Location: medium transport element (0x%x)\n", 
                                element_info.element_type );
                        break;
                case 2:                        
                        /* normal case: in its home slot, not possible inside the if */
                        break;
                case 3:
                        rmc_usrmsg ( rpfd, func, "Location: import/export element (0x%x)\n", 
                                element_info.element_type );
                        break;
                case 4:
                        rmc_usrmsg ( rpfd, func, "Location: data transfer element (0x%x)\n", 
                                element_info.element_type );
                        break;
                default:
                        rmc_usrmsg ( rpfd, func, "Location: unknown (0x%x)\n", 
                                element_info.element_type );
                }

                rmc_usrmsg ( rpfd, func, SR018, "mount", vid, drvord, "volume in use");
		return (RBT_SLOW_RETRY);
	}
	if ((c = smc_move_medium (fd, loader, element_info.element_address,
	    robot_info->device_start+drvord, invert)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		rmc_usrmsg ( rpfd, func, SR018, "mount", vid, drvord, msgaddr);
		return (c);
	}
	return (0);
}
