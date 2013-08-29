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
#include "Ctape.h"
#include "Ctape_api.h"
#include "getconfent.h"
#include "serrno.h"
#include "smc.h"

/* from smcsubr.c */
extern int smc_read_elem_status(int, char *, int, int, int, struct smc_element_info[]);
extern int smc_lasterror(struct smc_status *, char **);
extern int smc_move_medium(int, char *, int, int, int);
extern int smc_find_cartridge(int, char *, char *, int, int, int, struct smc_element_info[] );

int smc_dismount (int fd,
                  char *loader,
                  struct robot_info *robot_info,
                  int drvord,
                  char *vid)
{
	const unsigned int max_element_status_reads = 20;
	const unsigned int dismount_status_read_delay = 1; /* In seconds */
	unsigned int nb_element_status_reads = 0;
	int drive_not_unloaded = 1;
	struct smc_element_info drive_element_info;
	char func[16];
	char *msgaddr = 0;
	struct smc_status smc_status;
 
	ENTRY (smc_dismount);

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
			usrmsg (func, SR020, "read_elem_status", msgaddr);
			RETURN (smc_error);
		}
		if (0 == (drive_element_info.state & 0x1)) {
			usrmsg (func, SR018, "demount", vid, drvord, "Medium Not Present");
			RETURN (RBT_OK);
		}

		drive_not_unloaded = (0 == (drive_element_info.state & 0x8));
		if (drive_not_unloaded) {
			usrmsg (func, "read_elem_status of %s on drive %d detected Drive Not Unloaded\n", vid, drvord);
		}

		nb_element_status_reads++;

		if(nb_element_status_reads < max_element_status_reads) {
			sleep(dismount_status_read_delay);
		}
	}
	if(drive_not_unloaded) {
		usrmsg (func, SR018, "demount", vid, drvord, "Drive Not Unloaded");
		RETURN (RBT_UNLD_DMNT);
	}

	if (*vid && strcmp (drive_element_info.name, vid)) {
		usrmsg (func, SR009, vid, drive_element_info.name);
		RETURN (RBT_NORETRY);
	}
	if (0 > smc_move_medium (fd, loader, robot_info->device_start+drvord,
	    drive_element_info.source_address, (drive_element_info.flags & 0x40) ? 1 : 0)) {
		const int smc_error = smc_lasterror (&smc_status, &msgaddr);
		usrmsg (func, SR018, "demount", vid, drvord, msgaddr);
		RETURN (smc_error);
	}
    /* check that the vid is in a slot before returning */
    while (1) {   
          struct smc_element_info vol_element_info;
          if (0 > smc_find_cartridge (fd, loader, drive_element_info.name, 0, 0, 1, &vol_element_info)) {
              const int smc_error = smc_lasterror (&smc_status, &msgaddr);
              usrmsg (func, SR017, "find_cartridge", drive_element_info.name, msgaddr);
              RETURN (smc_error);
          }
         
          /* vid is in a storage slot */  
          if (vol_element_info.element_type == 2) break; 
          /* give time for the tape enter the slot */
          sleep (2);
    }

	RETURN (0);
}

int smc_export (int fd,
                char *loader,
                struct robot_info *robot_info,
                char *vid)
{
        int c;
        struct smc_element_info element_info;
	char func[16];
	int i;
        struct smc_element_info *impexp_info;
	char *msgaddr;
	int nbelem;
	struct smc_status smc_status;
 
	ENTRY (smc_export);
	if ((c = smc_find_cartridge (fd, loader, vid, 0, 0, 1, &element_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		usrmsg (func, SR017, "find_cartridge", vid, msgaddr);
		RETURN (c);
	}
	if (c == 0) {
		usrmsg (func, SR017, "export", vid, "volume not in library");
		RETURN (RBT_NORETRY);
	}
	if (element_info.element_type != 2) {
		usrmsg (func, SR017, "export", vid, "volume in use");
		RETURN (RBT_SLOW_RETRY);
	}
	/* look for a free export slot */

	nbelem = robot_info->port_count;
	if ((impexp_info = malloc (nbelem * sizeof(struct smc_element_info))) == NULL) {
		usrmsg (func, SR012);
		RETURN (RBT_NORETRY);
	}

	if ((c = smc_read_elem_status (fd, loader, 3, robot_info->port_start,
	    nbelem, impexp_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		usrmsg (func, SR020, "read_elem_status", msgaddr);
		free (impexp_info);
		RETURN (c);
	}
	for (i = 0; i < nbelem; i++) {
		if (((impexp_info+i)->state & 0x1) == 0)	/* element free */
			break;
	}
	if (i >= nbelem) {	/* export slots are full */
		usrmsg (func, SR013);
		free (impexp_info);
		RETURN (RBT_NORETRY);
	}

	if ((c = smc_move_medium (fd, loader, element_info.element_address,
	    (impexp_info+i)->element_address, 0)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		usrmsg (func, SR017, "export", vid, msgaddr);
		free (impexp_info);
		RETURN (c);
	}
	free (impexp_info);
	RETURN (0);
}

int smc_import (int fd,
                char *loader,
                struct robot_info *robot_info,
                char *vid)
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
 
	ENTRY (smc_import);
	nbelem = robot_info->transport_count + robot_info->slot_count +
		 robot_info->port_count + robot_info->device_count;
	if ((element_info = malloc (nbelem * sizeof(struct smc_element_info))) == NULL) {
		usrmsg (func, SR012);
		RETURN (RBT_NORETRY);
	}

	/* get inventory */

	if ((c = smc_read_elem_status (fd, loader, 0, 0, nbelem, element_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		usrmsg (func, SR020, "read_elem_status", msgaddr);
		free (element_info);
		RETURN (c);
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
				usrmsg (func, SR015);
				free (element_info);
				RETURN (RBT_NORETRY);
			}

			if ((c = smc_move_medium (fd, loader, (element_info+i)->element_address,
			    (element_info+j)->element_address, 0)) < 0) {
				c = smc_lasterror (&smc_status, &msgaddr);
				usrmsg (func, SR017, "import",
				    (element_info+i)->name, msgaddr);
				free (element_info);
				RETURN (c);
			}
			if (*vid || c) break;
			(element_info+j)->state |= 1;	/* dest slot is now full */
		}
	}
	free (element_info);
	RETURN (c);
}

int smc_mount (int fd,
               char *loader,
               struct robot_info *robot_info,
               int drvord,
               char *vid,
               int invert)
{
    int c;
    struct smc_element_info element_info;
	char func[16];
	char *msgaddr;
	struct smc_status smc_status;
 
	ENTRY (smc_mount);
	if ((c = smc_find_cartridge (fd, loader, vid, 0, 0, 1, &element_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		usrmsg (func, SR017, "find_cartridge", vid, msgaddr);
		RETURN (c);
	}
	if (c == 0) {
		usrmsg (func, SR018, "mount", vid, drvord, "volume not in library");
		RETURN (RBT_NORETRY);
	}
	if (element_info.element_type != 2) {

                /* compare requested and replied vid   */
                usrmsg( func, "Asked for %s, got reply for %s\n", 
                        vid, element_info.name );

                /* detail on a tape's current location */
                switch (element_info.element_type) {

                case 1:
                        usrmsg( func, "Location: medium transport element (0x%x)\n", 
                                element_info.element_type );
                        break;
                case 2:                        
                        /* normal case: in its home slot, not possible inside the if */
                        break;
                case 3:
                        usrmsg( func, "Location: import/export element (0x%x)\n", 
                                element_info.element_type );
                        break;
                case 4:
                        usrmsg( func, "Location: data transfer element (0x%x)\n", 
                                element_info.element_type );
                        break;
                default:
                        usrmsg( func, "Location: unknown (0x%x)\n", 
                                element_info.element_type );
                }

                usrmsg (func, SR018, "mount", vid, drvord, "volume in use");
		RETURN (RBT_SLOW_RETRY);
	}
	if ((c = smc_move_medium (fd, loader, element_info.element_address,
	    robot_info->device_start+drvord, invert)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		usrmsg (func, SR018, "mount", vid, drvord, msgaddr);
		RETURN (c);
	}
	RETURN (0);
}
