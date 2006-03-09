/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: wrttpmrk.c,v $ $Revision: 1.6 $ $Date: 2006/03/09 17:17:33 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#if defined(_AIX) && defined(_IBMR2)
#include <sys/tape.h>
#define mtop stop
#define mt_op st_op
#define mt_count st_count
#define MTIOCTOP STIOCTOP
#define MTWEOF STWEOF
#else
#if defined(_WIN32)
#include <windows.h>
#else
#include <sys/ioctl.h>
#include <sys/mtio.h>
#endif
#endif
#include "Ctape.h"
#include "serrno.h"
#include "scsictl.h"
wrttpmrk(tapefd, path, n)
#if defined(_WIN32)
HANDLE tapefd;
#else
int tapefd;
#endif
char *path;
int n;
{
	char func[16];
#if !defined(_WIN32)
	unsigned char cdb[6];
	char *msgaddr;
	char *getconfent();
	char *p;
	int nb_sense_ret;
	unsigned char sense[MAXSENSE];
#endif

	ENTRY (wrttpmrk);
#if !defined(_WIN32)
	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x10;                /* write tape mark code */

    if ((p = getconfent ("TAPE", "IMMEDIATE", 0)) != NULL) {
	  if (strcmp (p, "YES") || strcmp (p, "yes")){
	    cdb[1]= 0x01;             /* immediate bit set */
	  } else {
	       cdb[1]= 0x00;             /* immediate bit not set */
      }
    } else {
	      cdb[1] = 0x00;        /* immediate bit not set */
    } 

	cdb[4] = cdb[4] | n;    /* number of tapemarks*/

	if (send_scsi_cmd (tapefd, path, 0, cdb, 6, NULL, 36, sense, 38, 30000, 8, &nb_sense_ret, &msgaddr) < 0) {
#else
	if (WriteTapemark (tapefd, TAPE_FILEMARKS, n, n ? (BOOL)1 : (BOOL)0)) {
#endif
		serrno = rpttperror (func, tapefd, path, "ioctl");
		RETURN (-1);
	}
	RETURN (0);
}
