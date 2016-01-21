
/* A collection of hopefully soon eliminated legacy constants */

#pragma once


#define TPMAGIC 0x141001
			/* tape daemon reply types */

			/* tape daemon request types */

#define TPRSV 		0	/* reserve tape resources */
#define TPMOUNT		1	/* assign drive, ask for the tape to be mounted, check VOL1 */
#define TPSTAT		2	/* get tape status display */
#define TPRSTAT_UNUSED	3	/* get resource reservation status display */
#define TPCONF		4	/* configure tape drive up/down */
#define TPRLS		5	/* unload tape and release reservation */
#define UPDVSN		6	/* update vid, vsn and mount flag in tape drive table */
#define	TPKILL		8	/* mount killed by user */
#define	FREEDRV		9	/* free drive */
#define RSLT		11	/* switch to new drive */
#define UPDFIL		12	/* update blksize, cfseq, fid, lrecl, recfm in tpfil */
#define TPINFO		13	/* get info for a given mounted tape */
#define TPPOS		14	/* position tape and check HDR1/HDR2 */
#define DRVINFO		15	/* get info for a given drive */
#define UPDDRIVE  	16      /* update the status of a drive */
#define TPLABEL  	17      /* label a tape */

#define	MSG_OUT		0
#define	MSG_ERR		1
#define	MSG_DATA	2
#define	TAPERC		3