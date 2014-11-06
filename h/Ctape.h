/*
 * Ctape.h,v 1.43 2004/01/23 10:08:05 bcouturi Exp
 */

/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)Ctape.h,v 1.43 2004/01/23 10:08:05 CERN IT-PDP/DM Jean-Philippe Baud
 */

#pragma once

			/* tape daemon constants and macros */

#include "Ctape_constants.h"
#include "osdep.h"
#define CHECKI     10	     /* max interval to check for work to be done */
#define CLNREQI   180	     /* interval to check for jobs that have died */
#define LBLBUFSZ  128        /* size of buffers to read labels */
#define OPRMSGSZ  257	     /* maximum operator message/reply length */
#define	RBTFASTRI  60	     /* fast retry interval for robotic operations */
#define	RBTUNLDDMNTI  60     /* retry interval for unload/demount */
#define PRTBUFSZ  1024
#define REPBUFSZ 2800	     /* must be >= max tape daemon reply size */
#define REQBUFSZ 1104	     /* must be >= max tape daemon request size */
#define SMSGI      30	     /* retry interval when sending operator messages */
#define TPMAGIC 0x141001
#define TPTIMEOUT   5	     /* netread timeout while receiving a request */
#define UCHECKI    10	     /* max interval to check for drive ready or oper cancel */
#define VDQMCHKINTVLDFT 900  /* default check interval to confirm to VDQM that drives are free */

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

			/* tape daemon reply types */

#define	MSG_OUT		0
#define	MSG_ERR		1
#define	MSG_DATA	2
#define	TAPERC		3

			/* tape daemon messages */
 
#define TP000	"TP000 - tape daemon not available on %s\n"
#define TP001	"TP001 - no response from tape daemon\n"
#define	TP002	"TP002 - %s error : %s\n"
#define TP003   "TP003 - illegal function %d\n"
#define TP004   "TP004 - error getting request, netread = %d\n"
#define TP005	"TP005 - cannot get memory\n"
#define TP006	"TP006 - invalid value for %s\n"
#define	TP007	"TP007 - fid is mandatory when TPPOSIT_FID\n"
#define TP008	"TP008 - %s not accessible\n"
#define TP009	"TP009 - could not configure %s: %s\n"
#define TP010	"TP010 - resources already reserved for this job\n"
#define TP011	"TP011 - too many tape users\n"
#define TP012	"TP012 - too many drives requested\n"
#define TP013	"TP013 - invalid device group name\n"
#define TP014	"TP014 - reserve not done\n"
#define TP015	"TP015 - drive with specified name/characteristics does not exist\n"
#define TP017	"TP017 - cannot use blp in write mode\n"
#define TP018	"TP018 - duplicate option %s\n"
#define TP020	"TP020 - mount tape %s(%s) %s on drive %s@%s for %s %d (%s) or reply cancel | drive name"
#define TP021	"TP021 - label type mismatch: request %s, tape is %s\n"
#define TP022	"TP022 - path exists already\n"
#define TP023	"TP023 - mount cancelled by operator: %s\n"
#define TP024	"TP024 - file %s does not exist\n"
#define TP025	"TP025 - bad label structure\n\tReason  : %s\n\tPosition: ....v....1....v....2....v....3....v....4....v....5....v....6....v....7....8\n\tLabel   : %s\n"
#define TP026	"TP026 - system error\n"
#define TP027	"TP027 - you are not registered in account file\n"
#define TP028	"TP028 - file not expired\n"
#define	TP029	"TP029 - pathname is mandatory\n"
#define	TP030	"TP030 - I/O error\n"
#define	TP031	"TP031 - no vid specified\n"
#define	TP032	"TP032 - config file (line %d): %s\n"
#define	TP033	"TP033 - drive %s@%s not operational"
#define	TP034	"TP034 - all entries for a given device group must be grouped together\n"
#define TP035	"TP035 - configuring %s %s\n"
#define	TP036	"TP036 - path is mandatory when rls flags is %d\n"
#define TP037	"TP037 - path %s does not exist\n"
#define TP038	"TP038 - pathname too long\n"
#define TP039	"TP039 - vsn mismatch: request %s, tape vsn is %s\n"
#define TP040	"TP040 - release pending\n"
#define	TP041	"TP041 - %s of %s on %s failed : %s"
#define	TP042	"TP042 - %s : %s error : %s\n"
#define	TP043	"TP043 - configuration line too long: %s\n"
#define TP044	"TP044 - fid mismatch: request %s, tape fid for file %d is %s\n"
#define TP045	"TP045 - cannot write file %d (only %d files are on tape)\n"
#define TP046	"TP046 - request too large (max. %d)\n"
#define	TP047	"TP047 - reselect server requested by operator\n"
#define	TP048	"TP048 - config postponed: %s currently assigned\n"
#define	TP049	"TP049 - option IGNOREEOI is not valid for label type al or sl\n"
#define	TP050	"TP050 - vid mismatch: %s on request, %s on drive\n"
#define	TP051	"TP051 - fatal configuration error: %s %s\n"
#define	TP054	"TP054 - tape not mounted or not ready\n"
#define	TP055	"TP055 - parameter inconsistency with TMS for vid %s: %s on request <-> %s in TMS\n"
#define	TP056	"TP056 - %s request by %d,%d from %s\n"
#define	TP057	"TP057 - drive %s is not free\n"
#define	TP058	"TP058 - no free drive\n"
#define	TP059	"TP059 - invalid reason\n"
#define	TP060	"TP060 - invalid combination of method and filstat\n"
#define	TP061	"TP061 - filstat value incompatible with read-only mode\n"
#define	TP062	"TP062 - tape %s to be prelabelled %s%s"
#define	TP063	"TP063 - invalid user %d\n"
#define	TP064	"TP064 - invalid method for this label type\n"
#define TP065	"TP065 - tape %s(%s) on drive %s@%s for %s %d has bad MIR | reply to acknowledge or cancel\n"
