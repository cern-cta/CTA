/*
 * $Id: stage_constants.h,v 1.6 2001/05/31 12:08:36 jdurand Exp $
 */

#ifndef __stage_constants_h
#define __stage_constants_h

/* =================================== */
/* stage configuration default values  */
/* =================================== */
#ifdef  STAGE_NAME
#undef  STAGE_NAME
#endif
#define STAGE_NAME "stage"

#ifdef  STAGE_PROTO
#undef  STAGE_PROTO
#endif
#define STAGE_PROTO "tcp"

#ifdef  STAGE_PORT
#undef  STAGE_PORT
#endif
#define STAGE_PORT 5007

/* =================================== */
/* stage configuration maximum values  */
/* =================================== */
#ifdef  MAXPATH
#undef  MAXPATH
#endif
#define MAXPATH 80	/* maximum path length */

#ifdef  MAXVSN
#undef  MAXVSN
#endif
#define	MAXVSN 3	/* maximum number of vsns/vids on a stage command */

/* ====================================== */
/* stage configuration status bit values  */
/* ====================================== */
#ifdef  NOSTAGED
#undef  NOSTAGED
#endif
#define	NOTSTAGED	  0x000000   /* not staged */

#ifdef  WAITING_SPC
#undef  WAITING_SPC
#endif
#define	WAITING_SPC	  0x000010	/* waiting space */

#ifdef  WAITING_REQ
#undef  WAITING_REQ
#endif
#define	WAITING_REQ	  0x000020	/* waiting on an other request */

#ifdef  STAGED
#undef  STAGED
#endif
#define	STAGED		  0x000030	/* stage complete */

#ifdef  KILLED
#undef  KILLED
#endif
#define	KILLED		  0x000040	/* stage killed */

#ifdef  STG_FAILED
#undef  STG_FAILED
#endif
#define	STG_FAILED	  0x000050	/* stage failed */

#ifdef  PUT_FAILED
#undef  PUT_FAILED
#endif
#define	PUT_FAILED	  0x000060	/* stageput failed */

#ifdef  STAGED_LSZ
#undef  STAGED_LSZ
#endif
#define	STAGED_LSZ	  0x000100	/* stage limited by size */

#ifdef  STAGED_TPE
#undef  STAGED_TPE
#endif
#define	STAGED_TPE	  0x000200	/* blocks with parity error have been skipped */

#ifdef  CAN_BE_MIGR
#undef  CAN_BE_MIGR
#endif
#define	CAN_BE_MIGR	  0x000400	/* file can be migrated */

#ifdef  LAST_TPFILE
#undef  LAST_TPFILE
#endif
#define	LAST_TPFILE	  0x001000   /* last file on this tape */

#ifdef  BEING_MIGR
#undef  BEING_MIGR
#endif
#define	BEING_MIGR	  0x002000   /* file being migrated */

#ifdef  ISSTAGEDBUSY
#undef  ISSTAGEDBUSY
#endif
#define ISSTAGEDBUSY  0x004000   /* Internal error while scanning catalog : matched twice in catalog and the one to delete is busy */

#ifdef  ISSTAGEDSYERR
#undef  ISSTAGEDSYERR
#endif
#define ISSTAGEDSYERR 0x010000   /* Internal error while scanning catalog : matched twice in catalog and cannot delete one of them */

#ifdef  WAITING_MIGR
#undef  WAITING_MIGR
#endif
#define	WAITING_MIGR  0x020000   /* file being migrated and waiting confirmation (e.g. the real migration request) */

#ifdef  WAITING_NS
#undef  WAITING_NS
#endif
#define	WAITING_NS    0x040000   /* file waiting for an entry in the HSM name server */

#ifdef  MSG_OUT
#undef  MSG_OUT
#endif
#define	MSG_OUT		0

#ifdef  MSG_ERR
#undef  MSG_ERR
#endif
#define	MSG_ERR		1

/* ==================================== */
/* stage daemon return codes and states */
/* ==================================== */

#ifdef  USERR
#undef  USERR
#endif
#define	USERR	  1	/* user error */

#ifdef  SYERR
#undef  SYERR
#endif
#define	SYERR 	  2	/* system error */

#ifdef  UNERR
#under  UNERR
#endif
#define	UNERR	  3	/* undefined error */

#ifdef  CONFERR
#undef  CONFERR
#endif
#define	CONFERR	  4	/* configuration error */

#ifdef  LNKNSUP
#undef  LNKNSUP
#endif
#define	LNKNSUP	189	/* symbolic links not supported on that platform */

#ifdef  CLEARED
#undef  CLEARED
#endif
#define	CLEARED	192	/* aborted by stageclr */

#ifdef  BLKSKPD
#undef  BLKSKPD
#endif
#define	BLKSKPD	193	/* blocks were skipped */

#ifdef  TPE_LSZ
#undef  TPE_LSZ
#endif
#define	TPE_LSZ	194	/* blocks were skipped, stageing limited by size */

#ifdef  MNYPARI
#undef  MNYPARI
#endif
#define	MNYPARI	195	/* stagein stopped: too many tape errors, but -E keep */

#ifdef  REQKILD
#undef  REQKILD
#endif
#define	REQKILD	196	/* request killed by user */

#ifdef  LIMBYSZ
#undef  LIMBYSZ
#endif
#define	LIMBYSZ	197	/* limited by size */

#ifdef  ENOUGHF
#undef  ENOUGHF
#endif
#define	ENOUGHF	199	/* enough free space */

#endif /* __stage_constants_h */
