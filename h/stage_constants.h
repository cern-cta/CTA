/*
 * $Id: stage_constants.h,v 1.2 2001/02/05 11:22:52 jdurand Exp $
 */

#ifndef __stage_constants_h
#define __stage_constants_h

#ifdef MAXPATH
#undef MAXPATH
#endif
#define MAXPATH 80	/* maximum path length */
#ifdef MAXVSN
#undef MAXVSN
#endif
#define	MAXVSN 3	/* maximum number of vsns/vids on a stage command */

#define	NOTSTAGED	  0x00000   /* not staged */
#define	WAITING_SPC	  0x00010	/* waiting space */
#define	WAITING_REQ	  0x00020	/* waiting on an other request */
#define	STAGED		  0x00030	/* stage complete */
#define	KILLED		  0x00040	/* stage killed */
#define	STG_FAILED	  0x00050	/* stage failed */
#define	PUT_FAILED	  0x00060	/* stageput failed */
#define	STAGED_LSZ	  0x00100	/* stage limited by size */
#define	STAGED_TPE	  0x00200	/* blocks with parity error have been skipped */
#define	CAN_BE_MIGR	  0x00400	/* file can be migrated */
#define	LAST_TPFILE	  0x01000   /* last file on this tape */
#define	BEING_MIGR	  0x02000   /* file being migrated */
#define ISSTAGEDBUSY  0x04000   /* Internal error while scanning catalog : matched twice in catalog and the one to delete is busy */
#define ISSTAGEDSYERR 0x10000   /* Internal error while scanning catalog : matched twice in catalog and cannot delete one of them */
#define	WAITING_MIGR  0x20000   /* file being migrated and waiting confirmation (e.g. the real migration request) */
#define	WAITING_NS    0x40000   /* file waiting for an entry in the HSM name server */

#endif /* __stage_constants_h */
