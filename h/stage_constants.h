/*
 * $Id: stage_constants.h,v 1.4 2001/03/28 14:00:25 jdurand Exp $
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

#define	NOTSTAGED	  0x000000   /* not staged */
#define	WAITING_SPC	  0x000010	/* waiting space */
#define	WAITING_REQ	  0x000020	/* waiting on an other request */
#define	STAGED		  0x000030	/* stage complete */
#define	KILLED		  0x000040	/* stage killed */
#define	STG_FAILED	  0x000050	/* stage failed */
#define	PUT_FAILED	  0x000060	/* stageput failed */
#define	STAGED_LSZ	  0x000100	/* stage limited by size */
#define	STAGED_TPE	  0x000200	/* blocks with parity error have been skipped */
#define	CAN_BE_MIGR	  0x000400	/* file can be migrated */
#define	LAST_TPFILE	  0x001000   /* last file on this tape */
#define	BEING_MIGR	  0x002000   /* file being migrated */
#define ISSTAGEDBUSY  0x004000   /* Internal error while scanning catalog : matched twice in catalog and the one to delete is busy */
#define ISSTAGEDSYERR 0x010000   /* Internal error while scanning catalog : matched twice in catalog and cannot delete one of them */
#define	WAITING_MIGR  0x020000   /* file being migrated and waiting confirmation (e.g. the real migration request) */
#define	WAITING_NS    0x040000   /* file waiting for an entry in the HSM name server */

#define	MSG_OUT		0
#define	MSG_ERR		1

#endif /* __stage_constants_h */
