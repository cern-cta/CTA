/*
 * $Id: stage_constants.h,v 1.32 2002/09/30 16:53:43 jdurand Exp $
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
#include "stage_limits.h"

/* =================== */
/* stage request types */
/* =================== */
#ifdef STAGEIN
#undef STAGEIN
#endif
#define STAGEIN 1

#ifdef STAGEOUT
#undef STAGEOUT
#endif
#define STAGEOUT 2

#ifdef STAGEWRT
#undef STAGEWRT
#endif
#define STAGEWRT 3

#ifdef STAGEPUT
#undef STAGEPUT
#endif
#define STAGEPUT 4

#ifdef STAGEQRY
#undef STAGEQRY
#endif
#define STAGEQRY 5

#ifdef STAGECLR
#undef STAGECLR
#endif
#define STAGECLR 6

#ifdef STAGEKILL
#undef STAGEKILL
#endif
#define	STAGEKILL 7

#ifdef STAGEUPDC
#undef STAGEUPDC
#endif
#define	STAGEUPDC 8

#ifdef STAGEINIT
#undef STAGEINIT
#endif
#define	STAGEINIT 9

#ifdef STAGECAT
#undef STAGECAT
#endif
#define	STAGECAT 10

#ifdef STAGEALLOC
#undef STAGEALLOC
#endif
#define	STAGEALLOC 11

#ifdef STAGEGET
#undef STAGEGET
#endif
#define	STAGEGET 12

#ifdef STAGEMIGPOOL
#undef STAGEMIGPOOL
#endif
#define	STAGEMIGPOOL 13

#ifdef STAGEFILCHG
#undef STAGEFILCHG
#endif
#define STAGEFILCHG 14

#ifdef STAGESHUTDOWN
#undef STAGESHUTDOWN
#endif
#define STAGESHUTDOWN 15

#ifdef STAGEPING
#undef STAGEPING
#endif
#define STAGEPING 16

/* ================= */
/* stage reply types */
/* ================= */
#ifdef RTCOPY_OUT
#undef RTCOPY_OUT
#endif
#define	RTCOPY_OUT 2

#ifdef STAGERC
#undef STAGERC
#endif
#define	STAGERC 3

#ifdef SYMLINK
#undef SYMLINK
#endif
#define	SYMLINK 4

#ifdef RMSYMLINK
#undef RMSYMLINK
#endif
#define	RMSYMLINK 5

#ifdef API_STCP_OUT
#undef API_STCP_OUT
#endif
#define API_STCP_OUT 6

#ifdef API_STPP_OUT
#undef API_STPP_OUT
#endif
#define API_STPP_OUT 7

#ifdef UNIQUEID
#undef UNIQUEID
#endif
#define UNIQUEID 8

/* ======================== */
/* stage status bit values  */
/* ======================== */
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

#ifdef  STAGE_RDONLY
#undef  STAGE_RDONLY
#endif
#define	STAGE_RDONLY  0x100000   /* O_RDONLY file waiting for another being migrated */

/* ======================== */
/* stage status bit values  */
/* ======================== */
#ifdef  STAGE_AUTO_TPPOOL
#undef  STAGE_AUTO_TPPOOL
#endif
#define STAGE_AUTO_TPPOOL 1      /* Says if this castor file got a tape pool automatically or explicitely */

/* =================== */
/* stage message types */
/* =================== */
#ifdef  MSG_OUT
#undef  MSG_OUT
#endif
#define	MSG_OUT		0

#ifdef  MSG_ERR
#undef  MSG_ERR
#endif
#define	MSG_ERR		1

/* ================================================================= */
/* stage daemon return codes and states for the command-line clients */
/* ================================================================= */
#ifdef  USERR
#undef  USERR
#endif
#define	USERR	  1	/* user error - serrno: SEUSERR */

#ifdef  SYERR
#undef  SYERR
#endif
#define	SYERR 	  2	/* system error - serrno: SESYSERR */

#ifdef  CONFERR
#undef  CONFERR
#endif
#define	CONFERR	  4	/* configuration error - serrno: ESTCONF */

#ifdef SHIFT_ECUPVNACT
#undef SHIFT_ECUPVNACT
#endif
#define SHIFT_ECUPVNACT 187 /* serrno: ECUPVNACT */

#ifdef  ETHELDERR
#undef  ETHELDERR
#endif
#define	ETHELDERR	188	/* Tape is held - serrno: ETHELD */

#ifdef  LNKNSUP
#undef  LNKNSUP
#endif
#define	LNKNSUP	189	/* symbolic links not supported on that platform - serrno: ESTLNKNSUP */

#ifdef  CLEARED
#undef  CLEARED
#endif
#define	CLEARED	192	/* aborted by stageclr - serrno: ESTCLEARED */

#ifdef  BLKSKPD
#undef  BLKSKPD
#endif
#define	BLKSKPD	193	/* blocks were skipped - serrno: ERTBLKSKPD */

#ifdef  TPE_LSZ
#undef  TPE_LSZ
#endif
#define	TPE_LSZ	194	/* blocks were skipped, stageing limited by size - serrno: ERTTPE_LSZ */

#ifdef  MNYPARI
#undef  MNYPARI
#endif
#define	MNYPARI	195	/* stagein stopped: too many tape errors, but -E keep - serrno: ERTMNYPARY */

#ifdef  REQKILD
#undef  REQKILD
#endif
#define	REQKILD	196	/* request killed by user - serrno: ESTKILLED */

#ifdef  LIMBYSZ
#undef  LIMBYSZ
#endif
#define	LIMBYSZ	197	/* limited by size - serrno: ERTLIMBYSZ */

#ifdef SHIFT_ESTNACT
#undef SHIFT_ESTNACT
#endif
#define SHIFT_ESTNACT 198 /* Old SHIFT value when nomorestage - serrno: ESTNACT */

#ifdef  ENOUGHF
#undef  ENOUGHF
#endif
#define	ENOUGHF	199	/* enough free space - not mapped to serrno, this is an internal flag */

/* ======================= */
/* stage generic constants */
/* ======================= */
#ifdef STGMAGIC
#undef STGMAGIC
#endif
#define STGMAGIC 0x13140701

#ifdef STGMAGIC2
#undef STGMAGIC2
#endif
#define STGMAGIC2 0x13140702

#ifdef STGMAGIC3
#undef STGMAGIC3
#endif
#define STGMAGIC3   0x13140703

#ifdef STGMAGIC4
#undef STGMAGIC4
#endif
#define STGMAGIC4   0x13140704

#ifdef STGDAEMON_LEVEL
#undef STGDAEMON_LEVEL
#endif
#define STGDAEMON_LEVEL STGMAGIC4

#ifdef ONE_SECOND
#undef ONE_SECOND
#endif
#define ONE_SECOND 1

#ifdef ONE_MINUTE
#undef ONE_MINUTE
#endif
#define ONE_MINUTE 60

#ifdef ONE_HOUR
#undef ONE_HOUR
#endif
#define ONE_HOUR 3600

#ifdef ONE_DAY
#undef ONE_DAY
#endif
#define ONE_DAY (ONE_HOUR * 24)

#ifdef ONE_YEAR
#undef ONE_YEAR
#endif
#define ONE_YEAR (ONE_DAY * 365)

#ifdef RETRYI
#undef RETRYI
#endif
#define	RETRYI	60

#ifdef STGTIMEOUT
#undef STGTIMEOUT
#endif
#define STGTIMEOUT 10

#ifdef DEFDGN
#undef DEFDGN
#endif
#define DEFDGN "CART" /* default device group name */

#ifdef MAXRETRY
#undef MAXRETRY
#endif
#define MAXRETRY 5

#ifdef PRTBUFSZ
#undef PRTBUFSZ
#endif
#define PRTBUFSZ 1024

#ifdef REPBUFSZ
#undef REPBUFSZ
#endif
#define REPBUFSZ  512	/* must be >= max stage daemon reply size */

#ifdef REQBUFSZ
#undef REQBUFSZ
#endif
#define REQBUFSZ 20000	/* must be >= max stage daemon request size */

#ifdef CHECKI
#undef CHECKI
#endif
#define CHECKI	10	/* max interval to check for work to be done */

#ifdef STAGE_INPUT_MODE
#undef STAGE_INPUT_MODE
#endif
#define STAGE_INPUT_MODE 0

#ifdef STAGE_OUTPUT_MODE
#undef STAGE_OUTPUT_MODE
#endif
#define STAGE_OUTPUT_MODE 1

/* ============================================================================================ */
/* =========================== DEFINITION OF API FLAGS ======================================== */
/* ============================================================================================ */
#define STAGE_DEFERRED          CONSTLL(0x0000000000001)  /* -A deferred  [stage_iowc]           */
#define STAGE_GRPUSER           CONSTLL(0x0000000000002)  /* -G           [stage_iowc,stage_qry] */
#define STAGE_COFF              CONSTLL(0x0000000000004)  /* -c off       [stage_iowc]           */
#define STAGE_UFUN              CONSTLL(0x0000000000008)  /* -U           [stage_iowc]           */
#define STAGE_INFO              CONSTLL(0x0000000000010)  /* -z           [stage_iowc]           */
#define STAGE_ALL               CONSTLL(0x0000000000020)  /* -a           [stage_qry]            */
#define STAGE_LINKNAME          CONSTLL(0x0000000000040)  /* -L           [stage_qry,stage_clr]  */
#define STAGE_LONG              CONSTLL(0x0000000000080)  /* -l           [stage_qry]            */
#define STAGE_PATHNAME          CONSTLL(0x0000000000100)  /* -P           [stage_qry,stage_clr]  */
#define STAGE_SORTED            CONSTLL(0x0000000000200)  /* -S           [stage_qry]            */
#define STAGE_STATPOOL          CONSTLL(0x0000000000400)  /* -s           [stage_qry]            */
#define STAGE_TAPEINFO          CONSTLL(0x0000000000800)  /* -T           [stage_qry]            */
#define STAGE_USER              CONSTLL(0x0000000001000)  /* -u           [stage_qry]            */
#define STAGE_EXTENDED          CONSTLL(0x0000000002000)  /* -x           [stage_qry]            */
#define STAGE_ALLOCED           CONSTLL(0x0000000004000)  /* -A           [stage_qry]            */
#define STAGE_FILENAME          CONSTLL(0x0000000008000)  /* -f           [stage_qry]            */
#define STAGE_EXTERNAL          CONSTLL(0x0000000010000)  /* -I           [stage_qry]            */
#define STAGE_MULTIFSEQ         CONSTLL(0x0000000020000)  /* -Q           [stage_qry]            */
#define STAGE_MIGRULES          CONSTLL(0x0000000040000)  /* --migrator   [stage_qry]            */
#define STAGE_SILENT            CONSTLL(0x0000000080000)  /* --silent     [stage_iowc]           */
#define STAGE_NOWAIT            CONSTLL(0x0000000100000)  /* --nowait     [stage_iowc]           */
#define STAGE_NOREGEXP          CONSTLL(0x0000000200000)  /* --noregexp   [stage_qry]            */
#define STAGE_DUMP              CONSTLL(0x0000000400000)  /* --dump       [stage_qry]            */
#define STAGE_CLASS             CONSTLL(0x0000000800000)  /* --fileclass  [stage_qry]            */
#define STAGE_QUEUE             CONSTLL(0x0000001000000)  /* --queue      [stage_qry]            */
#define STAGE_COUNTERS          CONSTLL(0x0000002000000)  /* --counters   [stage_qry]            */
#define STAGE_NOHSMCREAT        CONSTLL(0x0000004000000)  /* --nohsmcreat [stage_iowc]           */
#define STAGE_CONDITIONAL       CONSTLL(0x0000008000000)  /* -c           [stage_clr]            */
#define STAGE_FORCE             CONSTLL(0x0000010000000)  /* -F           [stage_clr]            */
#define STAGE_REMOVEHSM         CONSTLL(0x0000020000000)  /* -remove_from_hsm [stage_clr]        */
#define STAGE_RETENP            CONSTLL(0x0000040000000)  /* --retenp     [stage_qry]            */
#define STAGE_MINTIME           CONSTLL(0x0000100000000)  /* --mintime    [stage_qry]            */
#define STAGE_VERBOSE           CONSTLL(0x0000200000000)  /* --verbose    [stage_ping]           */
#define STAGE_DISPLAY_SIDE      CONSTLL(0x0000400000000)  /* --display_side [stage_qry]          */
#define STAGE_SIDE              CONSTLL(0x0000800000000)  /* --side       [stage_qry,stage_iowc] */
#define STAGE_FILE_ROPEN        CONSTLL(0x0001000000000)  /* -o           [stage_updc]           */
#define STAGE_FILE_RCLOSE       CONSTLL(0x0002000000000)  /* -c           [stage_updc]           */
#define STAGE_FILE_WOPEN        CONSTLL(0x0004000000000)  /* -O           [stage_updc]           */
#define STAGE_FILE_WCLOSE       CONSTLL(0x0008000000000)  /* -C           [stage_updc]           */
#define STAGE_REQID             CONSTLL(0x0010000000000)  /* --reqid      [stage_clr,stage_qry]  */
#define STAGE_HSM_ENOENT_OK     CONSTLL(0x0020000000000)  /* --enoent_ok  [stage_wrt]            */
#define STAGE_NOLINKCHECK       CONSTLL(0x0040000000000)  /* --nolinkcheck [stage_iowc,stage_updc,stage_clr] */
#define STAGE_MIGLOG            CONSTLL(0x0080000000000)  /* internal flag for the migrator - will log in mig_log */
#define STAGE_NORETRY           CONSTLL(0x0100000000000)  /* --noretry  [stage_iowc]             */
#define STAGE_VOLATILE_TPPOOL   CONSTLL(0x0200000000000)  /* --volatile_tppool  [stage_iowc]     */
#define STAGE_NODISK            CONSTLL(0x0400000000000)  /* --nodisk  [stage_clr]               */
#define STAGE_MIGRINIT          CONSTLL(0x1000000000000)  /* -X  [stage_init]                    */
#define STAGE_HSMCREAT          CONSTLL(0x2000000000000)  /* --hsmcreat [stage_chng]             */
#define STAGE_FORMAT            CONSTLL(0x4000000000000)  /* --format [stageqry - hint for stgdaemon]  */

/* ======================================================================= */
/* =================== DEFINITION OF API METHODS ========================= */
/* ======================================================================= */
#define STAGE_00         100
#define STAGE_IN         101
#define STAGE_OUT        102
#define STAGE_WRT        103
#define STAGE_PUT        104           /* Not yet supported */
#define STAGE_QRY        105
#define STAGE_CLR        106
#define	STAGE_KILL       107
#define	STAGE_UPDC       108
#define	STAGE_INIT       109
#define	STAGE_CAT        110
#define	STAGE_ALLOC      111
#define	STAGE_GET        112
#define STAGE_FILCHG     114           /* Not yet supported */
#define	STAGE_SHUTDOWN   115
#define	STAGE_PING       116

#endif /* __stage_constants_h */
