/*
 * $Id: stage_constants.h,v 1.14 2002/01/22 07:36:24 jdurand Exp $
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

#ifdef MAX_NETDATA_SIZE
#undef MAX_NETDATA_SIZE
#endif
#define MAX_NETDATA_SIZE ONE_MB

#ifdef ROOTUIDLIMIT
#undef ROOTUIDLIMIT
#endif
#define ROOTUIDLIMIT 100

#ifdef ROOTGIDLIMIT
#undef ROOTGIDLIMIT
#endif
#define ROOTGIDLIMIT 100

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
#define	STAGE_RDONLY  0x100000   /* O_RDONLY file waiting for another being migrated */

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
#undef  UNERR
#endif
#define	UNERR	  3	/* undefined error */

#ifdef  CONFERR
#undef  CONFERR
#endif
#define	CONFERR	  4	/* configuration error */

#ifdef  ETHELDERR
#undef  ETHELDERR
#endif
#define	ETHELDERR	188	/* Tape is held */

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

#ifdef SHIFT_ESTNACT
#undef SHIFT_ESTNACT
#endif
#define SHIFT_ESTNACT 198 /* Old SHIFT value when nomorestage - remapped in send2stgd */

#ifdef  ENOUGHF
#undef  ENOUGHF
#endif
#define	ENOUGHF	199	/* enough free space */

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

#ifdef STAGER_SIDE_SERVER_SUPPORT
#ifdef STGMAGIC4
#undef STGMAGIC4
#endif
#define STGMAGIC4   0x13140704
#endif

#ifdef ONE_SECOND
#undef ONE_SECOND
#endif
#define ONE_SECOND 1

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

/* ==================================================================================== */
/* =========================== DEFINITION OF API FLAGS ================================ */
/* ==================================================================================== */
#define STAGE_DEFERRED          0x00000000001LL  /* -A deferred  [stage_iowc]           */
#define STAGE_GRPUSER           0x00000000002LL  /* -G           [stage_iowc,stage_qry] */
#define STAGE_COFF              0x00000000004LL  /* -c off       [stage_iowc]           */
#define STAGE_UFUN              0x00000000008LL  /* -U           [stage_iowc]           */
#define STAGE_INFO              0x00000000010LL  /* -z           [stage_iowc]           */
#define STAGE_ALL               0x00000000020LL  /* -a           [stage_qry]            */
#define STAGE_LINKNAME          0x00000000040LL  /* -L           [stage_qry,stage_clr]  */
#define STAGE_LONG              0x00000000080LL  /* -l           [stage_qry]            */
#define STAGE_PATHNAME          0x00000000100LL  /* -P           [stage_qry,stage_clr]  */
#define STAGE_SORTED            0x00000000200LL  /* -S           [stage_qry]            */
#define STAGE_STATPOOL          0x00000000400LL  /* -s           [stage_qry]            */
#define STAGE_TAPEINFO          0x00000000800LL  /* -T           [stage_qry]            */
#define STAGE_USER              0x00000001000LL  /* -u           [stage_qry]            */
#define STAGE_EXTENDED          0x00000002000LL  /* -x           [stage_qry]            */
#define STAGE_ALLOCED           0x00000004000LL  /* -A           [stage_qry]            */
#define STAGE_FILENAME          0x00000008000LL  /* -f           [stage_qry]            */
#define STAGE_EXTERNAL          0x00000010000LL  /* -I           [stage_qry]            */
#define STAGE_MULTIFSEQ         0x00000020000LL  /* -Q           [stage_qry]            */
#define STAGE_MIGRULES          0x00000040000LL  /* --migrator   [stage_qry]            */
#define STAGE_SILENT            0x00000080000LL  /* --silent     [stage_iowc]           */
#define STAGE_NOWAIT            0x00000100000LL  /* --nowait     [stage_iowc]           */
#define STAGE_NOREGEXP          0x00000200000LL  /* --noregexp   [stage_qry]            */
#define STAGE_DUMP              0x00000400000LL  /* --dump       [stage_qry]            */
#define STAGE_CLASS             0x00000800000LL  /* --fileclass  [stage_qry]            */
#define STAGE_QUEUE             0x00001000000LL  /* --queue      [stage_qry]            */
#define STAGE_COUNTERS          0x00002000000LL  /* --counters   [stage_qry]            */
#define STAGE_NOHSMCREAT        0x00004000000LL  /* --nohsmcreat [stage_iowc]           */
#define STAGE_CONDITIONAL       0x00008000000LL  /* -c           [stage_clr]            */
#define STAGE_FORCE             0x00010000000LL  /* -F           [stage_clr]            */
#define STAGE_REMOVEHSM         0x00020000000LL  /* -remove_from_hsm [stage_clr]        */
#define STAGE_RETENP            0x00040000000LL  /* --retenp     [stage_qry]            */
#define STAGE_MINTIME           0x00100000000LL  /* --mintime    [stage_qry]            */
#define STAGE_VERBOSE           0x00200000000LL  /* --verbose    [stage_ping]           */
#define STAGE_FORCE_SIDE_FORMAT 0x00400000000LL  /* --force_side_format [stage_qry]     */
#define STAGE_SIDE              0x00800000000LL  /* --side       [stage_qry]            */
#define STAGE_FILE_ROPEN        0x01000000000LL  /* -o           [stage_updc]           */
#define STAGE_FILE_RCLOSE       0x02000000000LL  /* -c           [stage_updc]           */
#define STAGE_FILE_WOPEN        0x04000000000LL  /* -O           [stage_updc]           */
#define STAGE_FILE_WCLOSE       0x08000000000LL  /* -C           [stage_updc]           */
#define STAGE_REQID             0x10000000000LL  /* --reqid      [stage_clr,stage_qry]  */

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
#define	STAGE_INIT       109           /* Not yet supported */
#define	STAGE_CAT        110
#define	STAGE_ALLOC      111           /* Not yet supported */
#define	STAGE_GET        112           /* Not yet supported */
#define STAGE_FILCHG     114           /* Not yet supported */
#define	STAGE_SHUTDOWN   115           /* Not yet supported */
#define	STAGE_PING       116

#endif /* __stage_constants_h */
