/*
 * $Id: rtcp_constants.h,v 1.3 2005/03/15 15:40:51 obarring Exp $
 *
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * @(#)$RCSfile: rtcp_constants.h,v $ $Revision: 1.3 $ $Date: 2005/03/15 15:40:51 $ CERN IT/ADC Olof Barring
 */

/*
 * rtcp_constants.h - basic rtcopy constants and macros
 */

#if !defined(RTCP_CONSTANTS_H)
#define RTCP_CONSTANTS_H
/*
 * Key number for communication with rtcopy daemon
 */
#define RTCOPY_MAGIC_VERYOLD    (0x0666)    /* Stone age */
#define RTCOPY_MAGIC_SHIFT      (0x0667)    /* SHIFT client */
#define RTCOPY_MAGIC_OLD0       (0x0668)    /* Old CASTOR client */
#define RTCOPY_MAGIC            (0x0669)    /* New CASTOR client */
#define RFIO2TPREAD_MAGIC       (0X0110)
#define VALID_MAGIC(X) (X==RTCOPY_MAGIC || X== RTCOPY_MAGIC_OLD0 || X==RTCOPY_MAGIC_SHIFT || \
    X==RTCOPY_MAGIC_VERYOLD)

#define RTCP_CONNECT_TO_CLIENT  1
#define RTCP_CONNECT_TO_SERVER  2
#define RTCP_ACCEPT_FROM_CLIENT 3
#define RTCP_ACCEPT_FROM_SERVER 4

/*
 * If one is missing we have to define both
 */
#if !defined(TRUE) || !defined(FALSE)
#ifdef TRUE
#undef TRUE
#endif
#define TRUE (1)
#ifdef FALSE
#undef FALSE
#endif
#define FALSE (0)
#endif /* !TRUE || !FALSE */

/*
 * Request types
 */
#define   RTCP_BASE_REQTYPE     (0x2100)
#define   RTCP_REQ_MIN          (RTCP_BASE_REQTYPE)
#define   RTCP_TAPE_REQ         (RTCP_BASE_REQTYPE+0x01)
#define   RTCP_FILE_REQ         (RTCP_BASE_REQTYPE+0x02)
#define   RTCP_NOMORE_REQ       (RTCP_BASE_REQTYPE+0x03)
#define   RTCP_TAPEERR_REQ      (RTCP_BASE_REQTYPE+0x04)
#define   RTCP_FILEERR_REQ      (RTCP_BASE_REQTYPE+0x05)
#define   RTCP_ENDOF_REQ        (RTCP_BASE_REQTYPE+0x06)
#define   RTCP_ABORT_REQ        (RTCP_BASE_REQTYPE+0x07)
#define   RTCP_DUMP_REQ         (RTCP_BASE_REQTYPE+0x08) /* Admin. req. */
#define   RTCP_DUMPTAPE_REQ     (RTCP_BASE_REQTYPE+0x09)
#define   RTCP_KILLJID_REQ      (RTCP_BASE_REQTYPE+0x0A) /* Admin. req. */
#define   RTCP_RSLCT_REQ        (RTCP_BASE_REQTYPE+0x0B)
#define   RTCP_PING_REQ         (RTCP_BASE_REQTYPE+0x0C)
#define   RTCP_HAS_MORE_WORK    (RTCP_BASE_REQTYPE+0x0D) /* Client signals more work to a running request*/
#define   RTCP_REQ_MAX          (RTCP_BASE_REQTYPE+0x0E)

#define RTCP_VALID_REQTYPE(X) (X>RTCP_REQ_MIN && X<RTCP_REQ_MAX)

/*
 * File request processing status codes (proc_status field in
 * rtcpFileRequest_t).
 */
#define   RTCP_WAITING            (0x1)
#define   RTCP_POSITIONED         (0x2)
#define   RTCP_PARTIALLY_FINISHED (0x3)
#define   RTCP_FINISHED           (0x4)
#define   RTCP_EOV_HIT            (0x5) /* File partially on this volume */
#define   RTCP_UNREACHABLE        (0x6) /* File resides on another volume */
#define   RTCP_REQUEST_MORE_WORK  (0x7) /* Placeholder causing server to query client for more work */
#define   VALID_PROC_STATUS(X) ((X)==RTCP_WAITING || \
        (X)==RTCP_POSITIONED || (X) == RTCP_PARTIALLY_FINISHED || \
        (X)==RTCP_FINISHED || (X) == RTCP_EOV_HIT || \
        (X)==RTCP_REQUEST_MORE_WORK)

/*
 * Tape or file copy system error severity and responsability
 * (returned in rtcpErrMsg_t)
 */
#define   RTCP_OK            (0x000001)
#define   RTCP_RETRY_OK      (0x000002)
#define   RTCP_RESELECT_SERV (0x000004)
#define   RTCP_FAILED        (0x000008)
#define   RTCP_SEND_OPMSG    (0x000010)
#define   RTCP_USERR         (0x000020)
#define   RTCP_SYERR         (0x000040)
#define   RTCP_UNERR         (0x000080)
#define   RTCP_SEERR         (0x000100)
#define   RTCP_NORLS         (0x000200)   /* Don't call Ctape_rls() */
#define   RTCP_LOCAL_RETRY   (0x000400)   /* Do local retry */
#define   RTCP_NORETRY       (0x000800)   /* Don't retry at all */
#define   RTCP_EOD           (0x001000)   /* Reached End Of Data */
#define   RTCP_NEXTRECORD    (0x002000)   /* Skip to next record */
#define   RTCP_BLKSKPD       (0x004000)   /* A skip occured */
#define   RTCP_TPE_LSZ       (0x008000)   /* A skip occured and limited by size */
#define   RTCP_MNYPARY       (0x010000)   /* Request failed though  skip was attempted */
#define   RTCP_LIMBYSZ       (0x020000)   /* File limited by size */
#define   RTCP_ENDVOL        (0x040000)   /* End of volume hit and no more vols.*/

/*
 * Error action constants (rtcp_err_action field in rtcpFileRequest_t)
 */
#define SKIPBAD          (0x1)   /* Skip bad blocks */
#define KEEPFILE         (0x2)   /* Keep file in case of error */
#define VALID_ERRACT(X) ( ((X)->rtcp_err_action == 0 || \
     ((X)->rtcp_err_action & (SKIPBAD|KEEPFILE)) == (X)->rtcp_err_action) && \
                          ((X)->tp_err_action == 0 || \
     ((X)->tp_err_action & (IGNOREEOI|NOTRLCHK)) == (X)->tp_err_action) )

/*
 * Format conversion flags (convert field in rtcpFileRequest_t)
 */
#define EBCCONV          (0x1)   /* ebcdic <--> ascii conversion */
#define FIXVAR           (0x2)   /* fixed records <--> variable records */
#define ASCCONV          (0x4)   /* no conversion */
#define NOF77CW          (0x8)   /* No F77 control words */
#define VALID_CONVERT(X) ( (((X)->convert & (ASCCONV | EBCCONV | NOF77CW)) && \
    (((X)->convert & (ASCCONV | EBCCONV)) != (ASCCONV | EBCCONV))) && \
    (((X)->convert & (FIXVAR | NOF77CW)) != (FIXVAR  | NOF77CW)) )

/*
 * Concatenate tape file to disk file. The *_TO_EOD constants reflect
 * that there was a trailing "-" if rtcopy client was the tpread command.
 */
#define NOCONCAT         (0x01)   /* Open disk file with O_CREAT | O_TRUNC */
#define   CONCAT         (0x02)   /* Open disk file with O_APPEND but with valid tape_fseq */
/*
 * The following two are for tape read only:
 *   NOCONCAT_TO_EOD should be set if the client doesn't know the 
 *                   number of files on tape. The only difference 
 *                   with NOCONCAT is that if EOT is hit before the 
 *                   number of disk files has been reached it is not 
 *                   considered as an error if NOCONCAT_TO_EOD has 
 *                   been set.
 *  CONCAT_TO_EOD    should be set if the client doesn't know the number
 *                   of files on tape but he want all data up to EOT
 *                   to be concatenated to the specified disk file.
 *  OPEN_NOTRUNC     Prevents the rtcpd from opening disk file with O_TRUNC
 *                   flag when offset==0. This flag is needed by the new
 *                   stager for the support of multi-semgented file recalls
 */
#define NOCONCAT_TO_EOD  (0x04) /* As NOCONCAT but without checking tape_fseq */
#define   CONCAT_TO_EOD  (0x08) /* As CONCAT but without checking tape_fseq */
#define VOLUME_SPANNING  (0x10) /* Tape file may spann over several vols. */
#define OPEN_NOTRUNC     (0x20) /* Force always opening disk file without O_TRUNC */
#define MASK_VS(X) ((X)->concat & ~VOLUME_SPANNING)
#define VALID_CONCAT(X) ((MASK_VS(X) == NOCONCAT) || (MASK_VS(X) == CONCAT) || \
    (MASK_VS(X) == NOCONCAT_TO_EOD) || (MASK_VS(X) == CONCAT_TO_EOD) || \
    (MASK_VS(X) == (CONCAT|CONCAT_TO_EOD)) || (MASK_VS(X) == OPEN_NOTRUNC) )

/*
 * Special tape mode to request device queue info. rather than
 * read/write tape. Make sure it doesn't clash with WRITE_ENABLE or 
 * WRITE_DISABLE (x^2 + y^2 + 1 = x doesn't have any real solutions if
 * y i real....).
 */
#define RTCP_INFO_REQ (WRITE_ENABLE*WRITE_ENABLE+WRITE_DISABLE*WRITE_DISABLE+1)

/*
 * Internal (server) buffer semaphores
 */
#define BUFFER_EMPTY     (0)
#define BUFFER_FULL      (1)

/*
 * Special return code from rtcpd_stageupdc()
 */
#define NEWPATH          (1)

/*
 * Listen backlog
 */
#if !defined(RTCOPY_BACKLOG)
#if defined(FD_SETSIZE)
#define RTCOPY_BACKLOG (FD_SETSIZE)
#else /* FD_SETSIZE */
#define RTCOPY_BACKLOG (5)
#endif /* FD_SETSIZE */
#endif /* RTCOPY_BACKLOG */
/*
 * netread/netwrite timeouts
 */
#define RTCP_NETTIMEOUT (60)

/*
 * Some general constants
 */
#if !defined(RTCOPY_PORT)
#ifdef CSEC
#define RTCOPY_PORT        (5503)
#else
#define RTCOPY_PORT        (5003)
#endif /* CSEC */
#endif /* RTCOPY_PORT */

#define RTCP_MAX_REQUEST_LENGTH (8*1024*1024)
#define RTCOPY_PORT_DEBUG  (8889)
#define RTCP_HDRBUFSIZ     (3*LONGSIZE)
#define RTCP_MSGBUFSIZ     (4096)
#define VALID_MSGLEN(X) ((X)>0 && (X)<(RTCP_MSGBUFSIZ))

#define RTCP_CKSUM_ADLER32 "adler32"
#define RTCP_CKSUM_CRC32   "crc32"
#if !defined(RTCP_CKSUM_DEFAULT)
#define RTCP_CKSUM_DEFAULT RTCP_CKSUM_ADLER32
#endif /* !RTCP_CKSUM_DEFAULT */
#define RTCP_VALID_CKSUMS {RTCP_CKSUM_ADLER32, \
                           RTCP_CKSUM_CRC32, \
                           ""}

#if !defined(RTCOPY_LOGFILE)
#define RTCOPY_LOGFILE     ""
#endif /* RTCOPY_LOGFILE */

#if !defined(NB_RTCP_BUFS)
#define NB_RTCP_BUFS       (32)
#endif /* NB_RTCP_BUFS */

#if !defined(RTCP_BUFS)
#define RTCP_BUFSZ         (4 * 1024 * 1024)
#endif /* RTCP_BUFSZ */

#if !defined(RTCPC_PING_INTERVAL)
#define RTCPC_PING_INTERVAL (120)
#endif /* RTCPC_PING_INTERVAL */

#if !defined(RTCP_STGUPDC_RETRIES)
#define RTCP_STGUPDC_RETRIES (6)
#endif /* RTCP_STGUPDC_RETRIES */

#define MAXNBSKIPS         (10)
#define MAX_TPRETRY        (2)
#define MAX_CPRETRY        (2)

/*
 * Fortran unit table, for U-format only
 */
#define FORTRAN_UNIT_OFFSET (20)   /* start at some safe number...*/
#define NB_FORTRAN_UNITS    (50)   /* Table size  */
#define VALID_FORTRAN_UNIT(X) \
    ((X)>=FORTRAN_UNIT_OFFSET && \
     (X)<FORTRAN_UNIT_OFFSET+NB_FORTRAN_UNITS)

/*
 * Support for old VMS clients
 */
#define   RTCP_VMSOPTLEN        (255)

/*
 * SHIFT clients message text buffer size
 */
#define   RTCP_SHIFT_BUFSZ      (256)

/*
 * Backward compatible error message formats
 */

#define RTCOPY_CMD_STRINGS {"CPDTPDSK","CPDSKTP"}


			/* rtcopyd error messages */
 
#define	RT001	"%s [%d]: netread(): %s\n"
#define	RT002	"%s [%d]: rtcopy stopped by signal %d\n"
#define	RT003	"%s [%d]: rtcopy killed by signal %d\n"
#define	RT004	"%s [%d]: request aborted by user\n"
#define	RT005	"%s [%d]: fork() failed: %s\n"
 
			/* cpdsktp/cptpdsk error messages */
 
#define	RT101	" %s ! -qn OPTION INVALID FOR CPTPDSK\n"
#define	RT102	" %s ! ABORT BECAUSE INVALID OPTION\n"
#define	RT103	" %s ! BLOCK REQUESTED SMALLER THAN BLOCK ON TAPE (BLOCK # %d)\n"
#define	RT104	" %s ! BLOCK SIZE CANNOT BE EQUAL TO ZERO\n"
#define	RT105	" %s ! BUFFER ALLOCATION ERROR: %s\n"
#define	RT106	" %s ! DISK FILE FORMAT NOT UNFORMATTED SEQUENTIAL FORTRAN\n"
#define	RT107	" %s ! DISK FILE PATHNAMES MUST BE SPECIFIED\n"
#define	RT108	" %s ! ERROR CLOSING DISK FILE: %s\n"
#define	RT109	" %s ! ERROR GETTING DEVICE STATUS: %s\n"
#define	RT110	" %s ! ERROR OPENING DISK FILE: %s\n"
#define	RT111	" %s ! ERROR OPENING TAPE FILE: %s\n"
#define	RT112	" %s ! ERROR READING DISK FILE: %s\n"
#define	RT113	" %s ! ERROR READING FROM TAPE: %s (BLOCK # %d)\n"
#define	RT114	" %s ! ERROR SETTING LIST TAPE I/O: %s\n"
#define	RT115	" %s ! ERROR WRITING ON DISK: %s\n"
#define	RT116	" %s ! ERROR WRITING ON TAPE: %s (BLOCK # %d)\n"
#define	RT117	" %s ! ERROR WRITING ON TAPE: erreg = %d (BLOCK # %d)\n"
#define	RT118	" %s ! ERROR, BLANK CHECK (BLOCK # %d)\n"
#define	RT119	" %s ! ERROR, BLOCK TOO LARGE OR TOO SMALL (BLOCK # %d)\n"
#define	RT120	" %s ! ERROR, DISK FILE NAME MISSING\n"
#define	RT121	" %s ! ERROR, DISK FILES ARE EMPTY\n"
#define	RT122	" %s ! ERROR, NEXTVOL INTERRUPTED\n"
#define	RT123	" %s ! ERROR, RECORD LENGTH GREATER THAN SPECIFIED SIZE %d\n"
#define	RT124	" %s ! ERROR, TAPE VOLUMES OVERFLOW\n"
#define	RT125	" %s ! I/O ERROR READING FROM TAPE: %s (BLOCK # %d)\n"
#define	RT126	" %s ! I/O ERROR WRITING ON TAPE: %s (BLOCK # %d)\n"
#define	RT127	" %s ! INCORRECT NUMBER OF FILENAMES SPECIFIED\n"
#define	RT128	" %s ! INCORRECT OR MISSING TRAILER LABEL ON TAPE\n"
#define	RT129	" %s ! INVALID BLOCK SIZE SPECIFIED\n"
#define	RT130	" %s ! INVALID FORMAT SPECIFIED\n"
#define	RT131	" %s ! INVALID LABEL TYPE: %s\n"
#define	RT132	" %s ! INVALID MAX. SIZE OF FILE SPECIFIED\n"
#define	RT133	" %s ! INVALID NUMBER OF RECORDS SPECIFIED\n"
#define	RT134	" %s ! INVALID OPTION FOR -E: %s\n"
#define	RT135	" %s ! INVALID RECORD LENGTH SPECIFIED\n"
#define	RT136	" %s ! NETWORK ERROR DURING RFIO OPERATION\n"
#define	RT137	" %s ! PARITY ERROR OR BLANK TAPE (BLOCK # %d)\n"
#define	RT138	" %s ! RECORD LENGTH (%d) CAN'T BE GREATER THAN BLOCK SIZE (%d)\n"
#define	RT139	" %s ! RECORD LENGTH HAS TO BE SPECIFIED\n"
#define	RT140	" %s ! TAPE DEVICE NOT OPERATIONAL\n"
#define	RT141	" %s ! TAPE IS NOW INCORRECTLY TERMINATED\n"
#define	RT142	" %s ! THE FILE SEQUENCE LIST IS NOT VALID\n"
#define	RT143	" %s ! TRAILING MINUS IN -q SEQUENCE LIST IS INVALID FOR CPDSKTP\n"
#define	RT144	" %s ! VSN OR VID MUST BE SPECIFIED\n"
#define RT145  	" %s ! CANNOT TRANSFER DATA TO AN AFS BASED FILE\n"
#define RT146  	" %s ! INTERNAL ERROR ON STARTUP SIZES\n"
#define RT147  	" %s ! ALREADY APPENDED %d KB TO FILE, LIMIT IS %d MB !\n"
#define RT148	" %s ! INVALID BUFFER SIZE SPECIFIED (%d). CHECK BLOCK SIZE AND RECORD LENGTH \n"
#define RT149   " %s ! SPECIFIED lrecl %d DOES NOT CORRESPOND TO f77 lrecl %d\n"
#define RT150   " %s ! DISK FILE MODIFIED DURING REQUEST: %s\n"
#define RT151   " %s ! TAPE FILES MUST BE IN SEQUENCE\n"
#define RT152   " %s ! DISK FILE CANNOT BE A CASTOR HSM FILE\n"
#define RT153   " %s ! INVALID SETTING FOR CONCAT_TO_EOD FLAG\n"
#define RT154   " %s ! INVALID SETTING FOR NOCONCAT_TO_EOD FLAG\n"
 
			/* cpdsktp/cptpdsk info messages */
 
#if defined(_WIN32)
#define RT201   "\n %s - %I64 BYTES COPIED\n"
#define RT202   " %s - %I64 RECORDS COPIED\n"
#define RT207   " %s - MAX. NB OF RECORDS: %I64\n"
#elif defined(__osf__) && defined(__alpha)
#define RT201   "\n %s - %lu BYTES COPIED\n"
#define RT202   " %s - %lu RECORDS COPIED\n"
#define RT207   " %s - MAX. NB OF RECORDS: %lu\n"
#else
#define RT201   "\n %s - %llu BYTES COPIED\n"
#define RT202   " %s - %llu RECORDS COPIED\n"
#define RT207   " %s - MAX. NB OF RECORDS: %llu\n"
#endif

#define	RT203	" %s - APPEND MODE\n"
#define	RT204	" %s - BLOCK SIZE: %d\n"
#define	RT205	" %s - END OF FILE\n"
#define	RT206	" %s - END OF TRANSFER\n"
#define	RT208	" %s - MAX. SIZE OF FILE: %d MB\n"
#define	RT209	" %s - RECORD FORMAT: %c\n"
#define	RT210	" %s - RECORD LENGTH: %d\n"
#define	RT211	" %s - SKIPPING TO NEXT BLOCK\n"
#define RT212   " %s - FILE TRUNCATED\n"
#define RT213	" %s - CONVERTING DISK DATA FROM ASCII TO EBCDIC\n"
#define RT214	" %s - CONVERTING TAPE DATA FROM EBCDIC TO ASCII\n"
#define RT215	" %s - KBYTES SENT BY HOST      : %d\n"
#define RT216	" %s - KBYTES WRITTEN TO TAPE   : %d\n"
#define RT217	" %s - COMPRESSION RATE ON TAPE : %f\n\n"
#define RT218	" %s - KBYTES RECEIVED BY HOST  : %d\n"
#define RT219	" %s - KBYTES READ BY TAPE DRIVE: %d\n"
#define RT220	" %s - MAXIMUM FILE SIZE IS BOUND BY (2GB -1) bytes\n"
#define RT221 	" %s - DATA TRANSFER BANDWIDTH (%d Kbytes through %s): %d KB/sec\n\n"
#define	RT222	" %s - %d RECORD(S) TRUNCATED\n"
#define RT223   " %s - MULTI VOLUME FILE! COMPRESSION DATA PER FILE SECTION\n"

/*
 * Backward compatibility. Support for old clients
 */
/*
 * "Old" request types
 */
#define RQST_TPDK       (0x1001)
#define ACKN_TPDK       (0x2001)
 
#define RQST_DKTP       (0x1002)
#define ACKN_DKTP       (0x2002)
 
#define RQST_PING       (0x1003)
#define ACKN_PING       (0x2003)
 
#define RQST_INFO       (0x1004)
#define GIVE_INFO       (0x2004)
 
#define GIVE_RESU       (0x1005)
 
#define GIVE_OUTP       (0x1006)
 
#define RQST_ABORT      (0x1007)
#define ACKN_ABORT      (0x2007)
 
#define RQST_DPTP       (0x1008)
#define ACKN_DPTP       (0x2008)

/*
 * Different possible answers to "old" RQST_INFO.
 */
#define IMPOSSIBLE              (-3)
#define CONNECTING              (-2)
#define SENDING                 (-1)
#define SYERROR                  (1)
#define NOTAUTH                  (2)
#define ALLDOWN                  (3)
#define NOTAVAIL                 (4)
#define AVAILABLE                (5)
#define PERMDENIED               (6)
#define UNKNOWNUID               (7)
#define UNKNOWNGID               (8)
#define UIDMISMATCH              (9)
#define HOSTNOTAUTH              (10)
#define HOSTMAINT                (11)
#define MUTEINFO                 (12)
#define HOSTNOTSUIT              (13)

/*
 * Error responsability 
 */
#define USERR   1       /* User                         */
#define SYERR   2       /* System                       */
#define UNERR   3       /* Undefined                    */
#define SEERR   4       /* Internal                     */
 
/*
 *  (Obsolete) Error responsability when -E option is invoked
 */
#define BLKSKPD 193     /* A skip occured  and request was sucessful */
#define TPE_LSZ 194     /* A skip occured and data sent is limited by -s */
#define MNYPARY 195     /* Request failed though  skip was attempted */
#define LIMBYSZ 197     /* Status when limited by -s option value */
#define ETMUSR  205     /* too many tape users */
#define RSLCT   222     /* Reselect requested  */
#define ENDVOL  213     /* Old (SHIFT) ETEOV flag, End Of Volume hit */

#endif /* RTCP_CONSTANTS_H */
