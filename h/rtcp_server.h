/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * @(#)$RCSfile: rtcp_server.h,v $ $Revision: 1.2 $ $Date: 2004/09/06 14:35:56 $ CERN IT/ADC Olof Barring
 */

/*
 * rtcp_server.h - rtcopy server specific definitions
 */

#if !defined(RTCP_SERVER_H)
#define RTCP_SERVER_H

/*
 * Data buffers
 */
typedef struct buffer_table {
    void *lock;                     /* Fast lock on flag */
    int flag;                       /* BUFFER_FULL or BUFFER_EMPTY semaphore */
    int nb_waiters;                 /* If > 0 we must broadcast condition */
    int end_of_tpfile;              /* TRUE if this is the last buffer
                                     * of a tape file */
    int last_buffer;                /* TRUE if we reached end of req. */
    int maxlength;                  /* Actual size of buffer */
    int length;                     /* Current size of buffer in this
                                     * request (given the blocksize) */
    int data_length;                /* Length of data <= length */
    int bufendp;                    /* Pointer to end of data (normally
                                     * == data_length except when several
                                     * small files are concatenated
                                     * within a single buffer).*/
    int nbrecs;                     /* U-format only: number of records
                                     * (==nb blocks) within this buffer
                                     */
    int *lrecl_table;               /* U-format only: array of record
                                     * lengths for each individual block */
    char *buffer;
} buffer_table_t;

/*
 * Processing control structure
 */
typedef struct processing_cntl {
    void *cntl_lock;                /* Lock two following elements */
    int nb_reserved_bufs;           /* Number of buffers reserved for
                                     * currently active disk IO threads.
                                     */
    int diskIOstarted;              /* Flag saying that previous disk
                                     * IO thread has started to fill
                                     * its first buffer. Thus it is safe
                                     * to start next one if there are
                                     * buffers free.
                                     */
    int diskIOfinished;             /* Flag saying that there are no
                                     * disk IO to be started. Note that
                                     * there may still be active disk IO
                                     * thread running. */
    int nb_diskIOactive;            /* Number of currently active disk IO
                                     * threads */
    int tapeIOstarted;              /* Flag saying that tape IO thread
                                     * has started. */
    int tapeIOfinished;             /* Flag saying that tape IO thread
                                     * finished. (dual with start flag) */
    void *ProcError_lock;           /* Global processing error lock */ 
    int ProcError;                  /* Processing error */
    void *ReqStatus_lock;           /* Separate lock on indiv. request
                                     * exceptions. Same lock is used for all 
                                     * request elements. */
    int ReqStatus;                  /* Request exception */
    void *DiskFileAppend_lock;      /* Exclusive lock on disk file if we
                                     * are concatenating data to it */
    int DiskFileAppend;             /* Semaphore (1 = locked) */
    void *TpPos_lock;               /* Exclusive lock on stgupdc tppos */
    int  TpPos;                     /* Semaphore (1 = locked) */
    void *requestMoreWork_lock;     /* Lock for extending request list */
    int requestMoreWork;            /* Semaphore (1 = locked) */
    int checkForMoreWork;           /* Flag indicating that client intends
                                     * to send more work (1 = yes)
                                     */
    int tapeIOthreadID;             /* As it says: tape I/O thread ID */
} processing_cntl_t;

/*
 * Old (SHIFT) clients structures
 */
typedef struct tpqueue_info {
    int status;
    int nb_units;
    int nb_used;
    int nb_queued;
} tpqueue_info_t;

typedef struct shift_client {
    char name[20];
    char orig_name[20];
    char dgn[CA_MAXDGNLEN+1];
    char clienthost[CA_MAXHOSTNAMELEN+1];
    char acctstr[7];
    tpqueue_info_t info;
    int uid;
    int orig_uid;
    int gid;
    int orig_gid;
    int islocal;
    int isoutofsite;
    tape_list_t *tape;
} shift_client_t;

/*
 * Health and processing heartbeat monitoring structure
 */
typedef struct diskIOstatus {
    int pool_index;                  /* Index in thread pool */
    int disk_fseq;                   /* Disk file sequence number */
    int current_activity;            /* RTCP_PS_DK_OPEN, ... see below */ 
    u_signed64 nbbytes;              /* Nb bytes transferred host<->disk */
} diskIOstatus_t;
typedef struct tapeIOstatus {
    int current_activity;            /* RTCP_PS_TP_MOUNT, ... see below */
    int tape_fseq;                   /* Tape file sequence number */
    u_signed64 nbbytes;              /* Nb bytes transferred tape<->host */
} tapeIOstatus_t;
typedef struct processing_status {
    time_t timestamp;
    int nb_diskIO;                   /* Number of entries in following array */
    tapeIOstatus_t tapeIOstatus;
    diskIOstatus_t *diskIOstatus;
} processing_status_t;

/*
 * Processing status (PS) constants. 
 */
#define RTCP_PS_NOBLOCKING           0x00  /* Not in blocking call */
#define RTCP_PS_RESERVE              0x01  /* Tape IO specific */
#define RTCP_PS_MOUNT                0x02  /* Tape IO specific */
#define RTCP_PS_POSITION             0x03  /* Tape IO specific */
#define RTCP_PS_INFO                 0x04  /* Tape IO specific */
#define RTCP_PS_KILL                 0x05  /* Tape IO specific */
#define RTCP_PS_RELEASE              0x06  /* Tape IO specific */
#define RTCP_PS_WAITMTX              0x07
#define RTCP_PS_WAITCOND             0x08
#define RTCP_PS_OPEN                 0x09
#define RTCP_PS_READ                 0x0A
#define RTCP_PS_WRITE                0x0B
#define RTCP_PS_CLOSE                0x0C
#define RTCP_PS_CLOSERR              0x0D  /* Tape IO specific */
#define RTCP_PS_STAGEUPDC            0x0E
#define RTCP_PS_FINISHED             0x0F  /* Processing has finished */
#define PROC_STATUS_STRINGS {"NOBLOCKING","RESERVE","MOUNT", \
    "POSITION","INFO","KILL","RELEASE","WAITMTX","WAITCOND", \
    "OPEN","READ","WRITE","CLOSE","CLOSERR","STAGEUPDC","FINISHED",""} 
/*
 * Macro to get configured processing time limits (in seconds),
 * e.g. PS_LIMIT("RTCOPYD","MOUNT_TIME",max_mount_time);
 * Defaults are given below.
 */
#define PS_LIMIT(X,Y,Z) {char *__p; (Z)=-1; \
    if ( ((__p = getenv(#X "_" #Y)) != NULL) || \
         ((__p = getconfent(#X,#Y,0)) != NULL) ) Z = atoi(__p); \
    else Z = X##_##Y;}

#define RTCOPYD_MOUNT_TIME          60*60
#define RTCOPYD_POSITION_TIME       60*60
#define RTCOPYD_OPEN_TIME            5*60
#define RTCOPYD_READ_TIME            5*60
#define RTCOPYD_WRITE_TIME           5*60
#define RTCOPYD_CLOSE_TIME           5*60
/*
 * The time limit for stageupdc() need to be large since several (SHIFT)
 * stageupdc() calls may be piled up due to serialization.
 */
#define RTCOPYD_STAGEUPDC_TIME      60*60

/*
 * Time limit before exit() is called after an attempt to stop the process
 * cleanly.
 */
#define RTCP_KILL_TIMEOUT           10*60

/*
 * Internal prototypes
 */
extern char rtcp_cmds[][10]; /* Hold constants "CPDSKTP" and "CPTPDSK".*/

int rtcpd_InitNW  _PROTO((SOCKET **));
int rtcp_CleanUp  _PROTO((SOCKET **, int));
#if !defined(_AIX) || defined(__STDC__)
void rtcpd_AppendClientMsg  _PROTO((tape_list_t *, file_list_t *, char *, ...));
#endif /* aix */
int rtcp_RecvReq  _PROTO((SOCKET *, rtcpHdr_t *, rtcpClientInfo_t *, 
                 rtcpTapeRequest_t *, rtcpFileRequest_t *));
int rtcp_SendReq  _PROTO((SOCKET *, rtcpHdr_t *, rtcpClientInfo_t *, 
                 rtcpTapeRequest_t *, rtcpFileRequest_t *));
int rtcp_RecvAckn  _PROTO((SOCKET *, int));
int rtcp_SendAckn  _PROTO((SOCKET *, int));
int rtcp_Listen  _PROTO((SOCKET, SOCKET *, int, int));
int rtcpd_ClientListen  _PROTO((SOCKET));
int rtcp_ClientMsg _PROTO((SOCKET *, char *));
int rtcp_RunOld  _PROTO((SOCKET *, rtcpHdr_t *));
int rtcp_SendOldCAckn _PROTO((SOCKET *, rtcpHdr_t *));
int rtcp_SendOldCinfo _PROTO((SOCKET *, rtcpHdr_t *, shift_client_t *));
int rtcp_CloseConnection  _PROTO((SOCKET *));
int rtcpd_ConnectToClient  _PROTO((SOCKET *, char *, int *));
int rtcpd_MainCntl  _PROTO((SOCKET *));
int rtcpd_CheckClient _PROTO((int, int, char *, char *, int *));
int rtcpd_StartMonitor _PROTO((int));
int rtcpd_InitDiskIO  _PROTO((int *));
int rtcpd_CleanUpDiskIO  _PROTO((int, int));
int rtcpd_StartTapeIO  _PROTO((rtcpClientInfo_t *, tape_list_t *));
int rtcpd_StartDiskIO  _PROTO((rtcpClientInfo_t *, tape_list_t *, file_list_t *, int, int));
int rtcpd_CheckProcError  _PROTO((void));
void rtcpd_SetProcError  _PROTO((int));
void rtcpd_SetReqStatus  _PROTO((tape_list_t *, file_list_t *, int, int));
void rtcpd_CheckReqStatus  _PROTO((tape_list_t *, file_list_t *, int *, int *));
int rtcpd_CalcBufSz  _PROTO((tape_list_t *, file_list_t *));
int rtcpd_VarToFix _PROTO((char *, char *, int, int, int, int *, int *, void **));
int rtcpd_FixToVar _PROTO((char *, char *, int, int));
int rtcpd_f77RecToFix _PROTO((char *, int, int, char *, void **));
int rtcpd_AdmUformatInfo _PROTO((file_list_t *, int));
int rtcpd_WaitTapeIO  _PROTO((int *));
int rtcpd_WaitCLThread  _PROTO((int, int *));
int rtcpd_CtapeInit _PROTO((void));
int rtcpd_CtapeFree _PROTO((void));
int rtcpd_Assign  _PROTO((tape_list_t *));
int rtcpd_Deassign  _PROTO((int, rtcpTapeRequest_t *, rtcpClientInfo_t *));
char *rtcpd_CtapeErrMsg  _PROTO((void));
char *rtcpd_GetCtapeErrBuf _PROTO((void));
void rtcpd_CtapeKill _PROTO((void));
int rtcpd_Reserve  _PROTO((tape_list_t *));
int rtcpd_Mount  _PROTO((tape_list_t *));
int rtcpd_Position  _PROTO((tape_list_t *, file_list_t *));
int rtcpd_Release  _PROTO((tape_list_t *, file_list_t *));
int rtcpd_Info  _PROTO((tape_list_t *, file_list_t *));
int rtcpd_drvinfo  _PROTO((tape_list_t *));
int rtcpd_DmpInit _PROTO((tape_list_t *));
int rtcpd_DmpFile _PROTO((tape_list_t *, file_list_t *));
int rtcpd_DmpEnd _PROTO((void));
int rtcpd_stageupdc  _PROTO((tape_list_t *, file_list_t *));
int rtcp_CheckReq  _PROTO((SOCKET *, rtcpClientInfo_t *, tape_list_t *));
int rtcpd_CheckFileReq _PROTO((file_list_t *));
int rtcp_CheckReqStructures _PROTO((SOCKET *, rtcpClientInfo_t *, tape_list_t *));
int rtcp_CallTMS  _PROTO((tape_list_t *, char *));
int tellClient  _PROTO((SOCKET *, tape_list_t *, file_list_t *, int));
int topen  _PROTO((tape_list_t *, file_list_t *));
int tclose  _PROTO((int, tape_list_t *, file_list_t *));
int tcloserr  _PROTO((int, tape_list_t *, file_list_t *));
int twrite  _PROTO((int, char *, int, tape_list_t *, file_list_t *));
int tread  _PROTO((int, char *, int, tape_list_t *, file_list_t *));
int rtcpd_WaitForPosition _PROTO((tape_list_t *, file_list_t *));
int rtcpd_SignalFilePositioned _PROTO((tape_list_t *, file_list_t *));
int rtcpd_tpdump _PROTO((rtcpClientInfo_t *, tape_list_t *));
int rtcp_SendTpDump _PROTO((SOCKET *, rtcpDumpTapeRequest_t *));
int rtcp_RecvTpDump _PROTO((SOCKET *, rtcpDumpTapeRequest_t *));
int rtcp_GetMsg _PROTO((SOCKET *, char *, int));
int rtcp_WriteAccountRecord _PROTO((rtcpClientInfo_t *, tape_list_t *, file_list_t *, int));
int rtcpd_tpdump _PROTO((rtcpClientInfo_t *, tape_list_t *));
int rtcp_CheckConnect _PROTO((SOCKET *, tape_list_t *));
int rtcpd_init_stgupdc _PROTO((tape_list_t *));
int rtcpd_GetRequestList _PROTO((SOCKET *, rtcpClientInfo_t *,
                                rtcpTapeRequest_t *, tape_list_t **,
                                file_list_t *));
void rtcpd_BroadcastException _PROTO((void));
int rtcpd_FreeBuffers _PROTO(());
int rtcpd_WaitCompletion _PROTO((tape_list_t *, file_list_t *));
int rtcpd_checkMoreWork _PROTO((SOCKET *, tape_list_t *, file_list_t *));
int rtcpd_waitMoreWork _PROTO((file_list_t *fl));

#if defined(CTAPE_DUMMIES)
int  DLL_DECL  stage_setlog _PROTO((void (*) _PROTO((int, char *))));
/*
int  DLL_DECL  stage_updc_filcp _PROTO((char *, int, int, char *, u_signed64, int, int, int, char *, char *, int, int, char *, char *));
int  DLL_DECL  stage_updc_tppos _PROTO((char *, int, int, int, char *, char *, int, int, char *, char *));
*/
#endif /* CTAPE_DUMMIES */

#ifdef MONITOR

#define CMONIT_FTRANSFER_SENDMSG_PERIOD 3 /* In seconds */


EXTERN_C int DLL_DECL Cmonit_send_transfer_info _PROTO((int, 
							u_signed64, 
							u_signed64,
							u_signed64, 
							int, int));

EXTERN_C int DLL_DECL Cmonit_send_rtcopy_status _PROTO ((int,
							 uid_t,
							 gid_t,
							 int,
							 int,
							 char,
							 char  *,
							 char  *,
							 int,
							 int,
							 int,
							 char  *,
							 char  *,
							 int,
							 char  *,
							 char *));


#endif /* MONITOR */

#endif /* RTCP_SERVER_H */
