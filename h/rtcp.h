/*
 * $Id: rtcp.h,v 1.4 1999/12/09 13:46:20 jdurand Exp $
 */

#if !defined(RTCP_H)
#define RTCP_H

typedef struct rtcpHdr {
    int magic;
    int reqtype;
    int len;
} rtcpHdr_t;

/*
 * Client information
 */
typedef struct rtcpClientInfo {
    char clienthost[CA_MAXHOSTNAMELEN+1]; /* Client host name */
    int clientport;                       /* Client listen port */
    int VolReqID;                         /* VDQM volume request ID */
    int jobID;                            /* Local RTCOPY server job ID */
    int uid;
    int gid;
    int umask;
    int rfio_key;
} rtcpClientInfo_t;
#define RTCP_CLINFOLEN(X) (7*LONGSIZE + strlen(X->clienthost) + 1)

/*
 * Error reporting. 
 */
typedef struct rtcpErrMsg {
    char errmsgtxt[CA_MAXLINELEN+1];
    int severity;                      /* Defined in rtcp_constants.h */
    int errorcode;                     /* Defined in rtcp_constants.h */
    int max_tpretry;                   /* Nb. of retries left on mount/pos */
    int max_cpretry;                   /* Nb. of retries left on copy */
} rtcpErrMsg_t;
#define RTCP_ERRMSGLEN(X) (4*LONGSIZE + strlen(X->err.errmsgtxt) + 1)

typedef struct rtcpFileRequest {
    char file_path[CA_MAXPATHLEN+1];   /* Disk file path */
    char tape_path[CA_MAXPATHLEN+1];   /* Tape device file */
    char recfm[CA_MAXRECFMLEN+1];
    char fid[CA_MAXFIDLEN+1];          /* Tape file ID */
    char ifce[5];                      /* Network interface name */
    char stageID[CA_MAXSTGRIDLEN+1];   /* Stage request ID */
    char vmsopt[RTCP_VMSOPTLEN+1];

    int VolReqID;                      /* VDQM volume request ID */
    int jobID;                         /* Local RTCOPY server job ID */
    int position_method;               /* TPPOSIT_FSEQ, TPPOSIT_FID, 
                                        * TPPOSIT_EOI and TPPOSIT_BLKID 
                                        */

    int tape_fseq;                     /* If position_method == TPPOSIT_FSEQ */

    int disk_fseq;                     /* Disk file sequence number. 
                                        * This is the order number of
                                        * the current disk file in the
                                        * request. 
                                        */
    int tape_fsec;                     /* Tape file section number */

    int blocksize;                     /* Tape blocksize (bytes)*/
    int recordlength;                  /* Tape record length (bytes)*/
    int retention;                     /* File retention time */
    int def_alloc;                     /* 0 = no deferred allocation */

    /*
     * Flags
     */
    int rtcp_err_action;               /* SKIPBAD or KEEPFILE */
    int tp_err_action;                 /* NOTRLCHK and IGNOREEOI */
    int convert;                       /* EBCCONV, FIXVAR */
    int check_fid;                     /* CHECK_FILE, NEW_FILE */
    int concat;                        /* CONCAT, CONCAT_TO_EOD,
                                        * NOCONCAT or NOCONCAT_TO_EOD */
    /*
     * Intermediate processing status used mainly to checkpoint
     * in case of a retry of a partially successful request.
     */
    int proc_status;                   /* RTCP_WAITING, RTC_POSITIONED, 
                                        * RTCP_FINISHED */
    /*
     * Final return code and timing information
     */
    int cprc;                          /* Copy return status */
    int TStartPosition;                /* Start time for position to this file */
    int TEndPosition;                  /* End time for position to this file */
    int TStartTransferDisk;            /* Start time for transfer to/from disk */
    int TEndTransferDisk;              /* End time for transfer to/from disk */
    int TStartTransferTape;            /* Start time for tranfser to/from tape */
    int TEndTransferTape;              /* End time for tranfser to/from tape */
    int blockid;                       /* If position_method == TPPOSIT_BLKID */

    /*
     * 64 bit quantities
     */

    /*
     * Compression statistics (actual transfer size)
     */
    u_signed64 offset;                 /* Start offset in disk file */
    u_signed64 bytes_in;               /* Bytes from host (write) or
                                        * bytes from tape (read)
                                        */
    u_signed64 bytes_out;              /* Bytes to tape (write) or
                                        * bytes to host (read)
                                        */
    u_signed64 bytes_from_host;        /* Bytes written to tape including
                                        * the label.
                                        */
    u_signed64 nbrecs;                 /* Number of records copied */

    /*
     * Imposed size restrictions (input parameters)
     */
    u_signed64 maxnbrec;               /* Read only "maxnbrec" records */
    u_signed64 maxsize;                /* Copy only "maxsize" bytes */
    u_signed64 startsize;              /* Accumulated size */

    /*
     * Error reporting
     */
    rtcpErrMsg_t err;

} rtcpFileRequest_t;

#define RTCP_FILEREQLEN(X) (24*LONGSIZE + 8*QUADSIZE + \
        strlen(X->file_path) + strlen(X->tape_path) + \
        strlen(X->recfm) + strlen(X->fid) + strlen(X->ifce) + \
        strlen(X->stageID) + strlen(X->vmsopt) + 7)

typedef struct rtcpTapeRequest {
    char vid[CA_MAXVIDLEN+1];
    char vsn[CA_MAXVIDLEN+1];
    char label[4];
    char dgn[CA_MAXDGNLEN+1];
    char devtype[CA_MAXDVTLEN+1];
    char density[CA_MAXDENLEN+1];
    char server[CA_MAXHOSTNAMELEN+1];   /* For client use only */
    char unit[CA_MAXUNMLEN+1];
    int VolReqID;                       /* VDQM volume request ID */
    int jobID;                          /* Local RTCOPY server job ID */
    int mode;                           /* WRITE_DISABLE or WRITE_ENABLE */
    int start_file;                     /* Start file if mapped VID */
    int end_file;                       /* End file if mapped VID */
    int partition;                      /* Partition number */
    int tprc;                           /* Return code from last Ctape */                                       
    int TStartRequest;                  /* Start time of request (set by client) */
    int TEndRequest;                    /* End time of request (set by client) */
    int TStartRtcpd;                    /* Time when request is received by rtcpd server */
    int TStartMount;                    /* Time when mount request is sent to Ctape */
    int TEndMount;                      /* Time when mount request returns */
    int TStartUnmount;                  /* Time when unmount request is sent to Ctape */
    int TEndUnmount;                    /* Time when unmount request returns */

    /*
     * Error reporting
     */
    rtcpErrMsg_t err;
} rtcpTapeRequest_t;

#define RTCP_TAPEREQLEN(X) (14*LONGSIZE + strlen(X->vid) + \
        strlen(X->vsn) + strlen(X->label) +  \
        strlen(X->devtype) + strlen(X->density) + strlen(X->unit)+6)

/*
 * Circular lists of tape and file requests. (Client only)
 */
typedef struct tape_list {
    int magic;
    int local_retry;                 /* Flag set if we are doing a
                                      * local retry. This is needed
                                      * to prevent Ctape_reserve() from
                                      * being called twice. */
    rtcpTapeRequest_t tapereq;        
    struct file_list *file;          /* List of files for this tape */
    struct tape_list *next;          /* Next in circular list */
    struct tape_list *prev;          /* Previous in circular list */
} tape_list_t;

typedef struct file_list {
    int magic;
    /*
     * Keep track of number of records transfered to/from tape.
     * On write this counter is used in the label handling.
     */
    int trec ;
    /*
     * For end of volume processing. 
     */
    int eovflag ;
    /*
     * Flags specific to SONY raw devices. Declare them in 
     * all cases to reduce the number of ifdefs
     */
    int negotiate;
    int sonyraw;
    /*
     * Internal counter on how many (uncompressed) bytes copied 
     * from/to disk and to/from tape so far
     */
    u_signed64 diskbytes_sofar;
    u_signed64 tapebytes_sofar;
    /*
     * Disk file FORTRAN UNIT number (recfm==U only)
     */
    int FortranUnit;

    rtcpFileRequest_t filereq;
    struct tape_list *tape;          /* Parent tape request */
    struct file_list *next;          /* Next in circular list */
    struct file_list *prev;          /* Previous in circular list */
} file_list_t;

/*
 * Macros
 */
#define LOWERCASE(X) {char *__c; \
    for (__c=X; *__c != '\0'; __c++) *__c=tolower(*__c); } 
#define UPPERCASE(X) {char *__c; \
    for (__c=X; *__c != '\0'; __c++) *__c=toupper(*__c); }

#if !defined(min)
#define min(X,Y) (X < Y ? X : Y)
#endif /* min */

#if !defined(max)
#define max(X,Y) (X > Y ? X : Y)
#endif /* max */

/*
 * Various circular list operations.
 */
#define CLIST_ITERATE_BEGIN(X,Y) {if ( (X) != NULL ) {(Y)=(X); do {
#define CLIST_ITERATE_END(X,Y) Y=(Y)->next; } while ((X) != (Y));}}
#define CLIST_INSERT(X,Y) {if ((X) == NULL) {X=(Y); (X)->next = (X)->prev = (X);} \
else {(Y)->next=(X); (Y)->prev=(X)->prev; (Y)->next->prev=(Y); (Y)->prev->next=(Y);}}
#define CLIST_DELETE(Y,X) {if ((X) != NULL) {if ( (X) == (Y) ) (Y)=(Y)->next; if ((X)->prev != (X) && (X)->next != (X)) {\
(X)->prev->next = (X)->next; (X)->next->prev = (X)->prev;} else {(Y)=NULL;}}}

/*
 * This extern declaration is needed by both client and server
 */
EXTERN_C void DLL_DECL (*rtcp_log) _PROTO((int, const char *, ...));

#endif /* RTCP_H */
