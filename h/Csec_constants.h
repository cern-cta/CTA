#ifndef _CSEC_CONSTANTS_H
#define _CSEC_CONSTANTS_H

/* Buffer sizes */
#define ERRBUFSIZE 2000
#define HEADBUFSIZE 20

/* Magic number for authentication tokens */
#define CSEC_TOKEN_MAGIC_1   0xCA03

/** Various token types */
#define CSEC_TOKEN_TYPE_PROTOCOL_REQ   0x1
#define CSEC_TOKEN_TYPE_PROTOCOL_RESP  0x2
#define CSEC_TOKEN_TYPE_HANDSHAKE      0x3
#define CSEC_TOKEN_TYPE_DATA           0x4


/* char CSEC_TOKEN_TYPE[][30] = { "UNKNOWN",  */
/* 			       "PROTOCOL_REQ", */
/* 			       "PROTOCOL_RESP", */
/* 			       "HANDSHAKE", */
/* 			       "DATA", */
/* 			       "OOB" }; */

/** Buffer size for errors */
#define PRTBUFSZ 2000
/** Timeout used in the netread/netwrites */
#define CSEC_NET_TIMEOUT 10000

/** Environment variables to set to use Csec_trace */
#define CSEC_TRACE "CSEC_TRACE"
#define CSEC_TRACEFILE "CSEC_TRACEFILE"

/** Environment variable to switch mechanism */
#define CSEC_MECH "CSEC_MECH"

#define PROTID_SIZE 20

#define CA_MAXSERVICENAMELEN 512

#define CSEC_SERVICE_TYPE_HOST    0
#define CSEC_SERVICE_TYPE_CENTRAL 1
#define CSEC_SERVICE_TYPE_DISK    2
#define CSEC_SERVICE_TYPE_TAPE    3

#define PROT_STAT_INITIAL       0x0
#define PROT_STAT_REQ_SENT      0x1
#define PROT_STAT_NOK           0x2
#define PROT_STAT_OK            0x4

#define PROT_REQ_OK "OK"
#define PROT_REQ_NOK "NOK"

/* Status of the context */
#define CSEC_CTX_INITIALIZED          0x00000001L
#define CSEC_CTX_CREDENTIALS_LOADED   0x00000002L
#define CSEC_CTX_CONTEXT_ESTABLISHED  0x00000004L
#define CSEC_CTX_SERVICE_TYPE_SET     0x00000008L
#define CSEC_CTX_PROTOCOL_LOADED      0x00000010L
#define CSEC_CTX_SHLIB_LOADED         0x00000020L
#define CSEC_CTX_SERVICE_NAME_SET     0x00000040L
#define CSEC_CTX_USER_MAPPED          0x00000080L

#endif /* _CSEC_CONSTANTS */
