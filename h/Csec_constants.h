#ifndef _CSEC_CONSTANTS_H
#define _CSEC_CONSTANTS_H

/* Buffer sizes */
#define ERRBUFSIZE 2000
#define HEADBUFSIZE 20

/* Version number relating to the Csec dialogue */
#define CSEC_VERSION 1

/* Magic number for authentication tokens */
#define CSEC_TOKEN_MAGIC_1   0xCA03

/* Magic number to ensure the structure is properly initialized */
/* BEWARE, all client magic numbers must correspond
   to this client mask ! */
#define CSEC_CONTEXT_MAGIC_CLIENT_MASK  0x80000000
#define CSEC_CONTEXT_MAGIC_SERVER_1 0x0CA00001
#define CSEC_CONTEXT_MAGIC_CLIENT_1 (CSEC_CONTEXT_MAGIC_SERVER_1|CSEC_CONTEXT_MAGIC_CLIENT_MASK)

/* Name of local protocols for mapped identity */
#define USERNAME_MECH "USERNAME"
#define LOCALID_MECH  "LOCALID"


/** Various token types */
#define CSEC_TOKEN_TYPE_PROTOCOL_REQ   0x1
#define CSEC_TOKEN_TYPE_PROTOCOL_RESP  0x2
#define CSEC_TOKEN_TYPE_HANDSHAKE      0x3
#define CSEC_TOKEN_TYPE_DATA           0x4

/** Buffer size for errors */
#define SECPRTBUFSZ 2000
/** Timeout used in the netread/netwrites */
#define CSEC_NET_TIMEOUT 10000

/** Environment variables to set to use Csec_trace */
#define CSEC_TRACE "CSEC_TRACE"
#define CSEC_TRACEFILE "CSEC_TRACEFILE"

/** Environment variable to switch mechanism */
#define CSEC_MECH "CSEC_MECH"
#define CSEC_AUTH_MECH "CSEC_AUTH_MECH"
#define CSEC_CONF_SECTION "CSEC"
#define CSEC_CONF_ENTRY_MECH "MECH"
#define CSEC_CONF_ENTRY_AUTHMECH "AUTHMECH"

enum Csec_context_type { 
  CSEC_CONTEXT_CLIENT=0, 
  CSEC_CONTEXT_SERVER=1 
};

enum Csec_service_type {
  CSEC_SERVICE_TYPE_HOST =0,
  CSEC_SERVICE_TYPE_CENTRAL =1,
  CSEC_SERVICE_TYPE_DISK =2,
  CSEC_SERVICE_TYPE_TAPE =3,
  CSEC_SERVICE_TYPE_STAGER =4,
  CSEC_SERVICE_TYPE_MAX =5
};

/* Various flags */

#define PROT_STAT_INITIAL       0x0
#define PROT_STAT_REQ_SENT      0x1
#define PROT_STAT_NOK           0x2
#define PROT_STAT_OK            0x4

/* Constants for protocol status */

#define PROT_REQ_OK 1
#define PROT_REQ_NOK 0

/* Status of the context */
#define CSEC_CTX_INITIALIZED          0x00000001L
#define CSEC_CTX_SERVICE_TYPE_SET     0x00000002L
#define CSEC_CTX_PROTOCOL_LOADED      0x00000004L
#define CSEC_CTX_PROTOCOL_CHOSEN      0x00000008L
#define CSEC_CTX_PLUGIN_LOADED        0x00000010L
#define CSEC_CTX_PLUGIN_INITIALIZED   0x00000020L
#define CSEC_CTX_CONTEXT_ESTABLISHED  0x00000040L
#define CSEC_CTX_DELEGCREDS_EXPORTED  0x00000080L
#define CSEC_CTX_USER_MAPPING_DONE    0x00000100L

/* Status of the protocols when being checked */
#define CSEC_PROT_NOSHLIB             0x00000001L
#define CSEC_PROT_NOCREDS             0x00000002L
#define CSEC_PROT_NOCHECK             0x00000004L

/* Security options */
#define CSEC_OPT_DELEG_FLAG           0x00000001L

#endif /* _CSEC_CONSTANTS */
