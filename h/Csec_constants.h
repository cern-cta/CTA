#ifndef _CSEC_CONSTANTS_H
#define _CSEC_CONSTANTS_H

/* Buffer sizes */
#define ERRBUFSIZE 2000
#define HEADBUFSIZE 20

/* Magic number for authentication tokens */
#define CSEC_TOKEN_MAGIC_1   0xCA03

/* Magic number to ensure the structure is properly initialized */
#define CSEC_CONTEXT_MAGIC_CLIENT_1 0x8CA00001
#define CSEC_CONTEXT_MAGIC_SERVER_1 0x0CA00001

/* BEWARE, all client magic numbers must correspond
   to this client mask ! */
#define CSEC_CONTEXT_MAGIC_CLIENT_MASK  0x80000000

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


#define PROTID_SIZE 20

#define CA_MAXSERVICENAMELEN 512

enum Csec_service_types {
  CSEC_SERVICE_TYPE_HOST,
  CSEC_SERVICE_TYPE_CENTRAL,
  CSEC_SERVICE_TYPE_DISK,
  CSEC_SERVICE_TYPE_TAPE,
  CSEC_SERVICE_TYPE_STAGER,
  CSEC_SERVICE_TYPE_MAX
};

#define PROT_STAT_INITIAL       0x0
#define PROT_STAT_REQ_SENT      0x1
#define PROT_STAT_NOK           0x2
#define PROT_STAT_OK            0x4

#define PROT_REQ_OK "OK"
#define PROT_REQ_NOK "NOK"

/* Status of the context */
#define CSEC_CTX_INITIALIZED          0x00000001L
#define CSEC_CTX_SERVICE_TYPE_SET     0x00000002L
#define CSEC_CTX_PROTOCOL_LOADED      0x00000004L

#define CSEC_CTX_SHLIB_LOADED         0x00000008L
#define CSEC_CTX_SERVICE_NAME_SET     0x00000010L
#define CSEC_CTX_CREDENTIALS_LOADED   0x00000020L
#define CSEC_CTX_CONTEXT_ESTABLISHED  0x00000040L
#define CSEC_CTX_USER_MAPPED          0x00000080L

/* Status of the protocols when being checked */
#define CSEC_PROT_NOSHLIB             0x00000001L
#define CSEC_PROT_NOCREDS             0x00000002L
#define CSEC_PROT_NOCHECK             0x00000004L



#endif /* _CSEC_CONSTANTS */
