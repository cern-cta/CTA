/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 1990-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#ifndef _SHIFT_H_INCLUDED_
#include <osdep.h>                      /* EXTERN_C   */
#endif
#include <sys/types.h>                  /* For size_t                   */
#include <stddef.h>                     /* For size_t on _WIN32         */

#define SEBASEOFF       1000            /* Base offset for special err. */
#define EDBBASEOFF      1200            /* CDB error base offset        */
#define EMSBASEOFF      1300            /* MSG error base offset        */
#define ENSBASEOFF      1400            /* NS error base offset         */
#define ERFBASEOFF      1500            /* RFIO error base offset       */
#define ERTBASEOFF      1600            /* RTCOPY error base offset     */
#define ESTBASEOFF      1700            /* STAGE error base offset      */
#define ESQBASEOFF      1800            /* SYSREQ error base offset     */
#define ETBASEOFF       1900            /* TAPE error base offset       */
#define EVMBASEOFF      2000            /* VMGR error base offset       */
#define EVQBASEOFF      2100            /* VDQM error base offset       */
#define ERMBASEOFF      2200            /* RMC error base offset        */
#define EMONBASEOFF     2300            /* Monitoring Error base offset */
#define EUPBASEOFF      2400            /* UPV error base offset        */
#define ESECBASEOFF     2700            /* Security error base offset   */
#define EDNSBASEOFF     3000            /* DNS error base offset        */

#define SENOERR         SEBASEOFF       /* No error                     */
#define SENOSHOST       SEBASEOFF+1     /* Host not known               */
#define SENOSSERV       SEBASEOFF+2     /* Service unknown              */
#define SENOTRFILE      SEBASEOFF+3     /* Not a remote file            */
#define SETIMEDOUT      SEBASEOFF+4     /* Has timed out                */
#define SEBADFFORM      SEBASEOFF+5     /* Bad fortran format specifier */
#define SEBADFOPT       SEBASEOFF+6     /* Bad fortran option specifier */
#define SEINCFOPT       SEBASEOFF+7     /* Incompatible fortran options */
#define SENAMETOOLONG   SEBASEOFF+8     /* File name too long           */
#define SENOCONFIG      SEBASEOFF+9     /* Can't open configuration file*/
#define SEBADVERSION    SEBASEOFF+10    /* Version ID mismatch          */
#define SEUBUF2SMALL    SEBASEOFF+11    /* User buffer too small        */
#define SEMSGINVRNO     SEBASEOFF+12    /* Invalid reply number         */
#define SEUMSG2LONG     SEBASEOFF+13    /* User message too long        */
#define SEENTRYNFND     SEBASEOFF+14    /* Entry not found              */
#define SEINTERNAL      SEBASEOFF+15    /* Internal error               */
#define SECONNDROP      SEBASEOFF+16    /* Connection closed by rem. end*/
#define SEBADIFNAM      SEBASEOFF+17    /* Can't get interface name     */
#define SECOMERR        SEBASEOFF+18    /* Communication error          */
#define SENOMAPDB       SEBASEOFF+19    /* Can't open mapping database  */
#define SENOMAPFND      SEBASEOFF+20    /* No user mapping              */
#define SERTYEXHAUST    SEBASEOFF+21    /* Retry count exhausted        */
#define SEOPNOTSUP      SEBASEOFF+22    /* Operation not supported      */
#define SEWOULDBLOCK    SEBASEOFF+23    /* Resource temporarily unavailable */
#define SEINPROGRESS    SEBASEOFF+24    /* Operation now in progress    */
#define SECTHREADINIT   SEBASEOFF+25    /* Cthread initialization error */
#define SECTHREADERR    SEBASEOFF+26    /* Thread interface call error  */
#define SESYSERR        SEBASEOFF+27    /* System error                 */
#define SENOTADMIN      SEBASEOFF+32    /* requestor is not administrator */
#define SEUSERUNKN      SEBASEOFF+33    /* User unknown                 */
#define SEDUPKEY        SEBASEOFF+34    /* Duplicate key value          */
#define SEENTRYEXISTS   SEBASEOFF+35    /* Entry already exists         */
#define SEGROUPUNKN     SEBASEOFF+36    /* Group Unknown                */
#define SECHECKSUM      SEBASEOFF+37    /* Bad checksum                 */
#define SESVCCLASSNFND  SEBASEOFF+38    /* This service class is not available for this host */
#define SESQLERR        SEBASEOFF+39    /* Got SQL exception from database */
#define SELOOP          SEBASEOFF+40    /* Too many symbolic links      */
#define SENOPORTINRANGE SEBASEOFF+41    /* No port in range             */
#define SENOVALUE       SEBASEOFF+42    /* No value                     */
#define SEINVALIDCONFIG SEBASEOFF+43    /* Invalid configuration        */
#define SEPYTHONEXEC    SEBASEOFF+44    /* Failed to execute Python     */
#define SEMISSINGOPER   SEBASEOFF+45    /* Missing operand              */
#define SEMISMATCH      SEBASEOFF+46    /* Mismatch                     */
#define SEREQUESTFAILED SEBASEOFF+47    /* Request failed               */
#define SEINVALIDNBARGS SEBASEOFF+48    /* Invalid number of arguments  */
#define SEALREADYINIT   SEBASEOFF+49    /* Already initialized          */
#define SECMDLNNOTPRSD  SEBASEOFF+50    /* Command line not parsed      */
#define SEACCPTCONNINTR SEBASEOFF+51    /* Accept connection was interrupted */
#define SEBADALLOC      SEBASEOFF+52    /* Failed to allocate memory    */
#define SENOTANOWNER    SEBASEOFF+53    /* Not an owner                 */

#define SEMAXERR        SEBASEOFF+53    /* Maximum error number         */

#define SERRNO  (serrno - SEBASEOFF)    /* User convenience             */
/*
 * Backward compatibility
 */
#define SEFNAM2LONG     SENAMETOOLONG

/*
 * Package specific error messages (don't forget to update commmon/serror.c)
 */

/*
 *------------------------------------------------------------------------
 * DB errors
 *------------------------------------------------------------------------
 */
#define EDB_A_ESESSION EDBBASEOFF+1    /* Cdb api           : invalid session    */
#define EDB_A_EDB      EDBBASEOFF+2    /* Cdb api           : invalid db         */
#define EDB_A_EINVAL   EDBBASEOFF+3    /* Cdb api           : invalid value      */
#define EDB_A_RESHOST  EDBBASEOFF+4    /* Cdb api           : host res error     */
#define EDB_A_TOOMUCH  EDBBASEOFF+5    /* Cdb api           : data size rejected */
#define EDB_AS_SOCKET  EDBBASEOFF+6    /* Cdb api    system : socket() error     */
#define EDB_AS_SOCKOPT EDBBASEOFF+7    /* Cdb api    system : [set/get]sockopt() error */
#define EDB_AS_MALLOC  EDBBASEOFF+8    /* Cdb api    system : malloc() error     */
#define EDB_A_NOERROR  EDBBASEOFF+9    /* Cdb api           : no last error      */
#define EDB_A_IEINVAL  EDBBASEOFF+10   /* Cdb api           : interface invalid value */
#define EDB_AS_BIND    EDBBASEOFF+11   /* Cdb api           : bind() error       */
#define EDB_AS_LISTEN  EDBBASEOFF+12   /* Cdb api           : listen() error     */
#define EDB_AS_GETSOCKNAME EDBBASEOFF+13 /* Cdb api         : getsockname() error */
#define EDB_AS_ACCEPT  EDBBASEOFF+14   /* Cdb api           : accept() error     */
#define EDB_AS_GETPEERNAME  EDBBASEOFF+15 /* Cdb api        : getpeername() error */
#define EDB_A_WHOISIT  EDBBASEOFF+16 /* Cdb api        : Connection from bad host */

#define EDB_D_EINVAL   EDBBASEOFF+20   /* Cdb daemon        : invalid value      */
#define EDB_D_EAGAIN   EDBBASEOFF+21   /* Cdb daemon        : yet done           */
#define EDB_D_AUTH     EDBBASEOFF+22   /* Cdb daemon        : unauthorized       */
#define EDB_D_LOGIN    EDBBASEOFF+23   /* Cdb daemon        : login refused      */
#define EDB_D_PWDCORR  EDBBASEOFF+24   /* Cdb daemon        : pwd file corrupted */
#define EDB_D_ANA      EDBBASEOFF+25   /* Cdb daemon        : db analysis error  */
#define EDB_D_HASHSIZE EDBBASEOFF+26   /* Cdb daemon        : bad hash size      */
#define EDB_D_UNKNOWN  EDBBASEOFF+27   /* Cdb daemon        : unkn. db/table/key */
#define EDB_D_NOLOCK   EDBBASEOFF+28   /* Cdb daemon        : lock is required   */
#define EDB_D_CORRUPT  EDBBASEOFF+29   /* Cdb daemon        : probably corrupted */
#define EDB_D_TOOMUCH  EDBBASEOFF+30   /* Cdb daemon        : data size rejected */
#define EDB_D_ENOENT   EDBBASEOFF+31   /* Cdb daemon        : no entry           */
#define EDB_D_ETYPE    EDBBASEOFF+32   /* Cdb daemon        : unknown member type*/
#define EDB_D_EVALUE   EDBBASEOFF+33   /* Cdb daemon        : unknown member val */
#define EDB_D_NULLVALUE EDBBASEOFF+34  /* Cdb daemon        : null member value  */
#define EDB_D_LOCK     EDBBASEOFF+35   /* Cdb daemon        : cannot gain lock   */
#define EDB_D_FREE     EDBBASEOFF+36   /* Cdb daemon        : unsafe free attempt*/
#define EDB_D_SHUTDOWN EDBBASEOFF+37   /* Cdb daemon        : shutdown in progress */
#define EDB_D_DEADLOCK EDBBASEOFF+38   /* Cdb daemon        : deadlock detected  */
#define EDB_D_EXIST    EDBBASEOFF+39   /* Cdb daemon        : yet exists         */
#define EDB_D_NOSPC    EDBBASEOFF+40   /* Cdb daemon        : no more space      */
#define EDB_D_DUMPEND  EDBBASEOFF+41   /* Cdb daemon        : end of dump        */
#define EDB_D_UNIQUE   EDBBASEOFF+42   /* Cdb daemon        : uniqued key yet exist */
#define EDB_D_LISTEND  EDBBASEOFF+43   /* Cdb daemon        : end of list        */
#define EDB_D_NOTDUMP  EDBBASEOFF+44   /* Cdb daemon        : not in dump mode   */
#define EDB_D_DNSCHECK EDBBASEOFF+45   /* Cdb daemon        : double DNS check error */
#define EDB_D_REJECTED EDBBASEOFF+46   /* Cdb daemon        : Connection rejected (not authorised) */
#define EDB_D_INIT     EDBBASEOFF+47   /* Cdb daemon        : init in progress */
#define EDB_D_INCONST  EDBBASEOFF+48   /* Cdb daemon        : inconsistent request (unstop and no previous stop, unfreeze and no previous freeze) */
#define EDB_D_FREEHASHSIZE EDBBASEOFF+49 /* Cdb daemon      : bad free hash size      */

#define EDB_DS_MALLOC  EDBBASEOFF+50   /* Cdb daemon system : malloc() error     */
#define EDB_DS_CALLOC  EDBBASEOFF+51   /* Cdb daemon system : calloc() error     */
#define EDB_DS_REALLOC EDBBASEOFF+52   /* Cdb daemon system : realloc() error    */
#define EDB_DS_OPEN    EDBBASEOFF+53   /* Cdb daemon system : open() error       */
#define EDB_DS_FSTAT   EDBBASEOFF+54   /* Cdb daemon system : fstat() error      */
#define EDB_DS_LSEEK   EDBBASEOFF+55   /* Cdb daemon system : lseek() error      */
#define EDB_DS_READ    EDBBASEOFF+56   /* Cdb daemon system : read() error       */
#define EDB_DS_WRITE   EDBBASEOFF+57   /* Cdb daemon system : write() error      */
#define EDB_DS_RENAME  EDBBASEOFF+58   /* Cdb daemon system : rename() error     */
#define EDB_DS_FTRUNC  EDBBASEOFF+59   /* Cdb daemon system : ftruncate() error  */
#define EDB_DS_TMPNAM  EDBBASEOFF+60   /* Cdb daemon system : tmpnam() error     */
#define EDB_DS_FCNTL   EDBBASEOFF+61   /* Cdb daemon system : fcntl() error      */
#define EDB_DS_MKDIR   EDBBASEOFF+62   /* Cdb daemon system : mkdir() error      */
#define EDB_DS_TIMES   EDBBASEOFF+63   /* Cdb daemon system : times() error      */
#define EDB_DS_SYSCONF EDBBASEOFF+64   /* Cdb daemon system : sysconf() err/unav */
#define EDB_DS_GETHOSTNAME EDBBASEOFF+65 /* Cdb daemon system : gethostname() error*/
#define EDB_DS_GETPEERNAME EDBBASEOFF+66 /* Cdb daemon system : getpeername() error*/
#define EDB_DS_INET_NTOA EDBBASEOFF+67 /* Cdb daemon system : getpeername() error*/
#define EDB_DS_REMOVE  EDBBASEOFF+68   /* Cdb daemon system : remove() error */
#define EDB_DS_SIGACTION  EDBBASEOFF+69   /* Cdb daemon system : sigaction() error */
#define EDB_DS_GETSOCKNAME EDBBASEOFF+70 /* Cdb daemon system : getsockname() error*/
#define EDB_DS_BIND    EDBBASEOFF+71 /* Cdb daemon system : bind() error*/
#define EDB_DS_LISTEN    EDBBASEOFF+72 /* Cdb daemon system : listen() error*/
#define EDB_DS_CONNECT   EDBBASEOFF+73 /* Cdb daemon system : connect() error*/
#define EDB_DS_SOCKET   EDBBASEOFF+74 /* Cdb daemon system : socket() error*/
#define EDB_DS_SOCKOPT  EDBBASEOFF+75 /* Cdb daemon system : [set/get]sockopt() error*/
#define EDB_D_RESHOST   EDBBASEOFF+76 /* Cdb daemon     : host res error     */
#define EDB_D_REQSIZE   EDBBASEOFF+77 /* Cdb daemon     : request too big    */

#define EDB_C_EINVAL     EDBBASEOFF+80 /* Cdb config        : invalid value      */
#define EDB_C_ENOENT     EDBBASEOFF+81  /* Cdb config        : configuration error*/
#define EDB_C_TOOMUCH  EDBBASEOFF+82   /* Cdb config        : conf. size rejected */
#define EDB_CS_GETHOSTNAME EDBBASEOFF+83 /* Cdb config system : gethostname() error*/

#define EDB_NOMOREDB   EDBBASEOFF+90 /* Cdb : nomoredb */

#define EDBMAXERR      EDBBASEOFF+90

/*
 *------------------------------------------------------------------------
 * MSG daemon errors
 *------------------------------------------------------------------------
 */
#define EMSMSGU2REP     EMSBASEOFF+1    /* msg daemon unable to reply   */
#define EMSMSGSYERR     EMSBASEOFF+2    /* msg daemon system error      */
#define EMSNOPERM       EMSBASEOFF+3    /* Permission denied            */
#define EMSMAXERR       EMSBASEOFF+3    /* Maximum error number of MSG  */
/*
 * Backward compatibility
 */
#define SEMSGU2REP      EMSMSGU2REP
#define SEMSGSYERR      EMSMSGSYERR
#define SENOPERM        EMSNOPERM

/*
 *------------------------------------------------------------------------
 * NS (Name Server) errors
 *------------------------------------------------------------------------
 */
#define	ENSNACT        ENSBASEOFF+1	/* Name server not active */
#define	ENSFILECHG     ENSBASEOFF+2	/* File has been overwritten, request ignored */
#define ENSNOSEG       ENSBASEOFF+3	/* Segment had been deleted */
#define ENSISLINK      ENSBASEOFF+4	/* Is a link */
#define ENSCLASSNOSEGS ENSBASEOFF+5	/* File class does not allow a copy on tape */
#define ENSTOOMANYSEGS ENSBASEOFF+6	/* Too many copies on tape */
#define ENSOVERWHENREP ENSBASEOFF+7	/* Cannot overwrite valid segment when replacing */
#define ENHOSTNOTSET   ENSBASEOFF+8	/* CNS HOST not set */
#define ENSMAXERR      ENSBASEOFF+8

/*
 *------------------------------------------------------------------------
 * RFIO errors
 *------------------------------------------------------------------------
 */
#define ERFNORCODE      ERFBASEOFF+1    /* RFIO communication error     */
#define ERFHOSTREFUSED  ERFBASEOFF+2    /* RFIO rejected connect attempt*/
#define ERFXHOST        ERFBASEOFF+3    /* Cross-host link (rename())   */
#define ERFPROTONOTSUP  ERFBASEOFF+4    /* RFIO protocol not supported  */
#define ERFMAXERR       ERFBASEOFF+4    /* Maximum error number of RFIO */
/*
 * Backward compatibility
 */
#define SENORCODE       ERFNORCODE
#define SEHOSTREFUSED   ERFHOSTREFUSED
#define SEXHOST         ERFXHOST
#define SEPROTONOTSUP   ERFPROTONOTSUP

/*
 *------------------------------------------------------------------------
 * RTCOPY errors
 *------------------------------------------------------------------------
 */
#define ERTTMSERR       ERTBASEOFF+1    /* TMS call failed */
#define ERTBLKSKPD      ERTBASEOFF+2    /* Blocks were skipped in file  */
#define ERTTPE_LSZ      ERTBASEOFF+3    /* Blocks skipped and file truncated */
#define ERTMNYPARY      ERTBASEOFF+4    /* Too many skipped blocks      */
#define ERTLIMBYSZ      ERTBASEOFF+5    /* File limited by size         */
#define ERTUSINTR       ERTBASEOFF+6    /* Request interrupted by user  */
#define ERTOPINTR       ERTBASEOFF+7    /* Request interrupted by operator */
#define ERTNOTCLIST     ERTBASEOFF+8    /* Request list is not circular */
#define ERTBADREQ       ERTBASEOFF+9    /* Bad request structure */
#define ERTMORETODO	ERTBASEOFF+10	/* Request partially processed */
#define ERTDBERR        ERTBASEOFF+11   /* Catalogue DB error */
#define ERTZEROSIZE     ERTBASEOFF+12   /* Zero sized file */
#define ERTWRONGSIZE    ERTBASEOFF+13   /* Recalled file size incorrect */
#define ERTWRONGFSEQ    ERTBASEOFF+14   /* Inconsistent FSEQ in VMGR and Cns */
#define ERTMAXERR       ERTBASEOFF+14

/*
 *------------------------------------------------------------------------
 * STAGE errors
 *------------------------------------------------------------------------
 */
#define ESTCLEARED      ESTBASEOFF+1	/* aborted */
#define ESTENOUGHF      ESTBASEOFF+2	/* enough free space */
#define ESTLNKNCR       ESTBASEOFF+3	/* symbolic link not created */
#define ESTLNKNSUP      ESTBASEOFF+4	/* symbolic link not supported */
#define ESTNACT         ESTBASEOFF+5	/* Stager not active */
#define ESTGROUP        ESTBASEOFF+6	/* Your group is invalid */
#define ESTGRPUSER      ESTBASEOFF+7	/* No GRPUSER in configuration */
#define ESTUSER         ESTBASEOFF+8	/* Invalid user */
#define ESTHSMHOST      ESTBASEOFF+9	/* HSM HOST not specified */
#define ESTTMSCHECK     ESTBASEOFF+10	/* tmscheck error */
#define ESTLINKNAME     ESTBASEOFF+11	/* User link name processing error */
#define ESTWRITABLE     ESTBASEOFF+12	/* User path in a non-writable directory */
#define ESTKILLED       ESTBASEOFF+13	/* aborted by kill */
#define ESTMEM          ESTBASEOFF+14	/* request too long (api) */
#define ESTCONF         ESTBASEOFF+15	/* Stage configuration error */
#define ESTSEGNOACC     ESTBASEOFF+16	/* Unreadable file on tape (segments not all accessible) */
#define ESTREPLFAILED   ESTBASEOFF+17	/* File replication failed */
#define ESTNOTAVAIL     ESTBASEOFF+18	/* File is currently not available */
#define ESTJOBKILLED    ESTBASEOFF+19   /* Job killed by service administrator */
#define ESTJOBTIMEDOUT	ESTBASEOFF+20	/* Job timed out while waiting to be scheduled */
#define ESTSCHEDERR     ESTBASEOFF+21   /* Scheduler error */
#define ESTSVCCLASSNOFS ESTBASEOFF+22   /* No filesystems available in service class */
#define ESTNOSEGFOUND   ESTBASEOFF+23   /* File has no copy on tape and no diskcopies are accessible */
#define ESTTAPEOFFLINE  ESTBASEOFF+24   /* File is on an offline tape */
#define ESTREQCANCELED  ESTBASEOFF+25   /* Request canceled while queuing */
#define ESTTCNOTFOUND   ESTBASEOFF+26   /* Tape-copy not found */
#define ESTNOTAPEROUTE  ESTBASEOFF+27   /* The file cannot be routed to tape */
#define ESTMAXERR       ESTBASEOFF+27

/*
 *------------------------------------------------------------------------
 * SYSREQ errors
 *------------------------------------------------------------------------
 */
#define ESQTMSNOTACT    ESQBASEOFF+1    /* TMS not active                 */
#define ESQMAXERR       ESQBASEOFF+1    /* Maximum error number of SYSREQ */
/*
 * Backward compatibility
 */
#define SETMSNOTACT     ESQTMSNOTACT

/*
 *------------------------------------------------------------------------
 * TAPE errors
 *------------------------------------------------------------------------
 */
#define ETDNP		ETBASEOFF+1	/* daemon not available */
#define ETSYS		ETBASEOFF+2	/* system error */
#define	ETPRM		ETBASEOFF+3	/* bad parameter */
#define	ETRSV		ETBASEOFF+4	/* reserv already issued */
#define	ETNDV		ETBASEOFF+5	/* too many drives requested */
#define	ETIDG		ETBASEOFF+6	/* invalid device group name */
#define	ETNRS		ETBASEOFF+7	/* reserv not done */
#define	ETIDN		ETBASEOFF+8	/* no drive with requested characteristics */
#define	ETLBL		ETBASEOFF+9	/* bad label structure */
#define	ETFSQ		ETBASEOFF+10	/* bad file sequence number */
#define	ETINTR		ETBASEOFF+11	/* interrupted by user */
#define ETEOV		ETBASEOFF+12	/* EOV found in multivolume set */
#define ETRLSP		ETBASEOFF+13	/* release pending */
#define ETBLANK		ETBASEOFF+14	/* blank tape */
#define ETCOMPA		ETBASEOFF+15	/* compatibility problem */
#define ETHWERR		ETBASEOFF+16	/* device malfunction */
#define ETPARIT		ETBASEOFF+17	/* parity error */
#define ETUNREC		ETBASEOFF+18	/* unrecoverable media error */
#define ETNOSNS		ETBASEOFF+19	/* no sense */
#define	ETRSLT		ETBASEOFF+20	/* reselect server */
#define	ETVBSY		ETBASEOFF+21	/* volume busy or inaccessible */
#define	ETDCA		ETBASEOFF+22	/* drive currently assigned */
#define	ETNRDY		ETBASEOFF+23	/* drive not ready */
#define	ETABSENT	ETBASEOFF+24	/* volume absent */
#define	ETARCH		ETBASEOFF+25	/* volume archived */
#define	ETHELD		ETBASEOFF+26	/* volume held or disabled */
#define	ETNXPD		ETBASEOFF+27	/* file not expired */
#define	ETOPAB		ETBASEOFF+28	/* operator cancel */
#define	ETVUNKN		ETBASEOFF+29	/* volume unknown */
#define	ETWLBL		ETBASEOFF+30	/* wrong label type */
#define	ETWPROT		ETBASEOFF+31	/* cartridge write protected */
#define	ETWVSN		ETBASEOFF+32	/* wrong vsn */
#define ETBADMIR	ETBASEOFF+33    /* Tape has a bad MIR */
#define ETNETACCEPTINTR ETBASEOFF+34    /* castor::tape::net::acceptConnection interrupted */
#define ETNOLBLINFO	ETBASEOFF+35    /* Label information not found in memory */
#define ETMLTDRVRSV	ETBASEOFF+36    /* Multi-drive reservations are not supported */
#define ETNOLBLINFOMEM	ETBASEOFF+37    /* No memory available for label information */
#define ETSESSIONERROR	ETBASEOFF+38    /* Tape-session error */
#define ETINVALIDTFSEQ	ETBASEOFF+39    /* Invalid tape-file sequence-number */
#define ETINVALIDTFSIZE	ETBASEOFF+40    /* Invalid tape-file file-size */
#define ETMOUNTFAILED	ETBASEOFF+41    /* Failed to mount volume */
#define ETDISMOUNTFAILED ETBASEOFF+42   /* Failed to dismount volume */
#define ETQUERYVOLFAILED ETBASEOFF+43   /* Failed to query volume */
#define ETFDISMOUNTFAILED ETBASEOFF+44  /* Failed to force dismount volume */
#define ETDRVNOTREADYFORMNT ETBASEOFF+45 /* Drive not ready for mount */
#define ETMAXERR        ETBASEOFF+45

/*
 *------------------------------------------------------------------------
 * VMGR (Volume Manager) errors
 *------------------------------------------------------------------------
 */
#define	EVMGRNACT	EVMBASEOFF+1	/* volume manager not active or service being drained */
#define EVMGRNOHOST	EVMBASEOFF+2	/* VMGR HOST not set */
#define EVMMAXERR       EVMBASEOFF+2

/*
 *------------------------------------------------------------------------
 * UPV (User Privilege Validator) errors
 *------------------------------------------------------------------------
 */
#define	ECUPVNACT	EUPBASEOFF+1	/* User Privilege Validator not active or service being drained */
#define EUPMAXERR       EUPBASEOFF+1

/*
 *------------------------------------------------------------------------
 * DNS errors - See netdb.h for details
 *------------------------------------------------------------------------
 */
#define EDNSHOSTNOTFOUND EDNSBASEOFF+1  /* Authoritative Answer Host not found. */
#define EDNSTRYAGAIN    EDNSBASEOFF+2   /* Non-Authoritative Host not found, or SERVERFAIL */
#define EDNSNORECOVERY  EDNSBASEOFF+3   /* Non recoverable errors, FORMERR, REFUSED, NOTIMP. */
#define EDNSNODATA      EDNSBASEOFF+4   /* Valid name, no data record of requested type. */
#define EDNSNOADDRESS   EDNSBASEOFF+5   /* No address, look for MX record. */
#define EDNSMAXERR      EDNSBASEOFF+6

/*
 *------------------------------------------------------------------------
 * VDQM (Volume & Drive Queue Manager) errors
 *------------------------------------------------------------------------
 */
#define EVQSYERR        EVQBASEOFF+1    /* Failed system call */
#define EVQINCONSIST    EVQBASEOFF+2    /* Internal DB inconsistency */
#define EVQREPLICA      EVQBASEOFF+3    /* DB replication failed */
#define EVQNOVOL        EVQBASEOFF+4    /* No volume request queued */
#define EVQNODRV        EVQBASEOFF+5    /* No free drive available */
#define EVQNOSVOL       EVQBASEOFF+6    /* Specified vol. req. not found */
#define EVQNOSDRV       EVQBASEOFF+7    /* Specified drv. req. not found */
#define EVQALREADY      EVQBASEOFF+8    /* Specified vol. req. already exists */
#define EVQUNNOTUP      EVQBASEOFF+9    /* Unit not up */
#define EVQBADSTAT      EVQBASEOFF+10   /* Bad unit status request */
#define EVQBADID        EVQBASEOFF+11   /* Incorrect vol.req or job ID */
#define EVQBADJOBID     EVQBASEOFF+12   /* Incorrect job ID */
#define EVQNOTASS       EVQBASEOFF+13   /* Unit not assigned */
#define EVQBADVOLID     EVQBASEOFF+14   /* Attempt to mount with wrong VOLID */
#define EVQREQASS       EVQBASEOFF+15   /* Attempt to delete an assigned req */
#define EVQDGNINVL      EVQBASEOFF+16   /* Vol. req. for non-existing DGN */
#define EVQPIPEFULL     EVQBASEOFF+17   /* Replication pipe is full */
#define EVQHOLD         EVQBASEOFF+18   /* Server is held */
#define EVQEOQREACHED   EVQBASEOFF+19   /* End of query reached */

#define EVQMAXERR       EVQBASEOFF+19

/*
 *------------------------------------------------------------------------
 * RMC (Remote SCSI media changer server) errors
 *------------------------------------------------------------------------
 */
#define	ERMCNACT	ERMBASEOFF+1	/* Remote SCSI media changer server not active or service being drained */
#define	ERMCRBTERR	(ERMBASEOFF+2)	/* Remote SCSI media changer error */
#define	ERMCUNREC	ERMCRBTERR+1	/* Remote SCSI media changer unrec. error */
#define	ERMCSLOWR	ERMCRBTERR+2	/* Remote SCSI media changer error (slow retry) */
#define	ERMCFASTR	ERMCRBTERR+3	/* Remote SCSI media changer error (fast retry) */
#define	ERMCDFORCE	ERMCRBTERR+4	/* Remote SCSI media changer error (demount force) */
#define	ERMCDDOWN	ERMCRBTERR+5	/* Remote SCSI media changer error (drive down) */
#define	ERMCOMSGN	ERMCRBTERR+6	/* Remote SCSI media changer error (ops message) */
#define	ERMCOMSGS	ERMCRBTERR+7	/* Remote SCSI media changer error (ops message + retry) */
#define	ERMCOMSGR	ERMCRBTERR+8	/* Remote SCSI media changer error (ops message + wait) */
#define	ERMCUNLOAD	ERMCRBTERR+9	/* Remote SCSI media changer error (unload + demount) */
#define ERMMAXERR       ERMBASEOFF+11

/*
 *------------------------------------------------------------------------
 * MONITORING ERRORS
 *------------------------------------------------------------------------
 */

#define EMON_SYSTEM     EMONBASEOFF+1  /* When a system error causes the monitoring to stop */
#define EMON_NO_HOST    EMONBASEOFF+2  /* No monitoring host defined in the configuration */
#define EMON_NO_PORT    EMONBASEOFF+3  /* No monitoring port defined in the configuration */
#define EMON_NO_CLIENTPORT    EMONBASEOFF+4  /* No port for client requests defined in the configuration */

#define EMONMAXERR     EMONBASEOFF+4

/*
 *------------------------------------------------------------------------
 * SECURITY ERRORS
 *------------------------------------------------------------------------
 */
#define ESEC_SYSTEM    ESECBASEOFF + 1 /* System error in the security package */
#define ESEC_BAD_CREDENTIALS ESECBASEOFF + 2 /* Bad credentials */
#define ESEC_NO_CONTEXT ESECBASEOFF + 3 /* Could not establish context */
#define ESEC_BAD_MAGIC ESECBASEOFF + 4 /* Bad magic number */
#define ESEC_NO_USER   ESECBASEOFF + 5 /* Could not map username to uid/gid */
#define ESEC_NO_PRINC   ESECBASEOFF + 6 /* Could not map principal to username */
#define ESEC_NO_SECMECH   ESECBASEOFF + 7 /* Could not load security mechanism */
#define ESEC_CTX_NOT_INITIALIZED   ESECBASEOFF + 8 /* Context not initialized */
#define ESEC_PROTNOTSUPP   ESECBASEOFF + 9 /* Security protocol not supported */
#define ESEC_NO_SVC_NAME   ESECBASEOFF + 10 /* Service name not set */
#define ESEC_NO_SVC_TYPE   ESECBASEOFF + 11 /* Service type not set */
#define ESEC_NO_SECPROT   ESECBASEOFF + 12 /* Could not lookup security protocol */
#define ESEC_BAD_CSEC_VERSION ESECBASEOFF + 13 /* Csec incompatability */
#define ESEC_BAD_PEER_RESP ESECBASEOFF + 14 /* Unexpected response from peer */
#define ESECMAXERR     ESECBASEOFF + 14


/*
 *------------------------------------------------------------------------
 * End of package specific error messages
 *------------------------------------------------------------------------
 */

/*
 * Multi-thread (MT) environment
 */
EXTERN_C int *C__serrno (void);

/*
 * Thread safe serrno. Note, C__serrno is defined in Cglobals.c rather
 * rather than serror.c .
 */
#define serrno (*C__serrno())

/**
 * Writes the string representation of the specified error code into the
 * specified buffer.
 *
 * The string representation is truncated if the specified buffer is too small.
 * In case of such a truncation the string written to the specified buffer is
 * null terminated.
 *
 * @param n The numeric error code.
 * @param buf The buffer to which the string representation should be written.
 * @param buflen The length of the buffer.
 * @return 0 on success or -1 on failure.  In the case of a failure both errno
 * and serrno are set to indicate the type of error.
 */
EXTERN_C int sstrerror_r (const int n, char *const buf, const size_t buflen);

/**
 * Returns the string representation of the specified error code.
 *
 * Allocates some thread specfic memory if needed using Cglobals_get() and then
 * passes that memory to sstrerror_r along with the specified error code.
 *
 * A string is always returned.  This function never returns NULL.
 *
 * @param n The numeric error code.
 * @return The string representation.
 */
EXTERN_C const char *sstrerror (const int n);

EXTERN_C void sperror (char *);

extern const char *sys_serrlist[]; /* Error text array             */
