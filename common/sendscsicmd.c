/*
 * Copyright (C) 1996-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: sendscsicmd.c,v $ $Revision: 1.14 $ $Date: 2005/01/20 16:25:55 $ CERN IT-PDP/DM Fabien Collin/Jean-Philippe Baud";
#endif /* not lint */

/*	send_scsi_cmd - Send a SCSI command to a device */
/*	return	-5	if not supported on this platform (serrno = SEOPNOTSUP)
 *		-4	if SCSI error (serrno = EIO)
 *		-3	if CAM error (serrno = EIO)
 *		-2	if ioctl fails with errno (serrno = errno)
 *		-1	if open/stat fails with errno (message fully formatted)
 *		 0	if successful with no data transfer
 *		>0	number of bytes transferred
 */
/*	currently implemented on SOLARIS25, sgi, hpux, Digital Unix and linux */
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <fcntl.h>
#if defined(SOLARIS25)
#include <sys/scsi/impl/uscsi.h>
#endif
#if defined(sgi)
#include <sys/dsreq.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#endif
#if defined(hpux) || defined(__Lynx__)
#include <sys/scsi.h>
#endif
#if defined(__osf__) && defined(__alpha)
#include <sys/ioctl.h>
#include <io/common/iotypes.h>
#if defined(DUXV4)
#include <io/common/kds.h>
#endif
#include <io/cam/cam.h>
#include <io/cam/dec_cam.h>
#include <io/cam/uagt.h>
#include <net/net_unixlock.h>
#if defined(DUXV4)
#include <io/cam/camdb.h>
#endif
#include <io/cam/pdrv.h>
#include <sys/stat.h>
#endif
#if defined(linux)
#include <stdlib.h>
#include <linux/version.h>
/* Impossible unless very very old kernels: */
#ifndef KERNEL_VERSION
#define KERNEL_VERSION(a,b,c) (((a) << 16) + ((b) << 8) + (c))
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,6,0)
#include <linux/compiler.h>
#endif
#include SCSIINC
#include <sys/stat.h>
#endif
#include "scsictl.h"
#include "serrno.h"
#if defined(TAPE)
#include "Ctape.h"
#define USRMSG(fmt,p,f,msg) \
	{ \
	sprintf (tp_err_msgbuf, fmt, p, f, msg); \
	*msgaddr = tp_err_msgbuf; \
	}
static char tp_err_msgbuf[132];
#else
#define USRMSG(fmt,p,f,msg) {}
#endif
static char nosensekey[] = "no sense key available";
static char notsupp[] = "send_scsi_cmd not supported on this platform";
static char *sk_msg[] = {
        "No sense",
        "Recovered error",
        "Not ready",
        "Medium error",
        "Hardware error",
        "Illegal request",
        "Unit attention",
        "Data protect",
        "Blank check",
        "Vendor unique",
        "Copy aborted",
        "Aborted command",
        "Equal",
        "Volume overflow",
        "Miscompare",
        "Reserved",
};
#if defined(sgi)
struct ds_ret_info {
	int ret;
	char *text;
};
struct ds_ret_info ds_ret_codmsg[] = {
	DSRT_DEVSCSI,	"General devscsi failure",
	DSRT_MULT,	"Request rejected",
	DSRT_CANCEL,	"Lower request cancelled",
	DSRT_REVCODE,	"Software obsolete must recompile",
	DSRT_AGAIN,	"Try again, recoverable bus error",
	DSRT_HOST,	"General host failure",
	DSRT_NOSEL,	"No unit responded to select",
	DSRT_NOSENSE,	"Command with status, error getting sense",
	DSRT_TIMEOUT,	"Command timeout",
	DSRT_LONG,	"Target overran data bounds",
	DSRT_PROTO,	"Miscellaneous protocol failure",
	DSRT_EBSY,	"Busy dropped unexpectedly",
	DSRT_REJECT,	"Message reject",
	DSRT_PARITY,	"Parity error on SCSI bus",
	DSRT_MEMORY,	"Host memory error",
	DSRT_CMDO,	"Error during command phase",
	DSRT_STAI,	"Error during status phase",
	DSRT_UNIMPL,	"Not implemented",
	0xFF,		NULL
};
#endif
#if defined(hpux)
struct cdb_stat_info {
	int status;
	char *text;
};
struct cdb_stat_info cdb_stat_codmsg[] = {
	SCTL_INVALID_REQUEST,	"CCB request is invalid",
	SCTL_SELECT_TIMEOUT,	"Target selection timeout",
	SCTL_INCOMPLETE,	"Command timeout",
	0xFFFF,			NULL
};
#endif
#if defined(__osf__) && defined(__alpha)
struct cam_info {
	int status;
	char *text;
};
struct cam_info cam_codmsg[] = {
	CAM_REQ_INPROG,		"CCB request is in progress",
	CAM_REQ_CMP,		"CCB request completed w/out error",
	CAM_REQ_ABORTED,	"CCB request aborted by the host",
	CAM_UA_ABORT,		"Unable to Abort CCB request",
	CAM_REQ_CMP_ERR,	"CCB request completed with an err",
	CAM_BUSY,		"CAM subsystem is busy",
	CAM_REQ_INVALID,	"CCB request is invalid",
	CAM_PATH_INVALID,	"Path ID supplied is invalid",
	CAM_DEV_NOT_THERE,	"SCSI device not installed/there",
	CAM_UA_TERMIO,		"Unable to Terminate I/O CCB req",
	CAM_SEL_TIMEOUT,	"Target selection timeout",
	CAM_CMD_TIMEOUT,	"Command timeout",
	CAM_MSG_REJECT_REC,	"Message reject received",
	CAM_SCSI_BUS_RESET,	"SCSI bus reset sent/received",
	CAM_UNCOR_PARITY,	"Uncorrectable parity err occurred",
	CAM_AUTOSENSE_FAIL,	"Autosense: Request sense cmd fail",
	CAM_NO_HBA,		"No HBA detected Error",
	CAM_DATA_RUN_ERR,	"Data overrun/underrun error",
	CAM_UNEXP_BUSFREE,	"Unexpected BUS free",
	CAM_SEQUENCE_FAIL,	"Target bus phase sequence failure",
	CAM_CCB_LEN_ERR,	"CCB length supplied is inadequate",
	CAM_PROVIDE_FAIL,	"Unable to provide requ. capability",
	CAM_BDR_SENT,		"A SCSI BDR msg was sent to target",
	CAM_REQ_TERMIO,		"CCB request terminated by the host",
	CAM_HBA_ERR,		"Unrecoverable host bus adaptor err",
	CAM_BUS_RESET_DENIED,	"SCSI bus reset denied",
	CAM_IDE,		"Initiator Detected Error Received",
	CAM_RESRC_UNAVAIL,	"Resource unavailable",
	CAM_UNACKED_EVENT,	"Unacknowledged event by host",
	CAM_MESSAGE_RECV,	"Msg received in Host Target Mode",
	CAM_INVALID_CDB,	"Invalid CDB recvd in HT Mode",
	CAM_LUN_INVALID,	"LUN supplied is invalid",
	CAM_TID_INVALID,	"Target ID supplied is invalid",
	CAM_FUNC_NOTAVAIL,	"The requ. func is not available",
	CAM_NO_NEXUS,		"Nexus is not established",
	CAM_IID_INVALID,	"The initiator ID is invalid",
	CAM_CDB_RECVD,		"The SCSI CDB has been received",
	CAM_LUN_ALLREADY_ENAB,	"LUN already enabled",
	CAM_SCSI_BUSY,		"SCSI bus busy",
	0xFF, NULL
};
static char rsq_failed_msg[] = "(and Release Sim Queue ioctl failed)";
#endif
struct scsi_info {
        int status;
        char *text;
};
struct scsi_info scsi_codmsg[] = {
	SCSI_STATUS_CHECK_CONDITION,	"Check condition",
	SCSI_STATUS_BUSY,		"Target busy",
	SCSI_STATUS_RESERVATION_CONFLICT, "Reservation conflict",
	0xFF,				 NULL
};
static char err_msgbuf[132];
send_scsi_cmd (tapefd, path, do_not_open, cdb, cdblen, buffer, buflen, sense, senselen, timeout, flags, nb_sense_ret, msgaddr)
int tapefd;
char *path;
int do_not_open;
char *cdb;
int cdblen;
char *buffer;
int buflen;
char *sense;
int senselen;
int timeout;	/* in milliseconds */
int flags;
int *nb_sense_ret;
char **msgaddr;
{
	int i;
#if defined(SOLARIS25)
	struct uscsi_cmd ucmd;

	memset ((char *)&ucmd, 0, sizeof(ucmd));
	ucmd.uscsi_timeout = timeout / 1000;
	ucmd.uscsi_cdb = (caddr_t)cdb;
	ucmd.uscsi_cdblen = cdblen;
	ucmd.uscsi_bufaddr = (caddr_t)buffer;
	ucmd.uscsi_buflen = buflen;
	ucmd.uscsi_flags = USCSI_DIAGNOSE | USCSI_SILENT;
	if (flags & SCSI_IN)
		ucmd.uscsi_flags |= USCSI_READ;
	if (flags & SCSI_OUT)
		ucmd.uscsi_flags |= USCSI_WRITE;
	if (flags & SCSI_SYNC)
		ucmd.uscsi_flags |= USCSI_SYNC;
	if (sense)
		ucmd.uscsi_flags |= USCSI_RQENABLE;
	ucmd.uscsi_rqbuf = (caddr_t)sense;
	ucmd.uscsi_rqlen = senselen;

	if (ioctl (tapefd, USCSICMD, &ucmd) < 0 &&
	    (errno != EIO || ucmd.uscsi_status == 0)) {
		*msgaddr = strerror(errno);
		serrno = errno;
		USRMSG (TP042, path, "ioctl", *msgaddr);
		return (-2);
	}
	*nb_sense_ret = ucmd.uscsi_rqlen - ucmd.uscsi_rqresid;
	if (ucmd.uscsi_status) {
		if (ucmd.uscsi_status == SCSI_STATUS_CHECK_CONDITION &&
		    *nb_sense_ret >= 14) {
			sprintf (err_msgbuf, "%s ASC=%X ASCQ=%X",
			    sk_msg[*(sense+2) & 0xF], *(sense+12), *(sense+13));
			*msgaddr = err_msgbuf;
		} else
			get_ss_msg (ucmd.uscsi_status, msgaddr);
		serrno = EIO;
		USRMSG (TP042, path, "ioctl", *msgaddr);
		return (-4);
	}
	return (ucmd.uscsi_buflen - ucmd.uscsi_resid);
#else
#if defined(sgi)
#if defined(IRIX64)
	char canonical_dev[256];
	int canonical_dev_len = sizeof(canonical_dev);
	char *p;
	char *module;
	int iox;
	char *pci;
	char *target;
#endif
	char dspath[80];
	struct dsreq dsreq;
	int fd;
	struct stat sbuf;

	if (do_not_open) {
		fd = tapefd;
		strcpy (dspath, path);
	} else {
		if (stat (path, &sbuf) < 0) {
			serrno = errno;
#if defined(TAPE)
			USRMSG (TP042, path, "stat", strerror(errno));
#else
			sprintf (err_msgbuf, "stat error: %s", strerror(errno));
			*msgaddr = err_msgbuf;
#endif
			return (-1);
		}
#if defined(IRIX64)
		if (attr_get (path, "_devname", canonical_dev, &canonical_dev_len, 0) < 0) {
			serrno = errno;
#if defined(TAPE)
			USRMSG (TP042, path, "attr_get", strerror(errno));
#else
			sprintf (err_msgbuf, "attr_get error: %s", strerror(errno));
			*msgaddr = err_msgbuf;
#endif
			return (-1);
		}
		p = strtok (canonical_dev, "/");
		while (p && strcmp (p, "module")) p = strtok (NULL, "/");
		module = strtok (NULL, "/");
		while (p && strcmp (p, "slot")) p = strtok (NULL, "/");
		iox = atoi(strtok (NULL, "/") + 2);
		while (p && strcmp (p, "pci")) p = strtok (NULL, "/");
		pci = strtok (NULL, "/");
		while (p && strcmp (p, "target")) p = strtok (NULL, "/");
		target = strtok (NULL, "/");
		sprintf (dspath, "/dev/scsi/sc%s%.2d%sd%sl0",
			module, iox, pci, target);
#else
		sprintf (dspath, "/dev/scsi/sc%dd%dl0", minor(sbuf.st_rdev) >> 9,
			(minor(sbuf.st_rdev) >> 5) & 0xF);
#endif
		if ((fd = open (dspath, O_RDWR|O_NDELAY)) < 0) {
			serrno = errno;
#if defined(TAPE)
			USRMSG (TP042, dspath, "open", strerror(errno));
#else
			sprintf (err_msgbuf, "open error: %s", strerror(errno));
			*msgaddr = err_msgbuf;
#endif
			return (-1);
		}
	}
	memset ((char *)&dsreq, 0, sizeof(dsreq));
	dsreq.ds_flags = DSRQ_DISC;
	if (flags & SCSI_IN)
		dsreq.ds_flags |= DSRQ_READ;
	if (flags & SCSI_OUT)
		dsreq.ds_flags |= DSRQ_WRITE;
	if (flags & SCSI_SEL_WITH_ATN)
		dsreq.ds_flags |= DSRQ_SELATN;
	if (flags & SCSI_SYNC)
		dsreq.ds_flags |= DSRQ_SYNXFR;
	if (sense)
		dsreq.ds_flags |= DSRQ_SENSE;
	dsreq.ds_cmdbuf = cdb;
	dsreq.ds_cmdlen = cdblen;
	dsreq.ds_databuf = buffer;
	dsreq.ds_datalen = buflen;
	dsreq.ds_sensebuf = sense;
	dsreq.ds_senselen = senselen;
	dsreq.ds_time = timeout;

	if (ioctl (fd, DS_ENTER, &dsreq) < 0 ) {
		*msgaddr = strerror(errno);
		serrno = errno;
		USRMSG (TP042, dspath, "ioctl", *msgaddr);
		if (! do_not_open) close (fd);
		return (-2);
	}
	if (! do_not_open) close (fd);
	*nb_sense_ret = dsreq.ds_sensesent;
	if (dsreq.ds_ret && dsreq.ds_ret != DSRT_SENSE && dsreq.ds_ret != DSRT_SHORT) {
		for (i = 0; i < 256; i++) {
			if (ds_ret_codmsg[i].ret == 0xFF) {
				sprintf (err_msgbuf,
				    "Undefined CAM status %02X", dsreq.ds_ret);
				*msgaddr = err_msgbuf;
				break;
			} else if (dsreq.ds_ret == ds_ret_codmsg[i].ret) {
				*msgaddr = ds_ret_codmsg[i].text;
				break;
			}
		}
		serrno = EIO;
		USRMSG (TP042, dspath, "ioctl", *msgaddr);
		return (-3);
	}
	if (dsreq.ds_status) {
		if (dsreq.ds_status == SCSI_STATUS_CHECK_CONDITION &&
		    *nb_sense_ret >= 14) {
			sprintf (err_msgbuf, "%s ASC=%X ASCQ=%X",
			    sk_msg[*(sense+2) & 0xF], *(sense+12), *(sense+13));
			*msgaddr = err_msgbuf;
		} else
			get_ss_msg (dsreq.ds_status, msgaddr);
		serrno = EIO;
		USRMSG (TP042, dspath, "ioctl", *msgaddr);
		return (-4);
	}
	return (dsreq.ds_datasent);
#else
#if defined(hpux)
	struct sctl_io sctl_io;

	memset ((char *)&sctl_io, 0, sizeof(sctl_io));
	if (flags & SCSI_IN)
		sctl_io.flags |= SCTL_READ;
	if (flags & SCSI_SYNC)
		sctl_io.flags |= SCTL_INIT_SDTR;
	if (flags & SCSI_WIDE)
		sctl_io.flags |= SCTL_INIT_WDTR;
	memcpy (sctl_io.cdb, cdb, cdblen);
	sctl_io.cdb_length = cdblen;
	sctl_io.data = buffer;
	sctl_io.data_length = buflen;
	sctl_io.max_msecs = timeout;
	if (ioctl (tapefd, SIOC_IO, &sctl_io) < 0) {
		*msgaddr = strerror(errno);
		serrno = errno;
		USRMSG (TP042, path, "ioctl", *msgaddr);
		return (-2);
	}
	memcpy (sense, sctl_io.sense, sctl_io.sense_xfer);
	*nb_sense_ret = sctl_io.sense_xfer;
	if (sctl_io.cdb_status > 0xFF) {
		for (i = 0; i < 256; i++) {
			if (cdb_stat_codmsg[i].status == 0xFFFF) {
				sprintf (err_msgbuf,
				    "Undefined CAM status %04X", sctl_io.cdb_status);
				*msgaddr = err_msgbuf;
				break;
			} else if (sctl_io.cdb_status == cdb_stat_codmsg[i].status) {
				*msgaddr = cdb_stat_codmsg[i].text;
				break;
			}
		}
		serrno = EIO;
		USRMSG (TP042, path, "ioctl", *msgaddr);
		return (-3);
	}
	if (sctl_io.cdb_status) {
		if (sctl_io.cdb_status == SCSI_STATUS_CHECK_CONDITION &&
		    *nb_sense_ret >= 14) {
			sprintf (err_msgbuf, "%s ASC=%X ASCQ=%X",
			    sk_msg[*(sense+2) & 0xF], *(sense+12), *(sense+13));
			*msgaddr = err_msgbuf;
		} else
			get_ss_msg (sctl_io.cdb_status, msgaddr);
		serrno = EIO;
		USRMSG (TP042, path, "ioctl", *msgaddr);
		return (-4);
	}
	return (sctl_io.data_xfer);
#else
#if defined(__osf__) && defined(__alpha)
	int cam_status;
	CCB_SCSIIO ccb;
	int fd;
	int rsqfailed = 0;
	struct stat sbuf;
	UAGT_CAM_CCB ua_ccb;
	static char xmsgbuf[132];

	if (stat (path, &sbuf) < 0) {
		serrno = errno;
#if defined(TAPE)
		USRMSG (TP042, path, "stat", strerror(errno));
#else
		sprintf (err_msgbuf, "stat error: %s", strerror(errno));
		*msgaddr = err_msgbuf;
#endif
		return (-1);
	}
	if (do_not_open) {
		fd = tapefd;
	} else {
		if ((fd = open ("/dev/cam", O_RDWR, 0)) < 0) {
			serrno = errno;
#if defined(TAPE)
			USRMSG (TP042, "/dev/cam", "open", strerror(errno));
#else
			sprintf (err_msgbuf, "open error: %s", strerror(errno));
			*msgaddr = err_msgbuf;
#endif
			return (-1);
		}
	}
	memset ((caddr_t)&ccb, 0, sizeof(ccb));
	ccb.cam_ch.my_addr = (struct ccb_header *)&ccb;
	ccb.cam_ch.cam_ccb_len = sizeof(CCB_SCSIIO);
	ccb.cam_ch.cam_func_code = XPT_SCSI_IO;
	ccb.cam_ch.cam_path_id = DEV_BUS_ID(minor(sbuf.st_rdev));
	ccb.cam_ch.cam_target_id = DEV_TARGET(minor(sbuf.st_rdev));
	ccb.cam_ch.cam_target_lun = DEV_LUN(minor(sbuf.st_rdev));
	if (flags & SCSI_IN)
		ccb.cam_ch.cam_flags |= CAM_DIR_IN;
	if (flags & SCSI_OUT)
		ccb.cam_ch.cam_flags |= CAM_DIR_OUT;
	if (flags & SCSI_NONE)
		ccb.cam_ch.cam_flags |= CAM_DIR_NONE;
	if (flags & SCSI_SYNC)
		ccb.cam_ch.cam_flags |= CAM_INITIATE_SYNC;
	ccb.cam_data_ptr = (u_char *)buffer;
	ccb.cam_dxfer_len = buflen;
	ccb.cam_timeout = timeout/1000;
	ccb.cam_cdb_len = cdblen;
	ccb.cam_sense_ptr = (u_char *)sense;
	ccb.cam_sense_len = senselen;
	memcpy (ccb.cam_cdb_io.cam_cdb_bytes, cdb, cdblen);

	memset ((caddr_t)&ua_ccb, 0, sizeof(ua_ccb));
	ua_ccb.uagt_ccb = (CCB_HEADER *)&ccb;
	ua_ccb.uagt_ccblen = sizeof(ccb);
	ua_ccb.uagt_buffer = (u_char *)buffer;
	ua_ccb.uagt_buflen = buflen;
	ua_ccb.uagt_snsbuf = (u_char *)sense;
	ua_ccb.uagt_snslen = senselen;
	ua_ccb.uagt_cdb = (CDB_UN *)NULL;
	ua_ccb.uagt_cdblen = 0;

	if (ioctl(fd, UAGT_CAM_IO, (caddr_t)&ua_ccb) < 0) {
		*msgaddr = strerror(errno);
		serrno = errno;
		USRMSG (TP042, "/dev/cam", "ioctl", *msgaddr);
		if (! do_not_open) close (fd);
		return (-2);
	}
        /* CAM_SIM_QFRZN always set with Version 4.0 in case of failure,
	   thus release SIM queue mandatory */
        /* Not always set under Digital Unix 3.2 */
	if (ccb.cam_ch.cam_status & CAM_SIM_QFRZN) {
		CCB_RELSIM ccb_sim_rel;      /* RELEASE SIMQUE CCB */
		UAGT_CAM_CCB ua_ccb_sim_rel;

		memset ((caddr_t)&ccb_sim_rel, 0, sizeof(ccb_sim_rel));
		ccb_sim_rel.cam_ch.my_addr = (struct ccb_header *)&ccb_sim_rel;
		ccb_sim_rel.cam_ch.cam_ccb_len = sizeof(CCB_RELSIM);
		ccb_sim_rel.cam_ch.cam_func_code = XPT_REL_SIMQ;
		ccb_sim_rel.cam_ch.cam_path_id = DEV_BUS_ID(sbuf.st_rdev);
		ccb_sim_rel.cam_ch.cam_target_id = DEV_TARGET(sbuf.st_rdev);
		ccb_sim_rel.cam_ch.cam_target_lun = DEV_LUN(sbuf.st_rdev);
		ccb_sim_rel.cam_ch.cam_flags = CAM_DIR_NONE;	/* No data */

		memset ((caddr_t)&ua_ccb_sim_rel, 0, sizeof(ua_ccb_sim_rel));
		ua_ccb_sim_rel.uagt_ccb = (CCB_HEADER *)&ccb_sim_rel;
		ua_ccb_sim_rel.uagt_ccblen = sizeof(CCB_RELSIM);
		ua_ccb_sim_rel.uagt_buffer = (u_char *)NULL;
		ua_ccb_sim_rel.uagt_buflen = 0;
		ua_ccb_sim_rel.uagt_snsbuf = (u_char *)NULL;
		ua_ccb_sim_rel.uagt_snslen = 0;
		ua_ccb_sim_rel.uagt_cdb    = (CDB_UN *)NULL;  /* CDB is in the CCB */
		ua_ccb_sim_rel.uagt_cdblen = 0;

		if (ioctl (fd, UAGT_CAM_IO, (caddr_t)&ua_ccb_sim_rel) < 0)
			rsqfailed = 1;
	}
	if (! do_not_open) close (fd);
	*nb_sense_ret = senselen - ccb.cam_sense_resid;
	cam_status = ccb.cam_ch.cam_status & CAM_STATUS_MASK;
	if (cam_status != CAM_REQ_CMP && cam_status != CAM_REQ_CMP_ERR) {
		get_cs_msg (cam_status, msgaddr);
		if (rsqfailed) {
			strcpy (xmsgbuf, *msgaddr);
			strcat (xmsgbuf, rsq_failed_msg);
			*msgaddr = xmsgbuf;
		}
		serrno = EIO;
		USRMSG (TP042, "/dev/cam", "ioctl", *msgaddr);
		return (-3);
	}
	if (ccb.cam_scsi_status) {
		if (ccb.cam_scsi_status == SCSI_STATUS_CHECK_CONDITION &&
		    *nb_sense_ret >= 14) {
			sprintf (err_msgbuf, "%s ASC=%X ASCQ=%X",
			    sk_msg[*(sense+2) & 0xF], *(sense+12), *(sense+13));
			*msgaddr = err_msgbuf;
		} else
			get_ss_msg (ccb.cam_scsi_status, msgaddr);
		if (rsqfailed) {
			strcpy (xmsgbuf, *msgaddr);
			strcat (xmsgbuf, rsq_failed_msg);
			*msgaddr = xmsgbuf;
		}
		serrno = EIO;
		USRMSG (TP042, "/dev/cam", "ioctl", *msgaddr);
		return (-4);
	}
	return (buflen - ccb.cam_resid);
#else
#if defined(linux)
	int fd;
	FILE *fopen();
	int n;
	char pssline[80];
	int resid = 0;
	struct stat sbuf;
	struct stat sbufa;
	static char *sg_buffer;
	static int sg_bufsiz = 0;
	struct sg_header *sg_hd;
	int sg_index;
	FILE *sgf;
	char sgpath[80];
	int st_index;
	static int Timeout = 0;

	if (sizeof(struct sg_header) + cdblen + buflen > SG_BIG_BUFF) {
#if defined(TAPE)
		sprintf (tp_err_msgbuf, "blocksize too large (max %d)\n",
		    SG_BIG_BUFF - sizeof(struct sg_header) - cdblen);
		*msgaddr = tp_err_msgbuf;
#else
		sprintf (err_msgbuf, "blocksize too large (max %d)",
		    SG_BIG_BUFF - sizeof(struct sg_header) - cdblen);
		*msgaddr = err_msgbuf;
#endif
		serrno = EINVAL;
		return (-1);
	}
	if (sizeof(struct sg_header)+cdblen+buflen > sg_bufsiz) {
		if (sg_bufsiz > 0) free (sg_buffer);
		if ((sg_buffer = malloc (sizeof(struct sg_header)+cdblen+buflen)) == NULL) {
			serrno = errno;
#if defined(TAPE)
			sprintf (tp_err_msgbuf, TP005);
			*msgaddr = tp_err_msgbuf;
#else
			sprintf (err_msgbuf, "malloc error: %s", strerror(errno));
			*msgaddr = err_msgbuf;
#endif
			return (-1);
		}
		sg_bufsiz = sizeof(struct sg_header) + cdblen + buflen;
	}
	if (do_not_open) {
		fd = tapefd;
		strcpy (sgpath, path);
	} else {
		if (stat (path, &sbuf) < 0) {
			serrno = errno;
#if defined(TAPE)
			USRMSG (TP042, path, "stat", strerror(errno));
#else
			sprintf (err_msgbuf, "stat error: %s", strerror(errno));
			*msgaddr = err_msgbuf;
#endif
			return (-1);
		}
		if (stat ("/dev/sga", &sbufa) < 0) {
			serrno = errno;
#if defined(TAPE)
			USRMSG (TP042, "/dev/sga", "stat", strerror(errno));
#else
			sprintf (err_msgbuf, "stat error: %s", strerror(errno));
			*msgaddr = err_msgbuf;
#endif
			return (-1);
		}
		if (major (sbuf.st_rdev) == major (sbufa.st_rdev)) {
			strcpy (sgpath, path);
		} else {
			sg_index = -1;
			st_index = -1;
			sgf = fopen ("/proc/scsi/scsi", "r");
			while (fgets (pssline, sizeof(pssline), sgf)) {
				if (strncmp (pssline, "  Type:", 7)) continue;
				sg_index++;
				if (strncmp (pssline+10, "Sequential-Access", 17)) continue;
				st_index++;
				if (st_index == (sbuf.st_rdev & 0x1F)) break;
			}
			fclose (sgf);
			sprintf (sgpath, "/dev/sg%c", sg_index + 'a');
		}
		if ((fd = open (sgpath, O_RDWR)) < 0) {
			serrno = errno;
#if defined(TAPE)
			USRMSG (TP042, sgpath, "open", strerror(errno));
#else
			sprintf (err_msgbuf, "open error: %s", strerror(errno));
			*msgaddr = err_msgbuf;
#endif
			return (-1);
		}
	}
	if (timeout != (Timeout * 10)) {
		Timeout = timeout / 10;
		ioctl (fd, SG_SET_TIMEOUT, &Timeout);
	}
	memset (sg_buffer, 0, sizeof(struct sg_header));
	sg_hd = (struct sg_header *) sg_buffer;
	sg_hd->reply_len = sizeof(struct sg_header) + ((flags & SCSI_IN) ? buflen : 0);
	sg_hd->twelve_byte = cdblen == 12;
	memcpy (sg_buffer+sizeof(struct sg_header), cdb, cdblen);
	n = sizeof(struct sg_header) + cdblen;
	if (buflen && (flags & SCSI_OUT)) {
		memcpy (sg_buffer+n, buffer, buflen);
		n+= buflen;
	}
	if (write (fd, sg_buffer, n) < 0) {
		*msgaddr = (char *) strerror(errno);
		serrno = errno;
		USRMSG (TP042, sgpath, "write", *msgaddr);
		if (! do_not_open) close (fd);
		return (-2);
	}
	if ((n = read (fd, sg_buffer, sizeof(struct sg_header) +
	    ((flags & SCSI_IN) ? buflen : 0))) < 0) {
		*msgaddr = (char *) strerror(errno);
		serrno = errno;
		USRMSG (TP042, sgpath, "read", *msgaddr);
		if (! do_not_open) close (fd);
		return (-2);
	}
	if (! do_not_open) close (fd);
	if (sg_hd->sense_buffer[0]) {
		memcpy (sense, sg_hd->sense_buffer, sizeof(sg_hd->sense_buffer));
		*nb_sense_ret = sizeof(sg_hd->sense_buffer);
	}
	if (sg_hd->sense_buffer[0] & 0x80) {	/* valid */
		resid = sg_hd->sense_buffer[3] << 24 | sg_hd->sense_buffer[4] << 16 |
		    sg_hd->sense_buffer[5] << 8 | sg_hd->sense_buffer[6];
	}
	if ((sg_hd->sense_buffer[0] & 0x70) &&
	    ((sg_hd->sense_buffer[2] & 0xE0) == 0 ||
	    (sg_hd->sense_buffer[2] & 0xF) != 0)) {
		sprintf (err_msgbuf, "%s ASC=%X ASCQ=%X",
		    sk_msg[*(sense+2) & 0xF], *(sense+12), *(sense+13));
		*msgaddr = err_msgbuf;
		serrno = EIO;
		USRMSG (TP042, sgpath, "scsi", *msgaddr);
		return (-4);
	} else if (sg_hd->result) {
		*msgaddr = (char *) strerror(sg_hd->result);
		serrno = sg_hd->result;
		USRMSG (TP042, sgpath, "read", *msgaddr);
		return (-2);
	}
	if (n)
		n -= sizeof(struct sg_header) + resid;
	if (n && (flags & SCSI_IN))
		memcpy (buffer, sg_buffer+sizeof(struct sg_header), n);
	return ((flags & SCSI_IN) ? n : buflen - resid);
#else
	*msgaddr = notsupp;
	serrno = SEOPNOTSUP;
	USRMSG (TP042, path, "ioctl", *msgaddr);
	return (-5);
#endif
#endif
#endif
#endif
#endif
}

#if defined(__osf__) && defined(__alpha)
get_cs_msg(cam_status, msgaddr)
int cam_status;
char **msgaddr;
{
	int i;

	for (i = 0; i < 256; i++) {
		if (cam_codmsg[i].status == 0xFF) {
			sprintf (err_msgbuf,
			    "Undefined CAM status %02X", cam_status);
			*msgaddr = err_msgbuf;
			break;
		} else if (cam_status == cam_codmsg[i].status) {
			*msgaddr = cam_codmsg[i].text;
			break;
		}
	}
}
#endif

get_ss_msg(scsi_status, msgaddr)
int scsi_status;
char **msgaddr;
{
	int i;

	for (i = 0; i < 256; i++) {
		if (scsi_codmsg[i].status == 0xFF) {
			sprintf (err_msgbuf,
			    "Undefined SCSI status %02X", scsi_status);
			*msgaddr = err_msgbuf;
			break;
		} else if (scsi_status == scsi_codmsg[i].status) {
			*msgaddr = scsi_codmsg[i].text;
			break;
		}
	}
}
