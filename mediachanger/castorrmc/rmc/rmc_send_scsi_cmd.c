/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2002-2022 CERN
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

/*	rmc_send_scsi_cmd - Send a SCSI command to a device */
/*	return	-5	if not supported on this platform (serrno = SEOPNOTSUP)
 *		-4	if SCSI error (serrno = EIO)
 *		-3	if CAM error (serrno = EIO)
 *		-2	if ioctl fails with errno (serrno = errno)
 *		-1	if open/stat fails with errno (message fully formatted)
 *		 0	if successful with no data transfer
 *		>0	number of bytes transferred
 */

#include <unistd.h>
#include <sys/ioctl.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <fcntl.h>
#include <stdlib.h>
#include <dirent.h>
#include <sys/stat.h>
#include <linux/version.h>
#include <sys/param.h>
/* Impossible unless very very old kernels: */
#ifndef KERNEL_VERSION
#define KERNEL_VERSION(a,b,c) (((a) << 16) + ((b) << 8) + (c))
#endif
#include "/usr/include/scsi/sg.h"
#include <sys/stat.h>
#include "scsictl.h"
#include "serrno.h"
#include "rmc_send_scsi_cmd.h"

#define RMC_ERR_MSG_BUFSZ 132
#define ST_DEV_BUFSZ 64
#define SYSPATH_BUFSZ 256
#define SGPATH_BUFSZ 80

static char rmc_err_msgbuf[RMC_ERR_MSG_BUFSZ];
static const char *sk_msg[] = {
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

static void find_sgpath(char *const sgpath, const int maj, const int min) {

        /*
          Find the sg device for a pair of major and minor device IDs
          of a tape device. The match is done by

          . identifying the tape's st device node
          . getting the device's unique ID from sysfs
          . searching the sg device with the same ID (in sysfs)

          If no match is found, the returned sg path will be an empty
          string.
        */

        char systape[] = "/sys/class/scsi_tape";
        char sysgen[]  = "/sys/class/scsi_generic";
        char syspath[SYSPATH_BUFSZ];

        char tlink[256];
        char glink[256];

        int match = 0;
        int retval;
        DIR *dir_tape, *dir_gen;
        struct dirent *dirent;
        char st_dev[ST_DEV_BUFSZ];

        struct stat sbuf;

        sgpath[0] = '\0';

        /* find the st sysfs entry */
        if (!(dir_tape = opendir(systape))) return;
        while ((dirent = readdir(dir_tape))) {

                if (0 == strcmp(".", dirent->d_name)) continue;
                if (0 == strcmp("..", dirent->d_name)) continue;

                retval = snprintf(st_dev, ST_DEV_BUFSZ, "/dev/%s", dirent->d_name);
                if(retval < 0) {
                  /* Buffer overflow */
                  exit(ENOBUFS);
                }
                stat(st_dev, &sbuf);
                if (maj == (int)major(sbuf.st_rdev) && min == (int)minor(sbuf.st_rdev)) {
                        retval = snprintf(syspath, SYSPATH_BUFSZ, "%s/%s/device", systape, dirent->d_name);
                        if(retval < 0) {
                          /* Buffer overflow */
                          exit(ENOBUFS);
                        }
                        match = 1;
                        break;
                }
        }
        closedir(dir_tape);

        if (0 == match) return;

        memset(tlink, 0, 256);
        readlink(syspath, tlink, 256);

        /* find the corresponding sg sysfs entry */
        if (!(dir_gen = opendir(sysgen))) return;
        while ((dirent = readdir(dir_gen))) {

                if (0 == strcmp(".", dirent->d_name)) continue;
                if (0 == strcmp("..", dirent->d_name)) continue;

                retval = snprintf(syspath, SYSPATH_BUFSZ, "%s/%s/device", sysgen, dirent->d_name);
                if(retval < 0) {
                  /* Buffer overflow */
                  exit(ENOBUFS);
                }

                memset(glink, 0, 256);
                readlink(syspath, glink, 256);

                if (0 == strcmp(glink, tlink)) {
                        retval = snprintf(sgpath, SGPATH_BUFSZ, "/dev/%s", dirent->d_name);
                        if(retval < 0) {
                          /* Buffer overflow */
                          exit(ENOBUFS);
                        }
                        goto out;
                }
        }
 out:
        closedir(dir_gen);
        return;
}


int rmc_send_scsi_cmd (
	const int tapefd,
	const char *const path,
	const int do_not_open,
	const unsigned char *const cdb,
	const int cdblen,
	unsigned char *const buffer,
	const int buflen,
	char *const sense,
	const int senselen,
	const int flags,
	int *const nb_sense_ret,
	const char **const msgaddr)
{
	/* The timeout used when sending SCSI commands through the sg driver is in */
	/* milliseconds and should equal that used by the st driver which on the   */
	/* 28/01/2014 is 900 seconds and therefore 900000 milliseconds             */
	const int timeout = 900000; /* milliseconds */
	int fd;
	FILE *fopen();
	int n;
	int resid = 0;
	struct stat sbuf;
	static char *sg_buffer;
	static int sg_bufsiz = 0;
	struct sg_header *sg_hd;
	char sgpath[SGPATH_BUFSZ];
	int timeout_in_jiffies = 0;
	int sg_big_buff_val =  SG_BIG_BUFF;
	int procfd, nbread;
	char procbuf[80];

  (void)senselen;
	/* First the value in /proc of the max buffer size for the sg driver */
	procfd = open("/proc/scsi/sg/def_reserved_size", O_RDONLY);
	if (procfd >= 0) {
	  memset(procbuf, 0, sizeof(procbuf));
	  nbread = read(procfd, procbuf, sizeof(procbuf) - 1);
	  if (nbread > 0) {
	    long int tmp;
	    char *endptr = NULL;
	    tmp = strtol(procbuf, &endptr, 10);
	    if (endptr == NULL || *endptr == '\n') {
	      sg_big_buff_val = (int) tmp;
	    }
	  }
	  close(procfd);
	}

	if ((int)sizeof(struct sg_header) + cdblen + buflen > sg_big_buff_val) {
		snprintf(rmc_err_msgbuf, RMC_ERR_MSG_BUFSZ, "blocksize too large (max %zd)\n",
		    sg_big_buff_val - sizeof(struct sg_header) - cdblen);
		*msgaddr = rmc_err_msgbuf;
		serrno = EINVAL;
		return (-1);
	}
	if ((int)sizeof(struct sg_header)+cdblen+buflen > sg_bufsiz) {
		if (sg_bufsiz > 0) free (sg_buffer);
		if ((sg_buffer = (char *)malloc (sizeof(struct sg_header)+cdblen+buflen)) == NULL) {
			serrno = errno;
			snprintf(rmc_err_msgbuf, RMC_ERR_MSG_BUFSZ, "cannot get memory");
			*msgaddr = rmc_err_msgbuf;
			return (-1);
		}
		sg_bufsiz = sizeof(struct sg_header) + cdblen + buflen;
	}
	if (do_not_open) {
		fd = tapefd;
		strncpy(sgpath, path, SGPATH_BUFSZ);
	} else {
		if (stat (path, &sbuf) < 0) {
			serrno = errno;
        		snprintf(rmc_err_msgbuf, RMC_ERR_MSG_BUFSZ, "%s : stat error : %s\n", path, strerror(errno));
			rmc_err_msgbuf[sizeof(rmc_err_msgbuf) - 1] = '\0';
        		*msgaddr = rmc_err_msgbuf;
			return (-1);
		}

		struct stat sbufa;
		if (stat("/dev/sg0", &sbufa) == 0 && major(sbuf.st_rdev) == major(sbufa.st_rdev)) {
		  /* If the major device ID of the specified device is the same as the major device ID of sg0,
                   * we can use the path directly */
		  strncpy(sgpath, path, SGPATH_BUFSZ);
		} else {
		  /* Otherwise, look up the path using the (major,minor) device ID. If no match is found,
                   * sgpath is set to an empty string */
		  find_sgpath(sgpath, major(sbuf.st_rdev), minor(sbuf.st_rdev));
		}

		if ((fd = open (sgpath, O_RDWR)) < 0) {
			serrno = errno;
			snprintf(rmc_err_msgbuf, RMC_ERR_MSG_BUFSZ, "%s : open error : %s\n", sgpath, strerror(errno));
			rmc_err_msgbuf[RMC_ERR_MSG_BUFSZ-1] = '\0';
			*msgaddr = rmc_err_msgbuf;
			return (-1);
		}
	}
	if(sg_buffer == NULL) {
		serrno = EFAULT;
		snprintf(rmc_err_msgbuf, RMC_ERR_MSG_BUFSZ, "sg_buffer points to NULL");
		*msgaddr = rmc_err_msgbuf;
		return -1;
	}

        /* set the sg timeout (in jiffies) */
        timeout_in_jiffies = timeout * HZ / 1000;
        ioctl (fd, SG_SET_TIMEOUT, &timeout_in_jiffies);

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
		snprintf(rmc_err_msgbuf, RMC_ERR_MSG_BUFSZ, "%s : write error : %s\n", sgpath, *msgaddr);
		rmc_err_msgbuf[sizeof(rmc_err_msgbuf) - 1] = '\0';
		*msgaddr = rmc_err_msgbuf;
		if (! do_not_open) close (fd);
		return (-2);
	}
	if ((n = read (fd, sg_buffer, sizeof(struct sg_header) +
	    ((flags & SCSI_IN) ? buflen : 0))) < 0) {
		*msgaddr = (char *) strerror(errno);
		serrno = errno;
		snprintf(rmc_err_msgbuf, RMC_ERR_MSG_BUFSZ, "%s : read error : %s\n", sgpath, *msgaddr);
		rmc_err_msgbuf[sizeof(rmc_err_msgbuf) - 1] = '\0';
		*msgaddr = rmc_err_msgbuf;
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
		char tmp_msgbuf[32];
		snprintf (tmp_msgbuf, sizeof(tmp_msgbuf), "%s ASC=%X ASCQ=%X",
		    sk_msg[*(sense+2) & 0xF], *(sense+12), *(sense+13));
		tmp_msgbuf[sizeof(tmp_msgbuf) - 1] = '\0';
		serrno = EIO;
		snprintf(rmc_err_msgbuf, RMC_ERR_MSG_BUFSZ, "%s : scsi error : %s\n", sgpath, tmp_msgbuf);
		rmc_err_msgbuf[sizeof(rmc_err_msgbuf) - 1] = '\0';
		*msgaddr = rmc_err_msgbuf;
		return (-4);
	} else if (sg_hd->result) {
		*msgaddr = (char *) strerror(sg_hd->result);
		serrno = sg_hd->result;
		snprintf(rmc_err_msgbuf, RMC_ERR_MSG_BUFSZ, "%s : read error : %s\n", sgpath, *msgaddr);
		rmc_err_msgbuf[sizeof(rmc_err_msgbuf) - 1] = '\0';
		*msgaddr = rmc_err_msgbuf;
		return (-2);
	}
	if (n)
		n -= sizeof(struct sg_header) + resid;
	if (n && (flags & SCSI_IN))
		memcpy (buffer, sg_buffer+sizeof(struct sg_header), n);
	return ((flags & SCSI_IN) ? n : buflen - resid);
}
