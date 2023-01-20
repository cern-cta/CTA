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
#include "mediachanger/castorrmc/h/scsictl.h"
#include "mediachanger/castorrmc/h/serrno.h"
#include "mediachanger/castorrmc/h/rmc_send_scsi_cmd.h"
static char rmc_err_msgbuf[132];
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
        char syspath[256];

        char tlink[256];
        char glink[256];

        int match = 0;
        DIR *dir_tape, *dir_gen;
        struct dirent *dirent;
        char st_dev[64];

        struct stat sbuf;

        sgpath[0] = '\0';

        /* find the st sysfs entry */
        if (!(dir_tape = opendir(systape))) return;
        while ((dirent = readdir(dir_tape))) {

                if (0 == strcmp(".", dirent->d_name)) continue;
                if (0 == strcmp("..", dirent->d_name)) continue;

                sprintf(st_dev, "/dev/%s", dirent->d_name);
                stat(st_dev, &sbuf);
                if (maj == (int)major(sbuf.st_rdev) && min == (int)minor(sbuf.st_rdev)) {
                        sprintf(syspath, "%s/%s/device", systape, dirent->d_name);
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

                sprintf(syspath, "%s/%s/device", sysgen, dirent->d_name);

                memset(glink, 0, 256);
                readlink(syspath, glink, 256);

                if (0 == strcmp(glink, tlink)) {
                        sprintf(sgpath, "/dev/%s", dirent->d_name);
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
	struct stat sbufa;
	static char *sg_buffer;
	static int sg_bufsiz = 0;
	struct sg_header *sg_hd;
	char sgpath[80];
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
		sprintf (rmc_err_msgbuf, "blocksize too large (max %zd)\n",
		    sg_big_buff_val - sizeof(struct sg_header) - cdblen);
		*msgaddr = rmc_err_msgbuf;
		serrno = EINVAL;
		return (-1);
	}
	if ((int)sizeof(struct sg_header)+cdblen+buflen > sg_bufsiz) {
		if (sg_bufsiz > 0) free (sg_buffer);
		if ((sg_buffer = (char *)malloc (sizeof(struct sg_header)+cdblen+buflen)) == NULL) {
			serrno = errno;
			sprintf (rmc_err_msgbuf, "cannot get memory");
			*msgaddr = rmc_err_msgbuf;
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
        		snprintf (rmc_err_msgbuf, sizeof(rmc_err_msgbuf), "%s : stat error : %s\n", path, strerror(errno));
			rmc_err_msgbuf[sizeof(rmc_err_msgbuf) - 1] = '\0';
        		*msgaddr = rmc_err_msgbuf;
			return (-1);
		}

                /* get the major device ID of the sg devices ... */
		if (stat ("/dev/sg0", &sbufa) < 0) {
			serrno = errno;
			snprintf (rmc_err_msgbuf, sizeof(rmc_err_msgbuf), "/dev/sg0 : stat error : %s\n", strerror(errno));
			rmc_err_msgbuf[sizeof(rmc_err_msgbuf) - 1] = '\0';
			*msgaddr = rmc_err_msgbuf;
			return (-1);
		}
                /* ... to detect links and use the path directly! */
		if (major(sbuf.st_rdev) == major(sbufa.st_rdev)) {
			strcpy (sgpath, path);
		} else {
                        find_sgpath(sgpath, major(sbuf.st_rdev), minor(sbuf.st_rdev));
		}

		if ((fd = open (sgpath, O_RDWR)) < 0) {
			serrno = errno;
			snprintf (rmc_err_msgbuf, sizeof(rmc_err_msgbuf), "%s : open error : %s\n", sgpath, strerror(errno));
			rmc_err_msgbuf[sizeof(rmc_err_msgbuf) - 1] = '\0';
			*msgaddr = rmc_err_msgbuf;
			return (-1);
		}
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
		snprintf (rmc_err_msgbuf, sizeof(rmc_err_msgbuf), "%s : write error : %s\n", sgpath, *msgaddr);
		rmc_err_msgbuf[sizeof(rmc_err_msgbuf) - 1] = '\0';
		*msgaddr = rmc_err_msgbuf;
		if (! do_not_open) close (fd);
		return (-2);
	}
	if ((n = read (fd, sg_buffer, sizeof(struct sg_header) +
	    ((flags & SCSI_IN) ? buflen : 0))) < 0) {
		*msgaddr = (char *) strerror(errno);
		serrno = errno;
		snprintf (rmc_err_msgbuf, sizeof(rmc_err_msgbuf), "%s : read error : %s\n", sgpath, *msgaddr);
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
		snprintf (rmc_err_msgbuf, sizeof(rmc_err_msgbuf), "%s : scsi error : %s\n", sgpath, tmp_msgbuf);
		rmc_err_msgbuf[sizeof(rmc_err_msgbuf) - 1] = '\0';
		*msgaddr = rmc_err_msgbuf;
		return (-4);
	} else if (sg_hd->result) {
		*msgaddr = (char *) strerror(sg_hd->result);
		serrno = sg_hd->result;
		snprintf (rmc_err_msgbuf, sizeof(rmc_err_msgbuf), "%s : read error : %s\n", sgpath, *msgaddr);
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
