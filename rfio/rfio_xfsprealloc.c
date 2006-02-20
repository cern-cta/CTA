/* RFIO XFS preallocation support */
/* 2006/02/15 KELEMEN Peter <Peter.Kelemen@cern.ch> CERN IT/FIO/LA */

#include <xfs/libxfs.h>
#include "log.h"
#include "rfio_xfsprealloc.h"

void
rfio_xfs_resvsp64(int fd, unsigned long mbytes)
{
	int err;
	xfs_flock64_t fl;

	if (mbytes == 0) {
		log(LOG_DEBUG, "%s: fd %d, 0 MB, ignored\n", __func__, fd);
		goto bailout;
	}

	if ( platform_test_xfs_fd(fd) == 0 ) {
		log(LOG_DEBUG, "%s: fd %d, not on XFS\n", __func__, fd);
		goto bailout;
	}

	fl.l_whence = SEEK_SET;
	fl.l_start = 1;
	fl.l_len = ((off64_t)mbytes * 1024 * 1024ULL);

	err = xfsctl(NULL, fd, XFS_IOC_RESVSP64, &fl);
	if (err < 0) {
		log(LOG_ERR, "%s: fd %d, %ld MB, error %d\n",
			__func__, fd, mbytes, errno);
		goto bailout;
	}
	log(LOG_DEBUG, "%s: fd %d, %ld MB, success\n", __func__, fd, mbytes);
bailout:
	return;
}

void
rfio_xfs_unresvsp64(int fd, unsigned long mbytes, off64_t written)
{
	int err;
	xfs_flock64_t fl;
	off64_t bytes;

	if (mbytes == 0) {
		log(LOG_DEBUG, "%s: fd %d, 0 MB, ignored\n", __func__, fd);
		goto bailout;
	}

	bytes = mbytes * 1024 * 1024ULL;
	log(LOG_DEBUG, "%s: fd %d, %lld bytes reservation\n", __func__, fd, bytes);
	log(LOG_DEBUG, "%s: fd %d, %lld bytes written\n", __func__, fd, written);

	if (written >= bytes) {
		log(LOG_DEBUG, "%s: fd %d, larger or equal to %ld MB, nothing to do\n",
			__func__, fd, mbytes);
		goto bailout;
	}

	if ( platform_test_xfs_fd(fd) == 0 ) {
		log(LOG_DEBUG, "%s: fd %d, not on XFS\n", __func__, fd);
		goto bailout;
	}

	bytes = bytes - written;	/* remaining bytes to unreserve */
	log(LOG_DEBUG, "%s: fd %d, %lld bytes remaining\n", __func__, fd, bytes);
	fl.l_whence = SEEK_SET;
	fl.l_start = written;
	fl.l_len = bytes;

	err = xfsctl(NULL, fd, XFS_IOC_UNRESVSP64, &fl);
	if (err < 0) {
		log(LOG_ERR, "%s: fd %d, %lld bytes, error %d\n",
			__func__, fd, bytes, errno);
		goto bailout;
	}
	log(LOG_DEBUG, "%s: fd %d, %lld bytes, success\n", __func__, fd, bytes);

bailout:
	return;
}

/* eof */
