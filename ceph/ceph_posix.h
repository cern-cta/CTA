
/*
 * This interface provides wrapper methods for using ceph through a POSIX API.
 * It will actually check whether the pathname given at open time starts with
 * a '/' or not. If yes, if will call standard POSIX call and use local files.
 * If no, it will consider the pathname as an object name for ceph
 */

#ifndef _CEPH_POSIX_H
#define _CEPH_POSIX_H

#include <sys/types.h>
#include <stdarg.h>

#ifdef __cplusplus
extern "C" {
#endif

  void ceph_posix_disconnect_all();
  void ceph_posix_set_logfunc(void (*logfunc) (char *, va_list argp));
  int ceph_posix_open(const char *pathname, int flags, mode_t mode);
  int ceph_posix_close(int fd);
  off_t ceph_posix_lseek(int fd, off_t offset, int whence);
  off64_t ceph_posix_lseek64(int fd, off64_t offset, int whence);
  ssize_t ceph_posix_write(int fd, const void *buf, size_t count);
  ssize_t ceph_posix_read(int fd, void *buf, size_t count);
  int ceph_posix_fstat(int fd, struct stat *buf);
  int ceph_posix_stat(const char *pathname, struct stat *buf);
  int ceph_posix_fstat64(int fd, struct stat64 *buf);
  int ceph_posix_stat64(const char *pathname, struct stat64 *buf);
  int ceph_posix_fsync(int fd);
  int ceph_posix_fcntl(int fd, int cmd, ... /* arg */ );
  ssize_t ceph_posix_fgetxattr(int fd, const char* name, void* value, size_t size);
  int ceph_posix_fsetxattr(int fd, const char* name, const void* value, size_t size, int flags);
  int ceph_posix_fremovexattr(int fd, const char* name);

#ifdef __cplusplus
}
#endif

#endif
