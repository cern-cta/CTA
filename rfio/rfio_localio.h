/*
 * This interface provides generic methods for local IO
 */

#include <sys/types.h>

int generic_open(const char *pathname, int flags, mode_t mode);
int generic_close(int fd);
off64_t generic_lseek64(int fd, off64_t offset, int whence);
ssize_t generic_write(int fd, const void *buf, size_t count);
ssize_t generic_read(int fd, const void *buf, size_t count);
int generic_fstat(int fd, struct stat *buf);
int generic_fstat64(int fd, struct stat64 *buf);
int generic_fsync(int fd);
int generic_fcntl(int fd, int cmd, ... /* arg */ );
ssize_t fgetxattr(int fd, const char* name, void* value, size_t size);
int fsetxattr(int fd, const char* name, const void* value, size_t size, int flags);
int removexattr(int fd, const char* name);
