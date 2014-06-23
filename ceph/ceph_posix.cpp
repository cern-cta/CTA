
/*
 * This file provides a generic interface looking like POSIX local I/O
 * In reality, it can also talk to ceph, wrapping around the CEPH/RADOS C++ api
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdarg.h>
#include <radosstriper/libradosstriper.hpp>
#include <map>
#include <string>
#include <sys/xattr.h>
#include <ceph/ceph_posix.h>

/// small structs to store file data, either for CEPH or for a local file
struct CephFileRef {
  std::string name;
  std::string pool;
  int flags;
  mode_t mode;
  unsigned long long offset;
};

struct FileRef {
  bool isCeph;
  CephFileRef cephFile; // only valid for ceph files
  int fd;               // only valid for local files
};

/// global variables holding stripers for each ceph pool
std::map<std::string, libradosstriper::RadosStriper*> g_radosStripers;
/// global variables holding a map of file descriptor to file reference
std::map<unsigned int, FileRef> g_fds;
/// global variable remembering the next available file descriptor
unsigned int g_nextCephFd = 0;

/// global variable for the log function. Defaults to one going to dev/null
static void (*g_logfunc) (char *, va_list argp) = 0;

static void logwrapper(char* format, ...) {
  if (0 == g_logfunc) return;
  va_list arg;
  va_start(arg, format);
  (*g_logfunc)(format, arg);
  va_end(arg);  
}

static libradosstriper::RadosStriper* getRadosStriper(std::string pool) {
  std::map<std::string, libradosstriper::RadosStriper*>::iterator it =
    g_radosStripers.find(pool);
  if (it == g_radosStripers.end()) {
    // we need to create a new radosStriper
    librados::Rados cluster;
    int rc = cluster.init(0);
    if (rc) return 0;
    rc = cluster.conf_read_file(NULL);
    if (rc) {
      cluster.shutdown();
      return 0;
    }
    cluster.conf_parse_env(NULL);
    rc = cluster.connect();
    if (rc) {
      cluster.shutdown();
      return 0;
    }
    librados::IoCtx ioctx;
    rc = cluster.ioctx_create(pool.c_str(), ioctx);
    if (rc != 0) return 0;
    libradosstriper::RadosStriper *newStriper = new libradosstriper::RadosStriper;
    rc = libradosstriper::RadosStriper::striper_create(ioctx, newStriper);
    if (rc != 0) return 0;
    it = g_radosStripers.insert(std::pair<std::string, libradosstriper::RadosStriper*>
                                (pool, newStriper)).first;
  }
  return it->second;
}

void ceph_open(CephFileRef &fr, const char *pathname, int flags, mode_t mode) {
  std::string path = pathname;
  int slashPos = path.find('/');
  fr.pool = path.substr(0,slashPos);
  fr.name = path.substr(slashPos+1);
  fr.flags = flags;
  fr.mode = mode;
  fr.offset = 0;
}

int ceph_close(CephFileRef &fr) {
  return 0;
}

off64_t ceph_lseek64(CephFileRef &fr, off64_t offset, int whence) {
  switch (whence) {
  case SEEK_SET:
    fr.offset = offset;
    break;
  case SEEK_CUR:
    fr.offset += offset;
    break;
  default:
    errno = EINVAL;
    return -1;
  }
  return 0;
}

ssize_t ceph_write(CephFileRef &fr, const char *buf, size_t count) {
  if ((fr.flags & O_WRONLY) == 0) {
    errno = EBADF;
    return -1;
  }
  libradosstriper::RadosStriper *striper = getRadosStriper(fr.pool);
  if (0 == striper) {
    errno = EINVAL;
    return -1;
  }
  ceph::bufferlist bl;
  bl.append(buf, count);
  if (striper->write(fr.name, bl, count, fr.offset)) {
    return -1;
  }
  fr.offset += count;
  return count;
}

ssize_t ceph_read(CephFileRef &fr, char *buf, size_t count) {
  if ((fr.flags & O_WRONLY) != 0) {
    errno = EBADF;
    return -1;
  }
  libradosstriper::RadosStriper *striper = getRadosStriper(fr.pool);
  if (0 == striper) {
    errno = EINVAL;
    return -1;
  }
  ceph::bufferlist bl;
  int rc = striper->read(fr.name, &bl, count, fr.offset);
  if (rc < 0) return rc;
  bl.copy(0, rc, buf);
  fr.offset += rc;
  return rc;
}

int ceph_fstat(CephFileRef &fr, struct stat *buf) {
  // minimal stat : only size and times are filled
  libradosstriper::RadosStriper *striper = getRadosStriper(fr.pool);
  if (0 == striper) {
    errno = EINVAL;
    return -1;
  }
  memset(buf, 0, sizeof(*buf));
  int rc = striper->stat(fr.name, (uint64_t*)&(buf->st_size), &(buf->st_atime));
  if (rc != 0) {
    errno = -rc;
    return -1;
  }
  buf->st_mtime = buf->st_atime;
  buf->st_ctime = buf->st_atime;  
  return 0;
}

int ceph_stat(const char *pathname, struct stat *buf) {
  // minimal stat : only size and times are filled
  std::string path = pathname;
  int slashPos = path.find('/');
  libradosstriper::RadosStriper *striper = getRadosStriper(path.substr(0,slashPos));
  if (0 == striper) {
    errno = EINVAL;
    return -1;
  }
  memset(buf, 0, sizeof(*buf));
  int rc = striper->stat(path.substr(slashPos+1), (uint64_t*)&(buf->st_size), &(buf->st_atime));
  if (rc != 0) {
    errno = -rc;
    return -1;
  }
  buf->st_mtime = buf->st_atime;
  buf->st_ctime = buf->st_atime;  
  return 0;
}

int ceph_fstat64(CephFileRef &fr, struct stat64 *buf) {
  // minimal stat : only size and times are filled
  libradosstriper::RadosStriper *striper = getRadosStriper(fr.pool);
  if (0 == striper) {
    errno = EINVAL;
    return -1;
  }
  memset(buf, 0, sizeof(*buf));
  int rc = striper->stat(fr.name, (uint64_t*)&(buf->st_size), &(buf->st_atime));
  if (rc != 0) {
    errno = -rc;
    return -1;
  }
  buf->st_mtime = buf->st_atime;
  buf->st_ctime = buf->st_atime;  
  return 0;
}

int ceph_stat64(const char *pathname, struct stat64 *buf) {
  // minimal stat : only size and times are filled
  std::string path = pathname;
  int slashPos = path.find('/');
  libradosstriper::RadosStriper *striper = getRadosStriper(path.substr(0,slashPos));
  if (0 == striper) {
    errno = EINVAL;
    return -1;
  }
  memset(buf, 0, sizeof(*buf));
  int rc = striper->stat(path.substr(slashPos+1), (uint64_t*)&(buf->st_size), &(buf->st_atime));
  if (rc != 0) {
    errno = -rc;
    return -1;
  }
  buf->st_mtime = buf->st_atime;
  buf->st_ctime = buf->st_atime;  
  return 0;
}

int ceph_fcntl(CephFileRef &fr, int cmd) {
  // minimal implementation for rfio
  switch (cmd) {
  case F_GETFL:
    return fr.mode;
  default:
    errno = EINVAL;
    return -1;
  }
}

ssize_t ceph_fgetxattr(CephFileRef &fr, const char* name, char* value, size_t size) {
  libradosstriper::RadosStriper *striper = getRadosStriper(fr.pool);
  if (0 == striper) {
    errno = EINVAL;
    return -1;
  }
  ceph::bufferlist bl;
  int rc = striper->getxattr(fr.name, name, bl);
  if (rc) {
    errno = -rc;
    return -1;
  }
  bl.copy(0, size, value);
  return 0;
}

int ceph_fsetxattr(CephFileRef &fr, const char* name, const char* value,
                   size_t size, int flags)  {
  libradosstriper::RadosStriper *striper = getRadosStriper(fr.pool);
  if (0 == striper) {
    errno = EINVAL;
    return -1;
  }
  ceph::bufferlist bl;
  bl.append(value, size);
  int rc = striper->setxattr(fr.name, name, bl);
  if (rc) {
    errno = -rc;
    return -1;
  }
  return 0;
}

int ceph_fremovexattr(CephFileRef &fr, const char* name) {
  libradosstriper::RadosStriper *striper = getRadosStriper(fr.pool);
  if (0 == striper) {
    errno = EINVAL;
    return -1;
  }
  int rc = striper->rmxattr(fr.name, name);
  if (rc) {
    errno = -rc;
    return -1;
  }
  return 0;
}

extern "C" {

  void ceph_posix_set_logfunc(void (*logfunc) (char *, va_list argp)) {
    g_logfunc = logfunc;
  };

  int ceph_posix_open(const char *pathname, int flags, mode_t mode) {
    FileRef fr;
    if (pathname[0] != '/') {
      // only allocate a file descriptor and remember the open parameters
      fr.isCeph = true;
      logwrapper((char*)"ceph_open: fd %d associated to %s\n", g_nextCephFd, pathname);
      ceph_open(fr.cephFile, pathname, flags, mode);
    } else {
      fr.isCeph = false;
      logwrapper((char*)"local_open: fd %d associated to %s\n", g_nextCephFd, pathname);
      fr.fd = open(pathname, flags, mode);
      if (fr.fd < 0) return fr.fd;
    }
    g_fds[g_nextCephFd] = fr;
    g_nextCephFd++;
    return g_nextCephFd-1;
  }

  int ceph_posix_close(int fd) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      int rc;
      if (fr.isCeph) {
        logwrapper((char*)"ceph_close: closed fd %d\n", fd);
        rc = ceph_close(fr.cephFile);
      } else {
        logwrapper((char*)"local_close: closed fd %d\n", fd);
        rc = close(fr.fd);
      }
      g_fds.erase(it);
      return rc;
    } else {
      errno = EBADF;
      return -1;
    }
  }

  off_t ceph_posix_lseek(int fd, off_t offset, int whence) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        logwrapper((char*)"ceph_lseek: for fd %d, offset=%d, whence=%d\n", fd, offset, whence);
        return (off_t)ceph_lseek64(fr.cephFile, offset, whence);
      } else {
        logwrapper((char*)"local_lseek: for fd %d, offset=%d, whence=%d\n", fd, offset, whence);
        return lseek(fr.fd, offset, whence);
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }

  off64_t ceph_posix_lseek64(int fd, off64_t offset, int whence) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        logwrapper((char*)"ceph_lseek64: for fd %d, offset=%d, whence=%d\n", fd, offset, whence);
        return ceph_lseek64(fr.cephFile, offset, whence);
      } else {
        logwrapper((char*)"local_lseek64: for fd %d, offset=%d, whence=%d\n", fd, offset, whence);
        return lseek64(fr.fd, offset, whence);
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }

  ssize_t ceph_posix_write(int fd, const void *buf, size_t count) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        logwrapper((char*)"ceph_write: for fd %d, count=%d\n", fd, count);
        return ceph_write(fr.cephFile, (const char*)buf, count);
      } else {
        logwrapper((char*)"local_write: for fd %d, count=%d\n", fd, count);
        return write(fr.fd, buf, count);
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }

  ssize_t ceph_posix_read(int fd, void *buf, size_t count) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        logwrapper((char*)"ceph_read: for fd %d, count=%d\n", fd, count);
        return ceph_read(fr.cephFile, (char*)buf, count);
      } else {
        logwrapper((char*)"local_read: for fd %d, count=%d\n", fd, count);
        return read(fr.fd, buf, count);
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }

  int ceph_posix_fstat(int fd, struct stat *buf) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        logwrapper((char*)"ceph_stat: fd %d\n", fd);
        return ceph_fstat(fr.cephFile, buf);
      } else {
        logwrapper((char*)"local_stat: fd %d\n", fd);
        return fstat(fr.fd, buf);
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }

  int ceph_posix_stat(const char *pathname, struct stat *buf) {
    if (pathname[0] != '/') {
      logwrapper((char*)"ceph_stat: %s\n", pathname);
      return ceph_stat(pathname, buf);
    } else {
      logwrapper((char*)"local_stat: %s\n", pathname);
      return stat(pathname, buf);
    }
  }

  int ceph_posix_fstat64(int fd, struct stat64 *buf) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        logwrapper((char*)"ceph_stat64: fd %d\n", fd);
        return ceph_fstat64(fr.cephFile, buf);
      } else {
        logwrapper((char*)"local_stat64: fd %d\n", fd);
        return fstat64(fr.fd, buf);
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }

  int ceph_posix_stat64(const char *pathname, struct stat64 *buf) {
    if (pathname[0] != '/') {
      logwrapper((char*)"ceph_stat: %s\n", pathname);
      return ceph_stat64(pathname, buf);
    } else {
      logwrapper((char*)"local_stat: %s\n", pathname);
      return stat64(pathname, buf);
    }
  }

  int ceph_posix_fsync(int fd) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        logwrapper((char*)"ceph_sync: fd %d\n", fd);
        return 0;
      } else {
        logwrapper((char*)"local_sync: fd %d\n", fd);
        return fsync(fr.fd);
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }

  int ceph_posix_fcntl(int fd, int cmd, ... /* arg */ ) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        logwrapper((char*)"ceph_fcntl: fd %d cmd=%d\n", fd, cmd);
        return ceph_fcntl(fr.cephFile, cmd);
      } else {
        logwrapper((char*)"local_fcntl: fd %d cmd=%d\n", fd, cmd);
        va_list arg;
        va_start(arg, cmd);
        int rc = fcntl(fr.fd, cmd, arg);
        va_end(arg);
        return rc;
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }

  ssize_t ceph_posix_fgetxattr(int fd, const char* name, void* value, size_t size) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        logwrapper((char*)"ceph_fgetxattr: fd %d name=%s\n", fd, name);
        return ceph_fgetxattr(fr.cephFile, name, (char*)value, size);
      } else {
        logwrapper((char*)"local_fgetxattr: fd %d name=%s\n", fd, name);
        return fgetxattr(fr.fd, name, value, size);
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }

  int ceph_posix_fsetxattr(int fd, const char* name, const void* value,
                           size_t size, int flags)  {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        logwrapper((char*)"ceph_fsetxattr: fd %d name=%s value=%s\n", fd, name, value);
        return ceph_fsetxattr(fr.cephFile, name, (const char*)value, size, flags);
      } else {
        logwrapper((char*)"local_fsetxattr: fd %d name=%s value=%s\n", fd, name, value);
        return fsetxattr(fr.fd, name, (const char*)value, size, flags);
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }

  int ceph_posix_fremovexattr(int fd, const char* name) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        logwrapper((char*)"ceph_removexattr: fd %d name=%s\n", fd, name);
        return ceph_fremovexattr(fr.cephFile, name);
      } else {
        logwrapper((char*)"local_removexattr: fd %d name=%s\n", fd, name);
        return fremovexattr(fr.fd, name);
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }

} // extern "C"
