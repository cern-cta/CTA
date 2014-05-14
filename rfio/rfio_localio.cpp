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

static libradosstriper::RadosStriper* getRadosStriper(std::string pool) {
  std::map<std::string, libradosstriper::RadosStriper*>::iterator it =
    g_radosStripers.find(pool);
  if (it == g_radosStripers.end()) {
    // we need to create a new radosStriper
    librados::Rados cluster;
    librados::IoCtx ioctx;
    int rc = cluster.ioctx_create(pool.c_str(), ioctx);
    if (rc != 0) return 0;
    libradosstriper::RadosStriper *newStriper;
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
  fr.name = path.substr(slashPos);
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
  return striper->write(fr.name, bl, count, fr.offset);
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
  if (0 != rc) return rc;
  bl.copy(0, count, buf);
  return 0;
}

int ceph_fstat(CephFileRef &fr, struct stat *buf) {
  // minimal stat for rfio usage : return only size
  libradosstriper::RadosStriper *striper = getRadosStriper(fr.pool);
  if (0 == striper) {
    errno = EINVAL;
    return -1;
  }
  time_t pmtime;
  int rc = striper->stat(fr.name, (uint64_t*)&(buf->st_size), &pmtime);
  if (rc != 0) {
    errno = -rc;
    return -1;
  }
  return 0;
}

int ceph_fstat64(CephFileRef &fr, struct stat64 *buf) {
  // minimal stat for rfio usage : return only size
  libradosstriper::RadosStriper *striper = getRadosStriper(fr.pool);
  if (0 == striper) {
    errno = EINVAL;
    return -1;
  }
  time_t pmtime;
  int rc = striper->stat(fr.name, (uint64_t*)&(buf->st_size), &pmtime);
  if (rc != 0) {
    errno = -rc;
    return -1;
  }
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
  
int ceph_removexattr(CephFileRef &fr, const char* name) {
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

  int generic_open(const char *pathname, int flags, mode_t mode) {
    FileRef fr;
    if (pathname[0] != '/') {
      // only allocate a file descriptor and remember the open parameters
      fr.isCeph = true;
      ceph_open(fr.cephFile, pathname, flags, mode);
    } else {
      fr.isCeph = false;
      fr.fd = open(pathname, flags, mode);
      if (fr.fd < 0) return fr.fd;
    }
    g_fds[g_nextCephFd] = fr;
    g_nextCephFd++;
    return g_nextCephFd-1;
  }

  int generic_close(int fd) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      int rc;
      if (fr.isCeph) {
        rc = ceph_close(fr.cephFile);
      } else {
        rc = close(fr.fd);
      }
      g_fds.erase(it);
      return rc;
    } else {
      errno = EBADF;
      return -1;
    }
  }

  off64_t generic_lseek64(int fd, off64_t offset, int whence) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        return ceph_lseek64(fr.cephFile, offset, whence);
      } else {
        return lseek64(fr.fd, offset, whence);
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }

  ssize_t generic_write(int fd, const void *buf, size_t count) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        return ceph_write(fr.cephFile, (const char*)buf, count);
      } else {
        return write(fr.fd, buf, count);
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }

  ssize_t generic_read(int fd, void *buf, size_t count) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        return ceph_read(fr.cephFile, (char*)buf, count);
      } else {
        return write(fr.fd, buf, count);
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }
  
  int generic_fstat(int fd, struct stat *buf) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        return ceph_fstat(fr.cephFile, buf);
      } else {
        return fstat(fr.fd, buf);
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }

  int generic_fstat64(int fd, struct stat64 *buf) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        return ceph_fstat64(fr.cephFile, buf);
      } else {
        return fstat64(fr.fd, buf);
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }

  int generic_fsync(int fd) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        return 0;
      } else {
        return fsync(fr.fd);
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }
  
  int generic_fcntl(int fd, int cmd, ... /* arg */ ) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        return ceph_fcntl(fr.cephFile, cmd);
      } else {
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

  ssize_t fgetxattr(int fd, const char* name, void* value, size_t size) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        return ceph_fgetxattr(fr.cephFile, name, (char*)value, size);
      } else {
        return fgetxattr(fr.fd, name, value, size);
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }

  int fsetxattr(int fd, const char* name, const void* value,
                size_t size, int flags)  {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        return ceph_fsetxattr(fr.cephFile, name, (const char*)value, size, flags);
      } else {
        return fsetxattr(fr.fd, name, (const char*)value, size, flags);
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }
  
  int removexattr(int fd, const char* name) {
    std::map<unsigned int, FileRef>::iterator it = g_fds.find(fd);
    if (it != g_fds.end()) {
      FileRef &fr = it->second;
      if (fr.isCeph) {
        return ceph_removexattr(fr.cephFile, name);
      } else {
        return removexattr(fr.fd, name);
      }
    } else {
      errno = EBADF;
      return -1;
    }
  }

} // extern "C"
