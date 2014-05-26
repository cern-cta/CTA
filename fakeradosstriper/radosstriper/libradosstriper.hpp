/******************************************************************************
 * This is a fake implementation of libradosstriper.hpp API allowing to
 * compile CASTOR code without depending on CEPH
 * This bad hack should be dropped. It only exists in order to support
 * compilation on SLC5, while ceph is not compiling on SLC5.
 * Obviously, even if the software will compile with this fake implementation
 * the ceph functionnality won't be present when using this.
 *****************************************************************************/

#ifndef __LIBRADOSSTRIPER_HPP
#define __LIBRADOSSTRIPER_HPP

#include <errno.h>
#include <string>

namespace ceph {

  class bufferlist {

  public:

    void append(const char *data, unsigned len) {};

    void copy(unsigned off, unsigned len, char *dest) const {};    

  };  

};

namespace librados {

  class IoCtx {

  public:

    IoCtx(){}
    
  };

  class Rados {

  public:

    Rados(){};
    
    int init(const char * const id) {
      return -EINVAL;
    }
    
    int conf_read_file(const char * const path) {
      return -EINVAL;
    }

    int conf_parse_env(const char *env) const {
      return -EINVAL;
    }

    int connect() {
      return -EINVAL;
    }

    void shutdown(){}

    int ioctx_create(const char *name, IoCtx &pioctx) {
      return -EINVAL;
    }
  };
};

namespace libradosstriper {

  class RadosStriper {

  public:

    RadosStriper(){};

    static int striper_create(librados::IoCtx& ioctx, RadosStriper *striper) {
      return -EINVAL;
    }

    int getxattr(const std::string& oid, const char *name, ceph::bufferlist& bl) {
      return -EINVAL;
    }

    int setxattr(const std::string& oid, const char *name, ceph::bufferlist& bl) {
      return -EINVAL;
    }

    int rmxattr(const std::string& oid, const char *name) {
      return -EINVAL;
    }

    int write(const std::string& soid, const ceph::bufferlist& bl, size_t len, uint64_t off) {
      return -EINVAL;
    }

    int read(const std::string& soid, ceph::bufferlist* pbl, size_t len, uint64_t off) {
      return -EINVAL;
    }

    int stat(const std::string& soid, uint64_t *psize, time_t *pmtime) {
      return -EINVAL;
    }

    int remove(const std::string& soid) {
      return -EINVAL;
    }

  };
};

#endif
