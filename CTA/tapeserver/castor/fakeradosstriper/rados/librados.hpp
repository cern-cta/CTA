/******************************************************************************
 * This is a fake implementation of librados.hpp API allowing to
 * compile CASTOR code without depending on CEPH
 * This bad hack should be dropped. It only exists in order to support
 * compilation on SLC5, while ceph is not compiling on SLC5.
 * Obviously, even if the software will compile with this fake implementation
 * the ceph functionnality won't be present when using this.
 *****************************************************************************/

#ifndef __LIBRADOS_HPP
#define __LIBRADOS_HPP

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

  class ObjectIterator {
  public:
    ObjectIterator() {}

    bool operator!=(const ObjectIterator& rhs) const {
      return false;
    }

    const std::pair<std::string, std::string>* operator->() const {
      return &m_null;
    };

    ObjectIterator operator++(int) {
      return *this;
    };
    
    std::pair<std::string, std::string> m_null;
  };
  
  class IoCtx {

  public:

    IoCtx(){}    

    ObjectIterator objects_begin() {
      return ObjectIterator();
    }

    ObjectIterator objects_end() {
      return ObjectIterator();
    }

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

#endif
