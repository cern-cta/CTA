/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <openssl/crypto.h>
#include "castor/tape/tapeserver/file/OpenSSLLocker.hpp"
#include "castor/server/Mutex.hpp"
#include "castor/server/MutexLocker.hpp"
#include <sys/types.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <vector>
#include "castor/exception/Exception.hpp"
#include "castor/log/SyslogLogger.hpp"
#include <openssl/err.h>
#include <openssl/evp.h>

extern "C" { 
  void castorTapeOpenSSLLockingCallback(int mode, int n, const char * file, int line);
  void castorTapeOpenSSLThreadIdCallback(CRYPTO_THREADID * threadId);
}

namespace castor {
namespace tape {
namespace diskFile {
  
  /**
   * A singleton implementing and registering the minimal callbacks for thread
   * safe operations of OpenSSL. It is designed to be instantiated late in
   * the tape session. The file factory will do it. The class will then 
   * "register" itself in a static pointer, which is needed as the OpenSSL callbacks
   * do not provide a  void* context pointer.
   * 
   * We do this so that the main process for not have statically allocated mutexes,
   * which could give side effects to initial forking.
   */
  class OpenSSLLocker {
  public:
    OpenSSLLocker () {
      if (gOpenSSLLocker) {
        throw castor::exception::Exception("Attempt to instantiate several OpenSSLLockers");
      } else {
        gOpenSSLLocker = this;
      }
      // In the current implementation of OpenSSL, CRYPTO_num_locks returns
      // CRYPTO_NUM_LOCKS, which is 41. a pretty reasonable number.
      // Just resize the array
      m_mutexes.resize(::CRYPTO_num_locks());
      ::CRYPTO_set_locking_callback(castorTapeOpenSSLLockingCallback);
      ::CRYPTO_THREADID_set_callback(castorTapeOpenSSLThreadIdCallback);
      // Also load the crypto error messages
      ::ERR_load_crypto_strings();
    }
    ~OpenSSLLocker() {
      // According to Network Security with OpenSSL, Chapter 4, we can unset the
      // callbacks
      ::CRYPTO_THREADID_set_callback(NULL);
      ::CRYPTO_set_locking_callback(NULL);
      // Cleanup OpenSSL's structures
      //::CONF_modules_free();
      //::ENGINE_cleanup();
      //::CONF_modules_unload(1);
      ::ERR_free_strings();
      ::EVP_cleanup();
      ::CRYPTO_cleanup_all_ex_data();
      gOpenSSLLocker = NULL;
    }
    void lockingCallback(int mode, int n, const char * file, int line) throw () {
      try {
        if (n < 0 || (size_t)n >= m_mutexes.size())
          throw castor::exception::Exception("In castor::tape::diskFile::OpenSSLLocker: index overflow");
        if (mode & CRYPTO_LOCK) {
          m_mutexes[n].lock();
        } else {
          m_mutexes[n].unlock();
        }
      } catch (castor::exception::Exception & e) {
        // Simple handling for the moment: log and die. IF not sufficient we will
        // have to use SSL's error system to push errors to the caller.
        // See https://www.openssl.org/docs/crypto/err.html
        castor::log::SyslogLogger sl("tapeserverd");
        sl(LOG_EMERG, e.what());
        abort();
      } catch (...) {
        castor::log::SyslogLogger sl("tapeserverd");
        castor::exception::Exception e("Unknown exception in castor::tape::diskFile::OpenSSLLocker");
        sl(LOG_EMERG, e.what());
        abort();
      }
    }
    static OpenSSLLocker * gOpenSSLLocker;
    static castor::server::Mutex gCreationMutex;
    static int gRefCount;
  private:
    std::vector<castor::server::Mutex> m_mutexes;
  };
  
  struct OpenSSLLockerRef {
    OpenSSLLockerRef() {
      castor::server::MutexLocker ml(&OpenSSLLocker::gCreationMutex);
      if (!OpenSSLLocker::gRefCount++) {
        OpenSSLLocker::gOpenSSLLocker = new OpenSSLLocker;
      }
    }
    ~OpenSSLLockerRef() {
      castor::server::MutexLocker ml(&OpenSSLLocker::gCreationMutex);
      if(!--OpenSSLLocker::gRefCount) {
        delete OpenSSLLocker::gOpenSSLLocker;
        OpenSSLLocker::gOpenSSLLocker = NULL;
      }
      // Remove this thread's error stack
      // This implies that OpenSSLLockerRef is instantiated once per thread,
      // which is the case in our application.
      ::ERR_remove_state(0);
    }
  };
  
  OpenSSLLockerRef * OpenSSLLockerRefFactory() {
    return new OpenSSLLockerRef();
  }
  
  OpenSSLLocker * OpenSSLLocker::gOpenSSLLocker;
  castor::server::Mutex OpenSSLLocker::gCreationMutex;
  int OpenSSLLocker::gRefCount;
}}}

void castorTapeOpenSSLLockingCallback(int mode, int n, const char * file, int line) {
  castor::tape::diskFile::OpenSSLLocker::
    gOpenSSLLocker->lockingCallback(mode,n,file,line);
}

void castorTapeOpenSSLThreadIdCallback(CRYPTO_THREADID * threadId) {
  CRYPTO_THREADID_set_numeric(threadId, syscall(SYS_gettid));
}

