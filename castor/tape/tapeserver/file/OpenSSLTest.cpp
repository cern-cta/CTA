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

#include <gtest/gtest.h>
#include "castor/server/Threading.hpp"
#include "castor/tape/tapeserver/file/DiskFileImplementations.hpp"
#include "castor/tape/tapeserver/file/OpenSSLLocker.hpp"
#include <openssl/pem.h>

namespace unitTests {
  class OpenSSLThread: public castor::server::Thread {
  public:
    void setKey(EVP_PKEY * key) { m_key = key; }
  private:
    virtual void run() {
       // We need one reference locker per thread for proper cleanup
       std::auto_ptr<castor::tape::diskFile::OpenSSLLockerRef> lockerRef(
        castor::tape::diskFile::OpenSSLLockerRefFactory());
      for (int i=0; i<100; i++) {
        std::string payload = "Some payload... And some more...";
        std::string signature = castor::tape::diskFile::OpenSSLSigner::sign(
          payload, m_key);
      }
    }
    EVP_PKEY * m_key;
  };
  
  TEST(castor_OpenSSL, multiThreading) {
    // Install the SSL locker
    std::auto_ptr<castor::tape::diskFile::OpenSSLLockerRef> lockerRef(
        castor::tape::diskFile::OpenSSLLockerRefFactory());
    // A random key from a bare "openssl genrsa".
    std::string somePrivateKey = 
        "-----BEGIN RSA PRIVATE KEY-----\n"
        "MIICXQIBAAKBgQDMGFBHGra0lHWyXA9oiwaMSPqKjv7tNuxAL3oUe+SbXBUJX6Nh\n"
        "oMh/uo71jSQpyEozxkTsHkwNCMAPq+fBlsjGFHoNkiAjH68zwzCILM3XDQx5+ztH\n"
        "e1att+niTVzLgrBy8R729Vgyvv/ToghLdwrJ+witd1YNPHHoZH5amLsHLwIDAQAB\n"
        "AoGBAJ7y0JKP23sHpCIkUFu66n6W14jRlPhprdTPJOSPGJtmO3vxX+zIq13OjUfv\n"
        "hBqGQkPQRh0d+1yrU+jgmL3MEM/OYJRm0iKAom3x28a9Rn2c+6tr7synBJMT3b8t\n"
        "c5ShEGU2diNR3VrjyRDLQplaWY+1txLp+5jZ86C10M5y22QBAkEA9hYkGHJ0q9B6\n"
        "16bHJvEUE8VbdFdZBPCE5HmJBoambFBeQgseuxi9fY3byrVvrRKgn8t7tocz0Ans\n"
        "8ND+QTWAUQJBANRRHpBv13JmmBN0SK57rw3bDz27CnDbCMN2/omC1Ykb1g3L+JvU\n"
        "VupF4zHo54VXIMXwRVQ8dSmTLxYEVMepr38CQC3Y/iyX1mjUVK6s4dm9fJIaaOmK\n"
        "BInJDdlLU14l5Ae2CXmgfL864sLrlRF1MDM8jzR2QrxFAEA4OS68oUIg56ECQQCR\n"
        "W0gVkrxpshuDliT8b+kVD1iL5rXrNcn2KE1zT4Np7wjJQU/fP6yRj29QCCgZfeEO\n"
        "IsUUOp/r6rxd0nFIkL95AkA3oztzUVis7R4g5gO9UXCJzhlIY7y67coAbBHrqiHa\n"
        "QeMkZnkN4sib4C4U2GanmAI05U05AIjDgp9lx3EYSulY\n"
        "-----END RSA PRIVATE KEY-----\n";
    // Import the key
    BIO * BIObuf = BIO_new(BIO_s_mem());
    BIO_write(BIObuf, somePrivateKey.c_str(), somePrivateKey.size());
    EVP_PKEY * OSSLKey;
    OSSLKey = PEM_read_bio_PrivateKey(BIObuf, NULL, NULL, NULL);
    BIO_free(BIObuf);
    std::vector<OpenSSLThread> m_threads;
    m_threads.resize(10);
    for (std::vector<OpenSSLThread>::iterator i=m_threads.begin(); 
        i!=m_threads.end(); i++) {
      i->setKey(OSSLKey);
      i->start();
    }
    for (std::vector<OpenSSLThread>::iterator i=m_threads.begin(); 
        i!=m_threads.end(); i++)
      i->wait();
    EVP_PKEY_free(OSSLKey);
  }
}
