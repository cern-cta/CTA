/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include <gtest/gtest.h>
#include "common/threading/Thread.hpp"
#include "disk/DiskFileImplementations.hpp"
#include "disk/DiskFile.hpp"
#include "disk/RadosStriperPool.hpp"
#include <cryptopp/base64.h>
#include <cryptopp/osrng.h>


namespace unitTests {
  class CryptoPPThread: public cta::threading::Thread {
  public:
    void setKey(const CryptoPP::RSA::PrivateKey & key) { m_key = key; }
  private:
    virtual void run() {
      for (int i=0; i<100; i++) {
        std::string payload = "Some payload... And some more...";
        std::string signature = cta::disk::CryptoPPSigner::sign(
          payload, m_key);
      }
    }
    CryptoPP::RSA::PrivateKey m_key;
  };
  
  class PEMKeyString {
  public:
    PEMKeyString(const std::string & keyString) {
      // Import the key
      const std::string HEADER = "-----BEGIN RSA PRIVATE KEY-----";
      const std::string FOOTER = "-----END RSA PRIVATE KEY-----";
          
      size_t pos1, pos2;
      pos1 = keyString.find(HEADER);
      if(pos1 == std::string::npos)
          throw cta::exception::Exception(
            "In DiskFileFactory::xrootCryptoPPPrivateKey, PEM header not found");
          
      pos2 = keyString.find(FOOTER, pos1+1);
      if(pos2 == std::string::npos)
          throw cta::exception::Exception(
            "In DiskFileFactory::xrootCryptoPPPrivateKey, PEM footer not found");
          
      // Start position and length
      pos1 = pos1 + HEADER.length();
      pos2 = pos2 - pos1;
      std::string keystr = keyString.substr(pos1, pos2);
      
      // Base64 decode, place in a ByteQueue    
      CryptoPP::ByteQueue queue;
      CryptoPP::Base64Decoder decoder;
      
      decoder.Attach(new CryptoPP::Redirector(queue));
      decoder.Put((const byte*)keystr.data(), keystr.length());
      decoder.MessageEnd();
      
      // Get the key
      m_key.BERDecodePrivateKey(queue, false /*paramsPresent*/, queue.MaxRetrievable());
      
      // BERDecodePrivateKey is a void function. Here's the only check
      // we have regarding the DER bytes consumed.
      if(!queue.IsEmpty())
        throw cta::exception::Exception(
          "In DiskFileFactory::xrootCryptoPPPrivateKey, garbage at end of key");
      
      CryptoPP::AutoSeededRandomPool prng;
      bool valid = m_key.Validate(prng, 3);
      if(!valid)
        throw cta::exception::Exception(
          "In DiskFileFactory::xrootCryptoPPPrivateKey, RSA private key is not valid");
    }
    operator CryptoPP::RSA::PrivateKey() {
      return m_key;
    }
  private:
    CryptoPP::RSA::PrivateKey m_key;
  };
  
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
  
  TEST(castor_CryptoPP, multiThreading) {
    // Read the key
    PEMKeyString privateKey(somePrivateKey);
    
    // Run the threads
    std::vector<CryptoPPThread> m_threads;
    m_threads.resize(3);
    for (std::vector<CryptoPPThread>::iterator i=m_threads.begin(); 
        i!=m_threads.end(); i++) {
      i->setKey(privateKey);
      i->start();
    }
    for (std::vector<CryptoPPThread>::iterator i=m_threads.begin(); 
        i!=m_threads.end(); i++)
      i->wait();
  }
  
  class CryptoPPKeyThread: public cta::threading::Thread {
  private:
    virtual void run() {
      for (int i=0; i<5; i++) {
        // Read keys in parallel and in a loop to test MT protection of the
        // key reading, not protected here.
        PEMKeyString CryptoPPKey(somePrivateKey);
      }
    }
  };
  
  TEST(castor_CryptoPP, multiThreadingKeyRead) {
    // Run the threads
    std::vector<CryptoPPKeyThread> m_threads;
    m_threads.resize(3);
    for (std::vector<CryptoPPKeyThread>::iterator i=m_threads.begin(); 
        i!=m_threads.end(); i++) {
      i->start();
    }
    for (std::vector<CryptoPPKeyThread>::iterator i=m_threads.begin(); 
        i!=m_threads.end(); i++)
      i->wait();
  }
  
  class castor_CryptoPPDiskFileFactory: public cta::threading::Thread {
  public:
    void setPath(const std::string & path) {
      m_keyPath = path;
    }
  private:
    virtual void run() {
      cta::disk::RadosStriperPool striperPool;
      cta::disk::DiskFileFactory dff(m_keyPath, 0, 
          striperPool);
      for (int i=0; i<5; i++) {
        // Read keys in parallel and in a loop to test MT protection of the
        // key reading, not protected here.
        dff.xrootPrivateKey();
      }
    }
    std::string m_keyPath;
  };
  
  class TempFileForXrootKey {
  public:
    TempFileForXrootKey(const std::string & content) {
      char path[100];
      strncpy(path, "/tmp/castorUnitTestPrivateKeyXXXXXX", 100);
      int tmpFileFd = mkstemp(path);
      cta::exception::Errnum::throwOnMinusOne(tmpFileFd, "Error creating a temporary file");
      m_path = path;
      try {
        cta::exception::Errnum::throwOnMinusOne(write(tmpFileFd, content.c_str(), content.size()));
      } catch (...) {
        close (tmpFileFd);
        ::unlink(m_path.c_str());
        throw;
      }
      close (tmpFileFd);
    }
    ~TempFileForXrootKey() {
      ::unlink(m_path.c_str());
    }
    const std::string & path() {
      return m_path;
    }
  private:
    std::string m_path;
  };
  
  TEST(castor_CryptoPP, multiThreadingFileFactoryKeyRead) {
    // Create the key file
    TempFileForXrootKey keyFile(somePrivateKey);
    // Run the threads
    std::vector<castor_CryptoPPDiskFileFactory> m_threads;
    m_threads.resize(3);
    for (std::vector<castor_CryptoPPDiskFileFactory>::iterator i=m_threads.begin(); 
        i!=m_threads.end(); i++) {
      i->setPath(keyFile.path());
    }
    for (std::vector<castor_CryptoPPDiskFileFactory>::iterator i=m_threads.begin(); 
        i!=m_threads.end(); i++) {
      i->start();
    }
    for (std::vector<castor_CryptoPPDiskFileFactory>::iterator i=m_threads.begin(); 
        i!=m_threads.end(); i++) {
      i->wait();
    }
  }
  
  TEST(castor_CryptoPP, agreesWithOpenSSL) {
    // Import the key for CryptoPP
    PEMKeyString CryptoPPKey(somePrivateKey);
    std::string msg("Any random message will do!");
    // This is the output of:
    // echo -n 'Any random message will do!' | openssl dgst -sha1 -sign  ~/testRSAPrivate.pem | openssl enc -base64 | tr -d '\n' ; echo
    std::string osslSign("bfqLxACTFS7fMKH5ewNUOaglRlIGCEPWGhx4fRPErFGHtuCi2yWlYFsXIfjBxOT+yCyKRpTnZWGJTbcP72eT7os2qCqIOejAM3nTcsChHN5f3UyADvsi1f7C3DqhYVKVFQPaBdb3zm8IBHsFjmu2EzVE5juc1C9L+ztVmoABptw=");
    std::string CryptoPPSign(cta::disk::CryptoPPSigner::sign(msg,CryptoPPKey));
    // std::cout << CryptoPPSign << std::endl;
    ASSERT_EQ(osslSign,CryptoPPSign);
    // A few examples generated with openssl's command line:
    // string=`dd if=/dev/urandom bs=40 count=1 2>/dev/null | openssl enc -base64 | tr -d 'n\'`
    // echo $string
    // openssl dgst -sha1 -sign ~/testRSAPrivate.pem | openssl enc -base64 | tr -d '\n' ; echo
    //
    // ~/testRSAPrivate.pem contains the same content as the variable somePrivateKey.
    
    ASSERT_EQ("O8nSHzVPXyNRGRu8vaQ+CrqJjlv28qdsiFCTjlmeMFgc/aEnlJq+2b2q5al7BiHmPAFOd6fUkvn8xlFBm9IUlPFENhPLuMKJGqRSBndE7At0t+/vbS9UVnKiuOjrFepXo8JOvbt7lpqfp3jBwrQE5OZpOT92Nh8GXlpiCksvoxI=",
      cta::disk::CryptoPPSigner::sign("rsoCb+KBj9j9Xk6wr5Cgh+TVuI3eDZHTzD9z8pTFjBPEjdyfiTFIeQ==",CryptoPPKey));
    ASSERT_EQ("uf8Cgkwtmh9K1VFLfeIfkZFQMd/pfSGCHNYPByH0nm0COPHNQAkYEI38ez9DS43fsIBVU9Gdrs0x50dVIntzawgzDrjp8YJIeARJF73he8M+6/FUgWJumNMoDE8fdvgiBaCFTv4+di5vtSb/abKVfqY9IbUPSDByxYjDKKI0OF0=",
      cta::disk::CryptoPPSigner::sign("MtvFsd09F8UQNpwsULF6eMyVkRDIU+uAvBXyJs/LoNM5HrjoJgZrig==",CryptoPPKey));
    ASSERT_EQ("EzSR5Fd1kfmdrVhCiYgoWQ7E1MSdv8OYng3L7LepCfS9OStlEFTkJcMezt4VRqUZnarlcIZ0yPAvrmOUscjrAOAbqA0rMYKsvHnAwd19RaH54QZhtRCDwMloxpuLmUC1cmyJ/PAdRoMYCoHiMVr7yQw0CnVJ5168MUe5o0v3swY=",
      cta::disk::CryptoPPSigner::sign("v3lPb49U+Zz+DNdzoTf2R8AU+AFP+/9/7nLlJV1+HNf3Z+Nzl/HuiQ==",CryptoPPKey));
  }
}
