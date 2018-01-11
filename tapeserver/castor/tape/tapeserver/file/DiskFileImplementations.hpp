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

#pragma once

#include "castor/tape/tapeserver/file/DiskFile.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"
#include "castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "common/exception/XrootCl.hpp"
#include "common/exception/Exception.hpp"
#include <xrootd/XrdCl/XrdClFile.hh>
#include <cryptopp/rsa.h>
#include <radosstriper/libradosstriper.hpp>

namespace castor {
  namespace tape {
    /**
     * Namespace managing the reading and writing of files to and from disk.
     */
    namespace diskFile {
      //==============================================================================
      // LOCAL FILES
      //==============================================================================
      class LocalReadFile: public ReadFile {
      public:
        LocalReadFile(const std::string &path);
        virtual size_t size() const;
        virtual size_t read(void *data, const size_t size) const;
        virtual ~LocalReadFile() throw();
      private:
        int m_fd;
      };
     
      class LocalWriteFile: public WriteFile {
      public:
        LocalWriteFile(const std::string &path);
        virtual void write(const void *data, const size_t size);
        virtual void setChecksum(uint32_t checksum);
        virtual void close();
        virtual ~LocalWriteFile() throw();
      private:
        int m_fd;
        bool m_closeTried;
      };
      
      //==============================================================================
      // CRYPTOPP SIGNER
      //==============================================================================
      struct CryptoPPSigner {
        static std::string sign(const std::string msg, 
          const CryptoPP::RSA::PrivateKey  & privateKey);
        static cta::threading::Mutex s_mutex;
      };
      
      //==============================================================================
      // XROOT FILES
      //==============================================================================  
      class XrootBaseReadFile: public ReadFile {
      public:
        XrootBaseReadFile(uint16_t timeout): m_timeout(timeout) {}
        virtual size_t size() const;
        virtual size_t read(void *data, const size_t size) const;
        virtual ~XrootBaseReadFile() throw();
      protected:
        // Access to parent's protected member...
        void setURL(const std::string & v) { m_URL = v; }
        // There is no const-correctness with XrdCl...
        mutable XrdCl::File m_xrootFile;
        mutable uint64_t m_readPosition;
        const uint16_t m_timeout;
        typedef cta::exception::XrootCl XrootClEx;
      };
      
      class XrootReadFile: public XrootBaseReadFile {
      public:
        XrootReadFile(const std::string &xrootUrl, uint16_t timeout = 0);
      };
      
      class XrootC2FSReadFile: public XrootBaseReadFile {
      public:
        XrootC2FSReadFile(const std::string &xrootUrl,
          const CryptoPP::RSA::PrivateKey & privateKey,
          uint16_t timeout = 0,
          const std::string & cephPool = "");
        virtual ~XrootC2FSReadFile() throw () {}
      private:
        std::string m_signedURL;
      };
      
      class XrootBaseWriteFile: public WriteFile {
      public:
        XrootBaseWriteFile(uint16_t timeout): m_writePosition(0), m_timeout(timeout), m_closeTried(false) {}
        virtual void write(const void *data, const size_t size);
        virtual void setChecksum(uint32_t checksum);
        virtual void close();
        virtual ~XrootBaseWriteFile() throw();        
      protected:
        // Access to parent's protected member...
        void setURL(const std::string & v) { m_URL = v; }
        XrdCl::File m_xrootFile;
        uint64_t m_writePosition;
        const uint16_t m_timeout;
        typedef cta::exception::XrootCl XrootClEx;
        bool m_closeTried;      
      };
      
      class XrootWriteFile: public XrootBaseWriteFile {
      public:
        XrootWriteFile(const std::string &xrootUrl, uint16_t timeout = 0);
      };
      
      class XrootC2FSWriteFile: public XrootBaseWriteFile {
      public:
        XrootC2FSWriteFile(const std::string &xrootUrl,
          const CryptoPP::RSA::PrivateKey & privateKey,
          uint16_t timeout = 0,
          const std::string & cephPool = "");
        virtual ~XrootC2FSWriteFile() throw () {}
      private:
        std::string m_signedURL;
      };     
      
      //==============================================================================
      // RADOS STRIPER FILES
      //==============================================================================
      // The Rados striper URLs in CASTOR are in the form:
      // radosstriper:///user@pool:filePath
      // We will not expect the
      class RadosStriperReadFile: public ReadFile {
      public:
        RadosStriperReadFile(const std::string &fullURL,
          libradosstriper::RadosStriper * striper,
          const std::string &osd);
        virtual size_t size() const;
        virtual size_t read(void *data, const size_t size) const;
        virtual ~RadosStriperReadFile() throw();
      private:
        libradosstriper::RadosStriper * m_striper;
        std::string m_osd;
        mutable size_t m_readPosition;
      };
      
      class RadosStriperWriteFile: public WriteFile {
      public:
        RadosStriperWriteFile(const std::string &fullURL,
          libradosstriper::RadosStriper * striper,
          const std::string &osd);
        virtual void write(const void *data, const size_t size);
        virtual void setChecksum(uint32_t checksum);
        virtual void close();
        virtual ~RadosStriperWriteFile() throw();
      private:
        libradosstriper::RadosStriper * m_striper;
        std::string m_osd;
        size_t m_writePosition;
      };
    } //end of namespace diskFile
    
 }}
