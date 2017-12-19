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

#include "common/utils/Regex.hpp"
#include "common/threading/Mutex.hpp"
#include <cryptopp/rsa.h>
#include <memory>
#include <stdint.h>
/*
 * This file only contains the interface declaration of the base classes
 * the real implementation, which depends on many includes is hidden in
 * DiskFileImplementations.hpp
 * There is still a single .cpp file
 */

namespace castor {
  namespace tape {
  // Forward declaration of RadosStriperPool
  namespace file { class RadosStriperPool; }

    /**
     * Namespace managing the reading and writing of files to and from disk.
     */
    namespace diskFile {
      
      class ReadFile;
      class WriteFile;
      
      /**
       * Factory class deciding on the type of read/write file type
       * based on the url passed
       */
      class DiskFileFactory {
        typedef cta::utils::Regex Regex;
      public:
        DiskFileFactory(const std::string & xrootPrivateKey, uint16_t xrootTimeout, 
          castor::tape::file::RadosStriperPool & striperPool);
        ReadFile * createReadFile(const std::string & path);
        WriteFile * createWriteFile(const std::string & path);
      private:
        Regex m_NoURLLocalFile;
        Regex m_NoURLRemoteFile;
        Regex m_NoURLRadosStriperFile;
        Regex m_URLLocalFile;
        Regex m_URLXrootFile;
        Regex m_URLCephFile;
        std::string m_xrootPrivateKeyFile;
        CryptoPP::RSA::PrivateKey m_xrootPrivateKey;
        bool m_xrootPrivateKeyLoaded;
        const uint16_t m_xrootTimeout;
        castor::tape::file::RadosStriperPool & m_striperPool;
        
      public:
        /** Return the private key. Read it from the file if necessary. */ 
        const CryptoPP::RSA::PrivateKey & xrootPrivateKey();
      };
      
      class ReadFile {
      public:
        /**
         * Return the size of the file in byte. Can throw
         * @return 
         */
        virtual size_t size() const = 0;
        
        /**
         * Reads data from the file.
         * @param data: pointer to the data buffer
         * @param size: size of the buffer
         * @return The amount of data actually copied. Zero at end of file.
         */
        virtual size_t read(void *data, const size_t size) const = 0;
        
        /**
         * Destructor of the ReadFile class. It closes the corresponding file descriptor.
         */
        virtual ~ReadFile() throw() {}
        
        /**
         * File protocol and path for logging
         */
        virtual std::string URL() const { return m_URL; }
        
      protected:
        /**
         * Storage for the URL
         */
        std::string m_URL;
      };
      
      class WriteFile {
      public:
        /**
         * Writes a block of data on disk
         * @param data: buffer to copy the data from
         * @param size: size of the buffer
         */
        virtual void write(const void *data, const size_t size) = 0;
        
        /**
         * Set the checksum as an extended attribute (only needed for Ceph storage).
         */
        virtual void setChecksum(uint32_t checksum) = 0;
        
        /**
         * Closes the corresponding file descriptor, which may throw an exception.
         */
        virtual void close() = 0;
        
        /**
         * Destructor of the WriteFile class.
         */
        virtual ~WriteFile() throw() {}
        
        /**
         * File protocol and path for logging
         */
        virtual std::string URL() const { return m_URL; }
        
      protected:
        /**
         * Storage for the URL
         */
        std::string m_URL;
      };

    } //end of namespace diskFile
    
 }}

