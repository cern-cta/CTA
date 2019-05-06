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
#include <set>
#include <future>
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
      class DiskFileRemover;
      class Directory;
      
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
      
      /**
       * This class is the base class to asynchronously delete
       * Disk Files
       */
      class AsyncDiskFileRemover{
      protected:
	std::future<void> m_futureDeletion;
      public:
	virtual void asyncDelete() = 0;
	virtual void wait() = 0;
	virtual ~AsyncDiskFileRemover(){}
      };
      
      /**
       * Factory class deciding which async disk file remover
       * to instanciate regarding the format of the path of the disk file
       */
      class AsyncDiskFileRemoverFactory {
	typedef cta::utils::Regex Regex;
      public:
	AsyncDiskFileRemoverFactory();
	AsyncDiskFileRemover * createAsyncDiskFileRemover(const std::string &path);
      private:
	Regex m_URLLocalFile;
        Regex m_URLXrootdFile;
      };
      
      class DiskFileRemover{
      public:
	virtual void remove() = 0;
	virtual ~DiskFileRemover() throw() {}
      protected:
	std::string m_URL;
      };
      
      /**
       * Factory class deciding what type of Directory subclass
       * to instanciate based on the URL passed
       */
      class DirectoryFactory{
	typedef cta::utils::Regex Regex;
      public:
	DirectoryFactory();
	
	/**
	 * Returns the correct directory subclass regarding the path passed in parameter
	 * @param path the path of the directory to manage
	 * @return the Directory subclass instance regarding the path passed in parameter
	 * @throws cta::exception if the path provided does not allow to determine which instance of
	 * Directory will be instanciated.
	 */
	Directory * createDirectory(const std::string &path);
	
      private:
	Regex m_URLLocalDirectory;
        Regex m_URLXrootDirectory;
      };
      
      
      class Directory {
      public:
	/**
	 * Creates a directory
	 * @throws an exception if the directory could not have been created
	 */
	virtual void mkdir() = 0;
	/**
	 * Check if the directory exist
	 * @return true if the directory exists, false otherwise
	 */
	virtual bool exist() = 0;
	/**
	 * Return all the names of the files present in the directory
	 * @return 
	 */
	virtual std::set<std::string> getFilesName() = 0;
	
	/**
	 * Remove the directory located at this->m_URL 
	 */
	virtual void rmdir() = 0;
	
	virtual ~Directory() throw() {}
      protected:
	/**
         * Storage for the URL
         */
	std::string m_URL;
      };
      
    } //end of namespace diskFile
    
 }}

