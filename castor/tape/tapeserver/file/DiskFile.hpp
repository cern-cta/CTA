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

#include "castor/tape/tapeserver/file/Structures.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/tape/tapegateway/FileToRecallStruct.hpp"
#include "castor/tape/tapegateway/FileToMigrateStruct.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"

namespace castor {
  namespace tape {
    /**
     * Namespace managing the reading and writing of files to and from disk.
     */
    namespace diskFile {
      
      class ReadFile {
      public:
        
        /**
         * Constructor of the ReadFile class. It opens the file for reading with the O_RDONLY flag.
         * @param url: Uniform Resource Locator of the file we want to read
         */
        ReadFile(const std::string &url) ;
        /**
         * Return the size of the file in byte. Can throw
         * @return 
         */
        size_t size() const;
        
        /**
         * Reads data from the file.
         * @param data: pointer to the data buffer
         * @param size: size of the buffer
         * @return The amount of data actually copied. Zero at end of file.
         */
        size_t read(void *data, const size_t size) ;
        
        /**
         * Destructor of the ReadFile class. It closes the corresponding file descriptor.
         */
        ~ReadFile() throw();
        
      private:
        int m_fd;
      };
      
      class WriteFile {
      public:
        
        /**
         * Constructor of the WriteFile class. It opens the file for writing with the O_WRONLY, O_CREAT and O_EXCL flags.
         * @param url: Uniform Resource Locator of the file we want to write
         */
        WriteFile(const std::string &url) ;
        
        /**
         * Writes a block of data on disk
         * @param data: buffer to copy the data from
         * @param size: size of the buffer
         */
        void write(const void *data, const size_t size) ;
        
        /**
         * Closes the corresponding file descriptor, which may throw an exception.
         */
        void close() ;
        
        /**
         * Destructor of the WriteFile class.
         */
        ~WriteFile() throw();
        
      private:
        int m_fd;
        bool closeTried;
      };
    } //end of namespace diskFile
    
 }}