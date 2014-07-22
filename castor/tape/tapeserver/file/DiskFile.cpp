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

#include "castor/tape/tapeserver/file/DiskFile.hpp"
#include "castor/exception/Errnum.hpp"
#include "castor/exception/SErrnum.hpp"
#include "castor/exception/Mismatch.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/tape/tapegateway/FilesToMigrateList.hpp"
#include <rfio_api.h>

namespace castor {
  namespace tape {
    namespace diskFile { //toy example using rfio, will not use rfio in production probably xroot or ceph protocols
      
      ReadFile::ReadFile(const std::string &url)  {
        m_fd = rfio_open64((char *)url.c_str(), O_RDONLY);
        castor::exception::SErrnum::throwOnMinusOne(m_fd, "Failed rfio_open64() in diskFile::ReadFile::ReadFile");
      }
      
      size_t ReadFile::read(void *data, const size_t size)  {
        return rfio_read(m_fd, data, size);
      }
      
      ReadFile::~ReadFile() throw() {
        rfio_close(m_fd);
      }
      
      WriteFile::WriteFile(const std::string &url)  : closeTried(false){
        m_fd = rfio_open64((char *)url.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0666); 
        /* 
         * The O_TRUNC flag is here to prevent file corruption in case retrying to write a file to disk.
         * In principle this should be totally safe as the filenames are generated using unique ids given by
         * the database, so the protection is provided by the uniqueness of the filenames.
         * As a side note, we tried to use the O_EXCL flag as well (which would provide additional safety)
         * however this flag does not work with rfio_open64 as apparently it causes memory corruption.
         */
        castor::exception::SErrnum::throwOnMinusOne(m_fd, "Failed rfio_open64() in diskFile::WriteFile::WriteFile");        
      }
      
      void WriteFile::write(const void *data, const size_t size)  {
        rfio_write(m_fd, (void *)data, size);
      }
      
      void WriteFile::close()  {
        closeTried=true;
        castor::exception::Errnum::throwOnMinusOne(rfio_close(m_fd), "Failed rfio_close() in diskFile::WriteFile::close");        
      }
      
      WriteFile::~WriteFile() throw() {
        if(!closeTried){
          rfio_close(m_fd);
        }
      }
    } //end of namespace diskFile
    
  }}