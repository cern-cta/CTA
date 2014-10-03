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
#include <sys/types.h>

#include "castor/tape/tapeserver/file/DiskFile.hpp"
#include "castor/tape/tapeserver/file/DiskFileImplementations.hpp"
#include "castor/exception/Errnum.hpp"
#include "castor/exception/SErrnum.hpp"
#include "castor/tape/tapegateway/FilesToMigrateList.hpp"
#include <rfio_api.h>
#include <xrootd/XrdCl/XrdClFile.hh>
#include <radosstriper/libradosstriper.hpp>

namespace castor {
namespace tape {
namespace diskFile {

DiskFileFactory::DiskFileFactory(const std::string & remoteFileProtocol):
  m_NoURLLocalFile("^(localhost:|)(/.*)$"),
  m_NoURLRemoteFile("^(.*:/.*)$"),
  m_NoURLRadosStriperFile("^localhost:([^/].*)$"),
  m_URLLocalFile("^file://(.*)$"),
  m_URLRfioFile("^rfio://(.*)$"),
  m_URLXrootFile("^(root://.*)$"),
  m_URLCephFile("^radosStriper://(.*)$"),
  m_remoteFileProtocol(remoteFileProtocol)
{}

ReadFile * DiskFileFactory::createReadFile(const std::string& path) {
  std::vector<std::string> regexResult;
  // URL path parsing
  // local file URL?
  regexResult = m_URLLocalFile.exec(path);
  if (regexResult.size()) {
    return new LocalReadFile(regexResult[1]);
  }
  // RFIO URL?
  regexResult = m_URLRfioFile.exec(path);
  if (regexResult.size()) {
    return new RfioReadFile(regexResult[1]);
  }
  // Xroot URL?
  regexResult = m_URLXrootFile.exec(path);
  if (regexResult.size()) {
    return new XrootReadFile(regexResult[1]);
  }
  // radosStriper URL?
  regexResult = m_URLCephFile.exec(path);
  if (regexResult.size()) {
    return new RadosStriperReadFile(regexResult[1]);
  }
  // No URL path parsing
  // Do we have a local file?
  regexResult = m_NoURLLocalFile.exec(path);
  if (regexResult.size()) {
    return new LocalReadFile(regexResult[2]);
  }
  // Do we have a remote file?
  regexResult = m_NoURLRemoteFile.exec(path);
  if (regexResult.size()) {
    if (m_remoteFileProtocol == "xroot") {
      return new XrootReadFile(std::string("root://") + regexResult[1]);
    } else {
      return new RfioReadFile(regexResult[1]);
    }
  }
  // Do we have a radosStriper file?
  regexResult = m_NoURLRadosStriperFile.exec(path);
  if (regexResult.size()) {
    return new RadosStriperReadFile(regexResult[1]);
  }
  throw castor::exception::Exception(
      std::string("In DiskFileFactory::createReadFile failed to parse URL: ")+path);
}

WriteFile * DiskFileFactory::createWriteFile(const std::string& path) {
  std::vector<std::string> regexResult;
  // URL path parsing
  // local file URL?
  regexResult = m_URLLocalFile.exec(path);
  if (regexResult.size()) {
    return new LocalWriteFile(regexResult[1]);
  }
  // RFIO URL?
  regexResult = m_URLRfioFile.exec(path);
  if (regexResult.size()) {
    return new RfioWriteFile(regexResult[1]);
  }
  // Xroot URL?
  regexResult = m_URLXrootFile.exec(path);
  if (regexResult.size()) {
    return new XrootWriteFile(regexResult[1]);
  }
  // radosStriper URL?
  regexResult = m_URLCephFile.exec(path);
  if (regexResult.size()) {
    return new RadosStriperWriteFile(regexResult[1]);
  }
  // No URL path parsing
  // Do we have a local file?
  regexResult = m_NoURLLocalFile.exec(path);
  if (regexResult.size()) {
    return new LocalWriteFile(regexResult[2]);
  }
  // Do we have a remote file?
  regexResult = m_NoURLRemoteFile.exec(path);
  if (regexResult.size()) {
    if (m_remoteFileProtocol == "xroot") {
      return new XrootWriteFile(std::string("root://") + regexResult[1]);
    } else {
      return new RfioWriteFile(regexResult[1]);
    }
  }
  // Do we have a radosStriper file?
  regexResult = m_NoURLRadosStriperFile.exec(path);
  if (regexResult.size()) {
    return new RadosStriperWriteFile(regexResult[0]);
  }
  throw castor::exception::Exception(
      std::string("In DiskFileFactory::createWriteFile failed to parse URL: ")+path);
}

//==============================================================================
// LOCAL READ FILE
//==============================================================================  
LocalReadFile::LocalReadFile(const std::string &path)  {
  m_fd = ::open64((char *)path.c_str(), O_RDONLY);
  castor::exception::SErrnum::throwOnMinusOne(m_fd,
      "Failed open64() in diskFile::LocalReadFile::LocalReadFile");
}

size_t LocalReadFile::read(void *data, const size_t size)  {
  return ::read(m_fd, data, size);
}

size_t LocalReadFile::size() const {
  //struct is mandatory here, because there is a function stat64 
  struct stat64 statbuf;        
  int ret = ::fstat64(m_fd,&statbuf);
  castor::exception::SErrnum::throwOnMinusOne(ret,
          "Error while trying to stat64() a local file");
  
  return statbuf.st_size;
}

LocalReadFile::~LocalReadFile() throw() {
  ::close(m_fd);
}

//==============================================================================
// LOCAL WRITE FILE
//============================================================================== 
LocalWriteFile::LocalWriteFile(const std::string &path): m_closeTried(false){
  // For local files, we truncate the file like for RFIO
  m_fd = ::open64((char *)path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0666);
  castor::exception::SErrnum::throwOnMinusOne(m_fd,
      "Failed to open64() in LocalWriteFile::LocalWriteFile()");        
}

void LocalWriteFile::write(const void *data, const size_t size)  {
  ::write(m_fd, (void *)data, size);
}

void LocalWriteFile::close()  {
  // Multiple close protection
  if (m_closeTried) return;
  m_closeTried=true;
  castor::exception::Errnum::throwOnMinusOne(::close(m_fd),
      "Failed rfio_close() in diskFile::WriteFile::close");        
}

LocalWriteFile::~LocalWriteFile() throw() {
  if(!m_closeTried){
    ::close(m_fd);
  }
}

//==============================================================================
// RFIO READ FILE
//==============================================================================  
RfioReadFile::RfioReadFile(const std::string &rfioUrl)  {
  m_fd = rfio_open64((char *)rfioUrl.c_str(), O_RDONLY);
  castor::exception::SErrnum::throwOnMinusOne(m_fd, 
      "Failed rfio_open64() in diskFile::RfioReadFile::RfioReadFile");
}

size_t RfioReadFile::read(void *data, const size_t size)  {
  return rfio_read(m_fd, data, size);
}

size_t RfioReadFile::size() const {
  //struct is mandatory here, because there is a function stat64 
  struct stat64 statbuf;        
  int ret = rfio_fstat64(m_fd,&statbuf);
  castor::exception::SErrnum::throwOnMinusOne(ret,
          "Error while trying to read some stats on an RFIO file");
  
  return statbuf.st_size;
}

RfioReadFile::~RfioReadFile() throw() {
  rfio_close(m_fd);
}

//==============================================================================
// RFIO WRITE FILE
//============================================================================== 
RfioWriteFile::RfioWriteFile(const std::string &rfioUrl)  : m_closeTried(false){
  m_fd = rfio_open64((char *)rfioUrl.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0666); 
  /* 
   * The O_TRUNC flag is here to prevent file corruption in case retrying to write a file to disk.
   * In principle this should be totally safe as the filenames are generated using unique ids given by
   * the database, so the protection is provided by the uniqueness of the filenames.
   * As a side note, we tried to use the O_EXCL flag as well (which would provide additional safety)
   * however this flag does not work with rfio_open64 as apparently it causes memory corruption.
   */
  castor::exception::SErrnum::throwOnMinusOne(m_fd, "Failed rfio_open64() in RfioWriteFile::RfioWriteFile::RfioWriteFile");        
}

void RfioWriteFile::write(const void *data, const size_t size)  {
  rfio_write(m_fd, (void *)data, size);
}

void RfioWriteFile::close()  {
  // Multiple close protection
  if (m_closeTried) return;
  m_closeTried=true;
  castor::exception::Errnum::throwOnMinusOne(rfio_close(m_fd), "Failed rfio_close() in diskFile::WriteFile::close");        
}

RfioWriteFile::~RfioWriteFile() throw() {
  if(!m_closeTried){
    rfio_close(m_fd);
  }
}

//==============================================================================
// XROOT READ FILE
//==============================================================================  
XrootReadFile::XrootReadFile(const std::string &url):
m_readPosition(0){
  using XrdCl::OpenFlags;
  XrootClEx::throwOnError(m_xrootFile.Open(url, OpenFlags::Read|OpenFlags::SeqIO),
    "Error while calling XrdCl::Open() in XrootReadFile::XrootReadFile()");
}

size_t XrootReadFile::read(void *data, const size_t size)  {
  uint32_t ret;
  XrootClEx::throwOnError(m_xrootFile.Read(m_readPosition, size, data, ret),
    "Error while calling XrdCl::Read() in XrootReadFile::read()");
  m_readPosition += ret;
  return ret;
}

size_t XrootReadFile::size() const {
  const bool forceStat=true;
  XrdCl::StatInfo *statInfo(NULL);
  size_t ret;
  XrootClEx::throwOnError(m_xrootFile.Stat(forceStat, statInfo),
    "Error while calling XrdCl::Stat() in XrootReadFile::size()");
  ret= statInfo->GetSize();
  delete statInfo;
  return ret;
}

XrootReadFile::~XrootReadFile() throw() {
  m_xrootFile.Close();
}

//==============================================================================
// XROOT WRITE FILE
//============================================================================== 
XrootWriteFile::XrootWriteFile(const std::string &url):
m_writePosition(0),m_closeTried(false){
  using XrdCl::OpenFlags;
  XrootClEx::throwOnError(m_xrootFile.Open(url, OpenFlags::Delete|OpenFlags::SeqIO),
    "Error while calling XrdCl::Open() in XrootWriteFile::XrootWriteFile()");
}

void XrootWriteFile::write(const void *data, const size_t size)  {
  XrootClEx::throwOnError(m_xrootFile.Write(m_writePosition, size, data),
    "Error while calling XrdCl::Write() in XrootWriteFile::write()");
  m_writePosition += size;
}

void XrootWriteFile::close()  {
  // Multiple close protection
  if (m_closeTried) return;
  m_closeTried=true;
  XrootClEx::throwOnError(m_xrootFile.Close(),
     "Error while calling XrdCl::Stat() in XrootWriteFile::close()");
}

XrootWriteFile::~XrootWriteFile() throw() {
  if(!m_closeTried){
    m_xrootFile.Close();
  }
}

//==============================================================================
// RADOS STRIPER READ FILE
//==============================================================================
RadosStriperReadFile::RadosStriperReadFile(const std::string &url){
  throw castor::exception::Exception(
      "RadosStriperReadFile::RadosStriperReadFile is not implemented");
}

size_t RadosStriperReadFile::read(void *data, const size_t size)  {
  throw castor::exception::Exception(
      "RadosStriperReadFile::read is not implemented");
}

size_t RadosStriperReadFile::size() const {
  throw castor::exception::Exception(
      "RadosStriperReadFile::size is not implemented");
}

RadosStriperReadFile::~RadosStriperReadFile() throw() {
}

//==============================================================================
// RADOS STRIPER WRITE FILE
//============================================================================== 
RadosStriperWriteFile::RadosStriperWriteFile(const std::string &url){
  throw castor::exception::Exception(
      "RadosStriperWriteFile::RadosStriperWriteFile is not implemented");
}

void RadosStriperWriteFile::write(const void *data, const size_t size)  {
  throw castor::exception::Exception(
      "RadosStriperWriteFile::write is not implemented");
}

void RadosStriperWriteFile::close()  {
  throw castor::exception::Exception(
      "RadosStriperWriteFile::close is not implemented");
}

RadosStriperWriteFile::~RadosStriperWriteFile() throw() {
}

}}} //end of namespace diskFile
