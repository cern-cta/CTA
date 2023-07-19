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
#include <sys/types.h>
#include <sys/stat.h>

#include "disk/DiskFileImplementations.hpp"
#include "disk/RadosStriperPool.hpp"
#include "common/exception/Errnum.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/utils/utils.hpp"
#include <rados/buffer.h>
#include <xrootd/XrdCl/XrdClFile.hh>
#include <uuid/uuid.h>
#include <algorithm>

namespace cta {
namespace disk {

DiskFileFactory::DiskFileFactory(uint16_t xrootTimeout, cta::disk::RadosStriperPool& striperPool) :
  m_NoURLLocalFile("^(localhost:|)(/.*)$"),
  m_URLLocalFile("^file://(.*)$"),
  m_URLXrootFile("^(root://.*)$"),
  m_URLCephFile("^radosstriper:///([^:]+@[^:]+):(.*)$"),
  m_xrootTimeout(xrootTimeout),
  m_striperPool(striperPool) {}

ReadFile * DiskFileFactory::createReadFile(const std::string& path) {
  std::vector<std::string> regexResult;
  // URL path parsing
  // local file URL?
  regexResult = m_URLLocalFile.exec(path);
  if (regexResult.size()) {
    return new LocalReadFile(regexResult[1]);
  }
  // Xroot URL?
  regexResult = m_URLXrootFile.exec(path);
  if (regexResult.size()) {
    return new XrootReadFile(regexResult[1], m_xrootTimeout);
  }
  // radosStriper URL?
  regexResult = m_URLCephFile.exec(path);
  if (regexResult.size()) {
    return new RadosStriperReadFile(regexResult[0],
      m_striperPool.throwingGetStriper(regexResult[1]),
      regexResult[2]);
  }
  // No URL path parsing
  // Do we have a local file?
  regexResult = m_NoURLLocalFile.exec(path);
  if (regexResult.size()) {
    return new LocalReadFile(regexResult[2]);
  }
  throw cta::exception::Exception(
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
  // Xroot URL?
  regexResult = m_URLXrootFile.exec(path);
  if (regexResult.size()) {
    return new XrootWriteFile(regexResult[1], m_xrootTimeout);
  }
  // radosStriper URL?
  regexResult = m_URLCephFile.exec(path);
  if (regexResult.size()) {
    return new RadosStriperWriteFile(regexResult[0],
      m_striperPool.throwingGetStriper(regexResult[1]),
      regexResult[2]);
  }
  // No URL path parsing
  // Do we have a local file?
  regexResult = m_NoURLLocalFile.exec(path);
  if (regexResult.size()) {
    return new LocalWriteFile(regexResult[2]);
  }
  throw cta::exception::Exception(
      std::string("In DiskFileFactory::createWriteFile failed to parse URL: ")+path);
}

//==============================================================================
// LOCAL READ FILE
//==============================================================================
LocalReadFile::LocalReadFile(const std::string &path)  {
  m_fd = ::open64((char *)path.c_str(), O_RDONLY);
  m_URL = "file://";
  m_URL += path;
  cta::exception::Errnum::throwOnMinusOne(m_fd,
    std::string("In diskFile::LocalReadFile::LocalReadFile failed open64() on ")+m_URL);

}

size_t LocalReadFile::read(void *data, const size_t size) const {
  return ::read(m_fd, data, size);
}

size_t LocalReadFile::size() const {
  //struct is mandatory here, because there is a function stat64
  struct stat64 statbuf;
  int ret = ::fstat64(m_fd,&statbuf);
  cta::exception::Errnum::throwOnMinusOne(ret,
    std::string("In diskFile::LocalReadFile::LocalReadFile failed stat64() on ")+m_URL);

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
  m_URL = "file://";
  m_URL += path;
  cta::exception::Errnum::throwOnMinusOne(m_fd,
      std::string("In LocalWriteFile::LocalWriteFile() failed to open64() on ")
      +m_URL);

}

void LocalWriteFile::write(const void *data, const size_t size)  {
  ::write(m_fd, (void *)data, size);
}

void LocalWriteFile::setChecksum(uint32_t checksum) {
  // Noop: this is only implemented for rados striper
}

void LocalWriteFile::close()  {
  // Multiple close protection
  if (m_closeTried) return;
  m_closeTried=true;
  cta::exception::Errnum::throwOnMinusOne(::close(m_fd),
      std::string("In LocalWriteFile::close failed close() on ")+m_URL);
}

LocalWriteFile::~LocalWriteFile() throw() {
  if(!m_closeTried){
    ::close(m_fd);
  }
}

//==============================================================================
// XROOT READ FILE
//==============================================================================
XrootReadFile::XrootReadFile(const std::string &xrootUrl, uint16_t timeout):
  XrootBaseReadFile(timeout) {
  // Setup parent's variables
  m_readPosition = 0;
  
  // and simply open
  using XrdCl::OpenFlags;
  XrootClEx::throwOnError(m_xrootFile.Open(xrootUrl, OpenFlags::Read, XrdCl::Access::None, m_timeout),
    std::string("In XrootReadFile::XrootReadFile failed XrdCl::File::Open() on ")+xrootUrl);
  m_xrootFile.GetProperty("LastURL", m_URL);
}

size_t XrootBaseReadFile::read(void *data, const size_t size) const {
  uint32_t ret;
  XrootClEx::throwOnError(m_xrootFile.Read(m_readPosition, size, data, ret, m_timeout),
    std::string("In XrootReadFile::read failed XrdCl::File::Read() on ")+m_URL);
  m_readPosition += ret;
  return ret;
}

size_t XrootBaseReadFile::size() const {
  const bool forceStat=false;
  XrdCl::StatInfo *statInfo(nullptr);
  size_t ret;
  XrootClEx::throwOnError(m_xrootFile.Stat(forceStat, statInfo, m_timeout),
    std::string("In XrootReadFile::size failed XrdCl::File::Stat() on ")+m_URL);
  ret= statInfo->GetSize();
  delete statInfo;
  return ret;
}

XrootBaseReadFile::~XrootBaseReadFile() throw() {
  try{
    // Use the result of Close() to avoid gcc >= 7 generating an unused-result
    // warning (casting the result to void is not good enough for gcc >= 7)
    if(!m_xrootFile.Close(m_timeout).IsOK()) {
      // Ignore the error
    }
  } catch (...) {}
}

//==============================================================================
// XROOT WRITE FILE
//==============================================================================
XrootWriteFile::XrootWriteFile(const std::string& xrootUrl, uint16_t timeout):
  XrootBaseWriteFile(timeout) {
  using XrdCl::OpenFlags;
  XrootClEx::throwOnError(m_xrootFile.Open(xrootUrl, OpenFlags::Delete | OpenFlags::Write,
    XrdCl::Access::None, m_timeout),
    std::string("In XrootWriteFile::XrootWriteFile failed XrdCl::File::Open() on ")+xrootUrl);
  m_xrootFile.GetProperty("LastURL", m_URL);
}

void XrootBaseWriteFile::write(const void *data, const size_t size)  {
  XrootClEx::throwOnError(m_xrootFile.Write(m_writePosition, size, data, m_timeout),
    std::string("In XrootWriteFile::write failed XrdCl::File::Write() on ")
    +m_URL);
  m_writePosition += size;
}

void XrootBaseWriteFile::setChecksum(uint32_t checksum) {
  // Noop: this is only implemented for rados striper
}

void XrootBaseWriteFile::close()  {
  // Multiple close protection
  if (m_closeTried) return;
  m_closeTried=true;
  XrootClEx::throwOnError(m_xrootFile.Close(m_timeout),
    std::string("In XrootWriteFile::close failed XrdCl::File::Close() on ")+m_URL);
}

XrootBaseWriteFile::~XrootBaseWriteFile() throw() {
  if(!m_closeTried){
    // Use the result of Close() to avoid gcc >= 7 generating an unused-result
    // warning (casting the result to void is not good enough for gcc >= 7)
    if(!m_xrootFile.Close(m_timeout).IsOK()) {
      // Ignore the error
    }
  }
}

//==============================================================================
// RADOS STRIPER READ FILE
//==============================================================================
RadosStriperReadFile::RadosStriperReadFile(const std::string &fullURL,
  libradosstriper::RadosStriper * striper,
  const std::string &osd): m_striper(striper),
  m_osd(osd), m_readPosition(0) {
    m_URL=fullURL;
}

size_t RadosStriperReadFile::read(void *data, const size_t size) const {
  ::ceph::bufferlist bl;
  int rc = m_striper->read(m_osd, &bl, size, m_readPosition);
  if (rc < 0) {
    throw cta::exception::Errnum(-rc,
        "In RadosStriperReadFile::read(): failed to striper->read: ");
  }
  bl.begin().copy(rc, (char *)data);
  m_readPosition += rc;
  return rc;
}

size_t RadosStriperReadFile::size() const {
  uint64_t size;
  time_t time;
  cta::exception::Errnum::throwOnReturnedErrno(
      -m_striper->stat(m_osd, &size, &time),
      "In RadosStriperReadFile::size(): failed to striper->stat(): ");
  return size;
}

RadosStriperReadFile::~RadosStriperReadFile() throw() {}

//==============================================================================
// RADOS STRIPER WRITE FILE
//==============================================================================
RadosStriperWriteFile::RadosStriperWriteFile(const std::string &fullURL,
  libradosstriper::RadosStriper * striper,
  const std::string &osd): m_striper(striper),
  m_osd(osd), m_writePosition(0) {
  m_URL=fullURL;
  // Truncate the possibly existing file. If the file does not exist, it's fine.
  int rc=m_striper->trunc(m_osd, 0);
  if (rc < 0 && rc != -ENOENT) {
    throw cta::exception::Errnum(-rc, "In RadosStriperWriteFile::RadosStriperWriteFile(): "
        "failed to striper->trunc(): ");
  }
}

void RadosStriperWriteFile::write(const void *data, const size_t size)  {
  ::ceph::bufferlist bl;
  bl.append((char *)data, size);
  int rc = m_striper->write(m_osd, bl, size, m_writePosition);
  if (rc) {
    throw cta::exception::Errnum(-rc, "In RadosStriperWriteFile::write(): "
        "failed to striper->write(): ");
  }
  m_writePosition += size;
}

void RadosStriperWriteFile::setChecksum(uint32_t checksum) {
  // Set the checksum type (hardcoded)
  int rc;
  std::string checksumType("ADLER32");
  ::ceph::bufferlist blType;
  blType.append(checksumType.c_str(), checksumType.size());
  rc = m_striper->setxattr(m_osd, "user.castor.checksum.type", blType);
  if (rc) {
    throw cta::exception::Errnum(-rc, "In RadosStriperWriteFile::setChecksum(): "
        "failed to striper->setxattr(user.castor.checksum.type): ");
  }
  // Turn the numeric checksum into a string and set it as checksum value
  std::stringstream checksumStr;
  checksumStr << std::hex << std::nouppercase << checksum;
  ::ceph::bufferlist blChecksum;
  blChecksum.append(checksumStr.str().c_str(), checksumStr.str().size());
  rc = m_striper->setxattr(m_osd, "user.castor.checksum.value", blChecksum);
  if (rc) {
    throw cta::exception::Errnum(-rc, "In RadosStriperWriteFile::setChecksum(): "
        "failed to striper->setxattr(user.castor.checksum.value): ");
  }
}

void RadosStriperWriteFile::close()  {
  // Nothing to do as writes are synchronous
}

RadosStriperWriteFile::~RadosStriperWriteFile() throw() {}

//==============================================================================
// AsyncDiskFileRemover FACTORY
//==============================================================================
AsyncDiskFileRemoverFactory::AsyncDiskFileRemoverFactory():
    m_URLLocalFile("^file://(.*)$"),
    m_URLXrootdFile("^(root://.*)$"){}

AsyncDiskFileRemover * AsyncDiskFileRemoverFactory::createAsyncDiskFileRemover(const std::string &path){
  // URL path parsing
  std::vector<std::string> regexResult;
  //local file URL?
  regexResult = m_URLLocalFile.exec(path);
  if(regexResult.size()){
    return new AsyncLocalDiskFileRemover(regexResult[1]);
  }
  regexResult = m_URLXrootdFile.exec(path);
  if(regexResult.size()){
    return new AsyncXRootdDiskFileRemover(path);
  }
  throw cta::exception::Exception("In DiskFileRemoverFactory::createDiskFileRemover: unknown type of URL");
}


//==============================================================================
// LocalDiskFileRemover
//==============================================================================
LocalDiskFileRemover::LocalDiskFileRemover(const std::string &path){
  m_URL = path;
}

void LocalDiskFileRemover::remove(){
  cta::exception::Errnum::throwOnNonZero(::remove(m_URL.c_str()),"In LocalDiskFileRemover::remove(), failed to delete the file at "+m_URL);
}

//==============================================================================
// XRootdDiskFileRemover
//==============================================================================
XRootdDiskFileRemover::XRootdDiskFileRemover(const std::string& path):m_xrootFileSystem(path){
  m_URL = path;
  m_truncatedFileURL = cta::utils::extractPathFromXrootdPath(path);
}

void XRootdDiskFileRemover::remove(){
  XrdCl::XRootDStatus statusRm = m_xrootFileSystem.Rm(m_truncatedFileURL,c_xrootTimeout);
  cta::exception::XrootCl::throwOnError(statusRm,"In XRootdDiskFileRemover::remove(), fail to remove file.");;
}

void XRootdDiskFileRemover::removeAsync(AsyncXRootdDiskFileRemover::XRootdFileRemoverResponseHandler &responseHandler){
  XrdCl::XRootDStatus statusRm = m_xrootFileSystem.Rm(m_truncatedFileURL,&responseHandler,c_xrootTimeout);
  try{
    cta::exception::XrootCl::throwOnError(statusRm,"In XRootdDiskFileRemover::remove(), fail to remove file.");
  } catch(const cta::exception::Exception &e){
    responseHandler.m_deletionPromise.set_exception(std::current_exception());
  }
}

//==============================================================================
// AsyncXrootdDiskFileRemover
//==============================================================================
AsyncXRootdDiskFileRemover::AsyncXRootdDiskFileRemover(const std::string &path){
  m_diskFileRemover.reset(new XRootdDiskFileRemover(path));
}

void AsyncXRootdDiskFileRemover::asyncDelete(){
  m_diskFileRemover->removeAsync(m_responseHandler);
}

void AsyncXRootdDiskFileRemover::wait(){
  m_responseHandler.m_deletionPromise.get_future().get();
}

void AsyncXRootdDiskFileRemover::XRootdFileRemoverResponseHandler::HandleResponse(XrdCl::XRootDStatus* status, XrdCl::AnyObject* response){
  try{
    cta::exception::XrootCl::throwOnError(*status,"In XRootdDiskFileRemover::remove(), fail to remove file.");
    m_deletionPromise.set_value();
  } catch(const cta::exception::Exception &e){
    m_deletionPromise.set_exception(std::current_exception());
  }
}

//==============================================================================
// AsyncLocalDiskFileRemover
//==============================================================================
AsyncLocalDiskFileRemover::AsyncLocalDiskFileRemover(const std::string& path){
  m_diskFileRemover.reset(new LocalDiskFileRemover(path));
}

void AsyncLocalDiskFileRemover::asyncDelete(){
  m_futureDeletion = std::async(std::launch::async,[this](){m_diskFileRemover->remove();});
}

void AsyncLocalDiskFileRemover::wait(){
  m_futureDeletion.get();
}

//==============================================================================
// DIRECTORY FACTORY
//==============================================================================
DirectoryFactory::DirectoryFactory():
    m_URLLocalDirectory("^file://(.*)$"),
    m_URLXrootDirectory("^(root://.*)$"){}


Directory * DirectoryFactory::createDirectory(const std::string& path){
  // URL path parsing
  std::vector<std::string> regexResult;
  // local file URL?
  regexResult = m_URLLocalDirectory.exec(path);
  if (regexResult.size()) {
    return new LocalDirectory(regexResult[1]);
  }
  // Xroot URL?
  regexResult = m_URLXrootDirectory.exec(path);
  if (regexResult.size()) {
    return new XRootdDirectory(path);
  }
  throw cta::exception::Exception("In DirectoryFactory::createDirectory: unknown type of URL");
}

//==============================================================================
// LOCAL DIRECTORY
//==============================================================================
LocalDirectory::LocalDirectory(const std::string& path){
  m_URL = path;
}

void LocalDirectory::mkdir(){
  const int retCode = ::mkdir(m_URL.c_str(),S_IRWXU);
  cta::exception::Errnum::throwOnMinusOne(retCode,"In LocalDirectory::mkdir(): failed to create directory at "+m_URL);
}

void LocalDirectory::rmdir(){
  const int retcode = ::rmdir(m_URL.c_str());
  cta::exception::Errnum::throwOnMinusOne(retcode,"In LocalDirectory::rmdir(): failed to remove the directory at "+m_URL);
}

bool LocalDirectory::exist(){
  struct stat buffer;
  return (stat(m_URL.c_str(), &buffer) == 0);
}

std::set<std::string> LocalDirectory::getFilesName(){
  std::set<std::string> names;
  DIR *dir;
  struct dirent *file;
  dir = opendir(m_URL.c_str());
  cta::exception::Errnum::throwOnNull(dir,"In LocalDirectory::getFilesName, failed to open directory at "+m_URL);
  while((file = readdir(dir)) != nullptr){
    char *fileName = file->d_name;
    if(strcmp(fileName,".")  && strcmp(fileName,"..")){
      names.insert(std::string(file->d_name));
    }
  }
  cta::exception::Errnum::throwOnMinusOne(::closedir(dir),"In LocalDirectory::getFilesName(), fail to close directory at "+m_URL);
  return names;
}

//==============================================================================
// XROOT DIRECTORY
//==============================================================================
XRootdDirectory::XRootdDirectory(const std::string& path):m_xrootFileSystem(path){
  m_URL = path;
  m_truncatedDirectoryURL = cta::utils::extractPathFromXrootdPath(path);
}

void XRootdDirectory::mkdir() {
  XrdCl::XRootDStatus mkdirStatus = m_xrootFileSystem.MkDir(m_truncatedDirectoryURL,XrdCl::MkDirFlags::None,XrdCl::Access::Mode::UR | XrdCl::Access::Mode::UW | XrdCl::Access::Mode::UX,c_xrootTimeout);
  cta::exception::XrootCl::throwOnError(mkdirStatus,"In XRootdDirectory::mkdir() : failed to create directory at "+m_URL);
}

void XRootdDirectory::rmdir() {
  XrdCl::XRootDStatus rmdirStatus = m_xrootFileSystem.RmDir(m_truncatedDirectoryURL, c_xrootTimeout);
  cta::exception::XrootCl::throwOnError(rmdirStatus,"In XRootdDirectory::rmdir() : failed to remove directory at "+m_URL);
}

bool XRootdDirectory::exist() {
  XrdCl::StatInfo *statInfo;
  XrdCl::XRootDStatus statStatus = m_xrootFileSystem.Stat(m_truncatedDirectoryURL,statInfo,c_xrootTimeout);
  if(statStatus.errNo == XErrorCode::kXR_NotFound){
    return false;
  }
  //If the EOS instance does not exist, we don't want to return false, we want to throw an exception.
  cta::exception::XrootCl::throwOnError(statStatus,"In XRootdDirectory::exist() : failed to stat the directory at "+m_URL);
  //No exception, return true
  return true;
}

std::set<std::string> XRootdDirectory::getFilesName(){
  std::set<std::string> ret;
  XrdCl::DirectoryList *directoryContent;
  XrdCl::XRootDStatus dirListStatus = m_xrootFileSystem.DirList(m_truncatedDirectoryURL,XrdCl::DirListFlags::Flags::Stat,directoryContent,c_xrootTimeout);
  cta::exception::XrootCl::throwOnError(dirListStatus,"In XrootdDirectory::getFilesName(): unable to list the files contained in the directory.");
  XrdCl::DirectoryList::ConstIterator iter = directoryContent->Begin();
  while(iter != directoryContent->End()){
    ret.insert((*iter)->GetName());
    iter++;
  }
  return ret;
}

}} //end of namespace cta::disk
