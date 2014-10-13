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
#include "castor/server/MutexLocker.hpp"
#include "castor/tape/tapegateway/FilesToMigrateList.hpp"
#include <rfio_api.h>
#include <xrootd/XrdCl/XrdClFile.hh>
#include <radosstriper/libradosstriper.hpp>
#include <algorithm>
#include <cryptopp/base64.h>
#include <cryptopp/osrng.h>

namespace castor {
namespace tape {
namespace diskFile {

DiskFileFactory::DiskFileFactory(const std::string & remoteFileProtocol,
  const std::string & xrootPrivateKeyFile):
  m_NoURLLocalFile("^(localhost:|)(/.*)$"),
  m_NoURLRemoteFile("^(.*:)(/.*)$"),
  m_NoURLRadosStriperFile("^localhost:([^/]+)/(.*)$"),
  m_URLLocalFile("^file://(.*)$"),
  m_URLRfioFile("^rfio://(.*)$"),
  m_URLXrootFile("^(root://.*)$"),
  m_URLCephFile("^radosStriper://(.*)$"),
  m_remoteFileProtocol(remoteFileProtocol),
  m_xrootPrivateKeyFile(xrootPrivateKeyFile),
  m_xrootPrivateKeyLoaded(false)
{
  // Lowercase the protocol string
  std::transform(m_remoteFileProtocol.begin(), m_remoteFileProtocol.end(),
    m_remoteFileProtocol.begin(), ::tolower);
}

const CryptoPP::RSA::PrivateKey & DiskFileFactory::xrootPrivateKey() {
  if(!m_xrootPrivateKeyLoaded) {
    // The loading of a PEM-style key is described in 
    // http://www.cryptopp.com/wiki/Keys_and_Formats#PEM_Encoded_Keys
    std::string key;
    std::ifstream keyFile(m_xrootPrivateKeyFile.c_str());
    char buff[200];
    while(!keyFile.eof()) {
      keyFile.read(buff, sizeof(buff));
      key.append(buff,keyFile.gcount());
    }
    const std::string HEADER = "-----BEGIN RSA PRIVATE KEY-----";
    const std::string FOOTER = "-----END RSA PRIVATE KEY-----";
        
    size_t pos1, pos2;
    pos1 = key.find(HEADER);
    if(pos1 == std::string::npos)
        throw castor::exception::Exception(
          "In DiskFileFactory::xrootCryptoPPPrivateKey, PEM header not found");
        
    pos2 = key.find(FOOTER, pos1+1);
    if(pos2 == std::string::npos)
        throw castor::exception::Exception(
          "In DiskFileFactory::xrootCryptoPPPrivateKey, PEM footer not found");
        
    // Start position and length
    pos1 = pos1 + HEADER.length();
    pos2 = pos2 - pos1;
    std::string keystr = key.substr(pos1, pos2);
    
    // Base64 decode, place in a ByteQueue    
    CryptoPP::ByteQueue queue;
    CryptoPP::Base64Decoder decoder;
    
    decoder.Attach(new CryptoPP::Redirector(queue));
    decoder.Put((const byte*)keystr.data(), keystr.length());
    decoder.MessageEnd();

    m_xrootPrivateKey.BERDecodePrivateKey(queue, false /*paramsPresent*/, queue.MaxRetrievable());
    
    // BERDecodePrivateKey is a void function. Here's the only check
    // we have regarding the DER bytes consumed.
    if(!queue.IsEmpty())
      throw castor::exception::Exception(
        "In DiskFileFactory::xrootCryptoPPPrivateKey, garbage at end of key");
    
    CryptoPP::AutoSeededRandomPool prng;
    bool valid = m_xrootPrivateKey.Validate(prng, 3);
    if(!valid)
      throw castor::exception::Exception(
        "In DiskFileFactory::xrootCryptoPPPrivateKey, RSA private key is not valid");

    m_xrootPrivateKeyLoaded = true;
  }
  return m_xrootPrivateKey;
}

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
      // In the current CASTOR implementation, the xrootd port is hard coded to 1095
      return new XrootC2FSReadFile(std::string("root://") + regexResult[1] + "1095/" 
        + regexResult[2],xrootPrivateKey());
    } else {
      return new RfioReadFile(regexResult[1]+regexResult[2]);
    }
  }
  // Do we have a radosStriper file?
  regexResult = m_NoURLRadosStriperFile.exec(path);
  if (regexResult.size()) {
    return new XrootC2FSReadFile(
      std::string("root://localhost:1095/")+regexResult[1]+"/"+regexResult[2],xrootPrivateKey(),
        regexResult[1]);
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
      // In the current CASTOR implementation, the xrootd port is hard coded to 1095
      return new XrootC2FSWriteFile(std::string("root://") + regexResult[1] + "1095/" 
        + regexResult[2],xrootPrivateKey());
    } else {
      return new XrootC2FSWriteFile(regexResult[1]+regexResult[2],xrootPrivateKey());
    }
  }
  // Do we have a radosStriper file?
  regexResult = m_NoURLRadosStriperFile.exec(path);
  if (regexResult.size()) {
    return new XrootC2FSWriteFile(
      std::string("root://localhost:1095/")+regexResult[1]+"/"+regexResult[2],xrootPrivateKey(),
        regexResult[1]);
  }
  throw castor::exception::Exception(
      std::string("In DiskFileFactory::createWriteFile failed to parse URL: ")+path);
}

//==============================================================================
// LOCAL READ FILE
//==============================================================================  
LocalReadFile::LocalReadFile(const std::string &path)  {
  m_fd = ::open64((char *)path.c_str(), O_RDONLY);
  m_URL = "file://";
  m_URL += path;
  castor::exception::SErrnum::throwOnMinusOne(m_fd,
    std::string("In diskFile::LocalReadFile::LocalReadFile failed open64() on ")+m_URL);

}

size_t LocalReadFile::read(void *data, const size_t size) const {
  return ::read(m_fd, data, size);
}

size_t LocalReadFile::size() const {
  //struct is mandatory here, because there is a function stat64 
  struct stat64 statbuf;        
  int ret = ::fstat64(m_fd,&statbuf);
  castor::exception::SErrnum::throwOnMinusOne(ret,
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
  castor::exception::SErrnum::throwOnMinusOne(m_fd,
      std::string("In LocalWriteFile::LocalWriteFile() failed to open64() on ")
      +m_URL);        

}

void LocalWriteFile::write(const void *data, const size_t size)  {
  ::write(m_fd, (void *)data, size);
}

void LocalWriteFile::close()  {
  // Multiple close protection
  if (m_closeTried) return;
  m_closeTried=true;
  castor::exception::Errnum::throwOnMinusOne(::close(m_fd),
      std::string("In LocalWriteFile::close failed rfio_close() on ")+m_URL);        
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
  m_URL = "rfio://";
  m_URL += rfioUrl;
  castor::exception::SErrnum::throwOnMinusOne(m_fd, 
      std::string("In RfioReadFile::RfioReadFile failed rfio_open64() on ") + m_URL);

}

size_t RfioReadFile::read(void *data, const size_t size) const {
  return rfio_read(m_fd, data, size);
}

size_t RfioReadFile::size() const {
  //struct is mandatory here, because there is a function stat64 
  struct stat64 statbuf;        
  int ret = rfio_fstat64(m_fd,&statbuf);
  castor::exception::SErrnum::throwOnMinusOne(ret,
    std::string("In RfioReadFile::RfioReadFile failed to rfio_fstat64() on ")+m_URL);
  
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
  m_URL = "rfio://";
  m_URL += rfioUrl;
  castor::exception::SErrnum::throwOnMinusOne(m_fd, 
    std::string("In RfioWriteFile::RfioWriteFile failed rfio_open64()on ")+m_URL);        

}

void RfioWriteFile::write(const void *data, const size_t size)  {
  rfio_write(m_fd, (void *)data, size);
}

void RfioWriteFile::close()  {
  // Multiple close protection
  if (m_closeTried) return;
  m_closeTried=true;
  castor::exception::Errnum::throwOnMinusOne(rfio_close(m_fd), 
    std::string("In RfioWriteFile::close failed rfio_close on ")+m_URL);
}

RfioWriteFile::~RfioWriteFile() throw() {
  if(!m_closeTried){
    rfio_close(m_fd);
  }
}

//==============================================================================
// CRYPTOPP SIGNER
//==============================================================================
castor::server::Mutex CryptoPPSigner::s_mutex;

std::string CryptoPPSigner::sign(const std::string msg, 
  const CryptoPP::RSA::PrivateKey& privateKey) {
  // Global lock as Crypto++ seems not to be thread safe (helgrind complains)
  castor::server::MutexLocker ml(&s_mutex);
  // Create a signer object
  CryptoPP::RSASSA_PKCS1v15_SHA_Signer signer(privateKey);
  // Return value
  std::string ret;
  // Random number generator (not sure it's really used)
  CryptoPP::AutoSeededRandomPool rng;
  // Construct a pipe: msg -> sign -> Base64 encode -> result goes into ret.
  const bool noNewLineInBase64Output = false;
  CryptoPP::StringSource ss1(msg, true, 
      new CryptoPP::SignerFilter(rng, signer,
        new CryptoPP::Base64Encoder(
          new CryptoPP::StringSink(ret), noNewLineInBase64Output)));
  // That's all job's already done.
  return ret;
}

//==============================================================================
// XROOT READ FILE
//==============================================================================  
XrootC2FSReadFile::XrootC2FSReadFile(const std::string &url,
  const CryptoPP::RSA::PrivateKey & xrootPrivateKey,
  const std::string & pool) {
  // Setup parent's members
  m_readPosition = 0;
  m_URL = url;
  // And start opening
  using XrdCl::OpenFlags;
  m_signedURL = m_URL;
  // Turn the bare URL into a Castor URL, by adding opaque tags:
  // ?castor2fs.pfn1=/srv/castor/...  (duplication of the path in practice)
  // ?castor2fs.pool=xxx optional ceph pool
  // ?castor2fs.exptime=(unix time)
  // ?castor2fs.signature=
  //Find the path part of the url. It is the first occurence of "//"
  // after the inital [x]root://
  const std::string scheme = "root://";
  size_t schemePos = url.find(scheme);
  if (std::string::npos == schemePos) 
    throw castor::exception::Exception(
      std::string("In XrootC2FSReadFile::XrootC2FSReadFile could not find the scheme[x]root:// in URL "+
        url));
  size_t pathPos = url.find("/", schemePos + scheme.size());
  if (std::string::npos == pathPos) 
    throw castor::exception::Exception(
      std::string("In XrootC2FSReadFile::XrootC2FSReadFile could not path in URL "+
        url));
  std::string path = url.substr(pathPos + 1);
  // Build signature block
  time_t expTime = time(NULL)+3600;
  std::stringstream signatureBlock;
  signatureBlock << path << "0" << expTime;
  
  // Sign the block
  std::string signature = CryptoPPSigner::sign(signatureBlock.str(), xrootPrivateKey);

  std::stringstream opaqueBloc;
  opaqueBloc << "?castor2fs.pfn1=" << path;
  if (pool.size())
    opaqueBloc << "&castor2fs.pool=" << pool;
  opaqueBloc << "&castor2fs.exptime=" << expTime;
  opaqueBloc << "&castor2fs.signature=" << signature;
  m_signedURL = m_URL + opaqueBloc.str();
  
  // ... and finally open the file
  XrootClEx::throwOnError(m_xrootFile.Open(m_signedURL, OpenFlags::Read),
    std::string("In XrootC2FSReadFile::XrootC2FSReadFile failed XrdCl::File::Open() on ")
    +m_URL+" opaqueBlock="+opaqueBloc.str());
}

XrootReadFile::XrootReadFile(const std::string &xrootUrl) {
  // Setup parent's variables
  m_readPosition = 0;
  m_URL = xrootUrl;
  // and simply open
  using XrdCl::OpenFlags;
  XrootClEx::throwOnError(m_xrootFile.Open(m_URL, OpenFlags::Read),
    std::string("In XrootC2FSReadFile::XrootC2FSReadFile failed XrdCl::File::Open() on ")+m_URL);
}

size_t XrootBaseReadFile::read(void *data, const size_t size) const {
  uint32_t ret;
  XrootClEx::throwOnError(m_xrootFile.Read(m_readPosition, size, data, ret),
    std::string("In XrootReadFile::read failed XrdCl::File::Read() on ")+m_URL);
  m_readPosition += ret;
  return ret;
}

size_t XrootBaseReadFile::size() const {
  const bool forceStat=true;
  XrdCl::StatInfo *statInfo(NULL);
  size_t ret;
  XrootClEx::throwOnError(m_xrootFile.Stat(forceStat, statInfo),
    std::string("In XrootReadFile::size failed XrdCl::File::Stat() on ")+m_URL);
  ret= statInfo->GetSize();
  delete statInfo;
  return ret;
}

XrootBaseReadFile::~XrootBaseReadFile() throw() {
  try{
    m_xrootFile.Close();
  } catch (...) {}
}

//==============================================================================
// XROOT WRITE FILE
//============================================================================== 
XrootC2FSWriteFile::XrootC2FSWriteFile(const std::string &url,
  const CryptoPP::RSA::PrivateKey & xrootPrivateKey,
  const std::string & pool){
  // Setup parent's members
  m_writePosition = 0;
  m_closeTried = false;
  // and start opening
  using XrdCl::OpenFlags;
  m_URL=url;
  m_signedURL = m_URL;
  // Turn the bare URL into a Castor URL, by adding opaque tags:
  // ?castor2fs.pfn1=/srv/castor/...  (duplication of the path in practice)
  // ?castor2fs.pool=xxx optional ceph pool
  // ?castor2fs.exptime=(unix time)
  // ?castor2fs.signature=
  //Find the path part of the url. It is the first occurence of "//"
  // after the inital [x]root://
  const std::string scheme = "root://";
  size_t schemePos = url.find(scheme);
  if (std::string::npos == schemePos) 
    throw castor::exception::Exception(
      std::string("In XrootC2FSWriteFile::XrootC2FSWriteFile could not find the scheme[x]root:// in URL "+
        url));
  size_t pathPos = url.find("//", schemePos + scheme.size());
  if (std::string::npos == pathPos) 
    throw castor::exception::Exception(
      std::string("In XrootC2FSWriteFile::XrootC2FSWriteFile could not path in URL "+
        url));
  std::string path = url.substr(pathPos + 1);
  // Build signature block
  time_t expTime = time(NULL)+3600;
  std::stringstream signatureBlock;
  signatureBlock << path << "0" << expTime;
  
  // Sign the block
  std::string signature = CryptoPPSigner::sign(signatureBlock.str(), xrootPrivateKey);

  std::stringstream opaqueBloc;
  opaqueBloc << "?castor2fs.pfn1=" << path;
  if (pool.size())
    opaqueBloc << "&castor2fs.pool=" << pool;
  opaqueBloc << "&castor2fs.exptime=" << expTime;
  opaqueBloc << "&castor2fs.signature=" << signature;
  m_signedURL = m_URL + opaqueBloc.str();
  
  XrootClEx::throwOnError(m_xrootFile.Open(m_signedURL, OpenFlags::Delete),
    std::string("In XrootC2FSWriteFile::XrootC2FSWriteFile failed XrdCl::File::Open() on ")
    +m_URL);

}

XrootWriteFile::XrootWriteFile(const std::string& xrootUrl) {
  // Setup parent's variables
  m_writePosition = 0;
  m_URL = xrootUrl;
  // and simply open
  using XrdCl::OpenFlags;
  XrootClEx::throwOnError(m_xrootFile.Open(m_URL, OpenFlags::Delete),
    std::string("In XrootWriteFile::XrootWriteFile failed XrdCl::File::Open() on ")+m_URL);

}

void XrootBaseWriteFile::write(const void *data, const size_t size)  {
  XrootClEx::throwOnError(m_xrootFile.Write(m_writePosition, size, data),
    std::string("In XrootWriteFile::write failed XrdCl::File::Write() on ")
    +m_URL);
  m_writePosition += size;
}

void XrootBaseWriteFile::close()  {
  // Multiple close protection
  if (m_closeTried) return;
  m_closeTried=true;
  XrootClEx::throwOnError(m_xrootFile.Close(),
    std::string("In XrootWriteFile::close failed XrdCl::File::Stat() on ")+m_URL);
}

XrootBaseWriteFile::~XrootBaseWriteFile() throw() {
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

size_t RadosStriperReadFile::read(void *data, const size_t size) const {
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
