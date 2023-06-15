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

#pragma once

#include "disk/DiskFile.hpp"
#include "tapeserver/castor/tape/tapeserver/file/Structures.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "common/exception/XrootCl.hpp"
#include "common/exception/Exception.hpp"
#include <xrootd/XrdCl/XrdClFile.hh>
#include <cryptopp/rsa.h>
#include <radosstriper/libradosstriper.hpp>

namespace cta {
namespace disk {
/**
     * Namespace managing the reading and writing of files to and from disk.
     */

//Forward declarations
class XRootdDiskFileRemover;

//==============================================================================
// LOCAL FILES
//==============================================================================
class LocalReadFile : public ReadFile {
public:
  LocalReadFile(const std::string& path);
  virtual size_t size() const;
  virtual size_t read(void* data, const size_t size) const;
  virtual ~LocalReadFile() throw();

private:
  int m_fd;
};

class LocalWriteFile : public WriteFile {
public:
  LocalWriteFile(const std::string& path);
  virtual void write(const void* data, const size_t size);
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
  static std::string sign(const std::string msg, const CryptoPP::RSA::PrivateKey& privateKey);
  static cta::threading::Mutex s_mutex;
};

//==============================================================================
// XROOT FILES
//==============================================================================
class XrootBaseReadFile : public ReadFile {
public:
  XrootBaseReadFile(uint16_t timeout) : m_timeout(timeout) {}

  virtual size_t size() const;
  virtual size_t read(void* data, const size_t size) const;
  virtual ~XrootBaseReadFile() throw();

protected:
  // Access to parent's protected member...
  void setURL(const std::string& v) { m_URL = v; }

  // There is no const-correctness with XrdCl...
  mutable XrdCl::File m_xrootFile;
  mutable uint64_t m_readPosition;
  const uint16_t m_timeout;
  typedef cta::exception::XrootCl XrootClEx;
};

class XrootReadFile : public XrootBaseReadFile {
public:
  XrootReadFile(const std::string& xrootUrl, uint16_t timeout = 0);
};

class XrootC2FSReadFile : public XrootBaseReadFile {
public:
  XrootC2FSReadFile(const std::string& xrootUrl,
                    const CryptoPP::RSA::PrivateKey& privateKey,
                    uint16_t timeout = 0,
                    const std::string& cephPool = "");

  virtual ~XrootC2FSReadFile() throw() {}

private:
  std::string m_signedURL;
};

class XrootBaseWriteFile : public WriteFile {
public:
  XrootBaseWriteFile(uint16_t timeout) : m_writePosition(0), m_timeout(timeout), m_closeTried(false) {}

  virtual void write(const void* data, const size_t size);
  virtual void setChecksum(uint32_t checksum);
  virtual void close();
  virtual ~XrootBaseWriteFile() throw();

protected:
  // Access to parent's protected member...
  void setURL(const std::string& v) { m_URL = v; }

  XrdCl::File m_xrootFile;
  uint64_t m_writePosition;
  const uint16_t m_timeout;
  typedef cta::exception::XrootCl XrootClEx;
  bool m_closeTried;
};

class XrootWriteFile : public XrootBaseWriteFile {
public:
  XrootWriteFile(const std::string& xrootUrl, uint16_t timeout = 0);
};

class XrootC2FSWriteFile : public XrootBaseWriteFile {
public:
  XrootC2FSWriteFile(const std::string& xrootUrl,
                     const CryptoPP::RSA::PrivateKey& privateKey,
                     uint16_t timeout = 0,
                     const std::string& cephPool = "");

  virtual ~XrootC2FSWriteFile() throw() {}

private:
  std::string m_signedURL;
};

//==============================================================================
// RADOS STRIPER FILES
//==============================================================================
// The Rados striper URLs in CASTOR are in the form:
// radosstriper:///user@pool:filePath
// We will not expect the
class RadosStriperReadFile : public ReadFile {
public:
  RadosStriperReadFile(const std::string& fullURL, libradosstriper::RadosStriper* striper, const std::string& osd);
  virtual size_t size() const;
  virtual size_t read(void* data, const size_t size) const;
  virtual ~RadosStriperReadFile() throw();

private:
  libradosstriper::RadosStriper* m_striper;
  std::string m_osd;
  mutable size_t m_readPosition;
};

class RadosStriperWriteFile : public WriteFile {
public:
  RadosStriperWriteFile(const std::string& fullURL, libradosstriper::RadosStriper* striper, const std::string& osd);
  virtual void write(const void* data, const size_t size);
  virtual void setChecksum(uint32_t checksum);
  virtual void close();
  virtual ~RadosStriperWriteFile() throw();

private:
  libradosstriper::RadosStriper* m_striper;
  std::string m_osd;
  size_t m_writePosition;
};

//==============================================================================
// LocalDisk Removers
//==============================================================================

/**
       * This class allows to delete a file from a local disk
       */
class LocalDiskFileRemover : public DiskFileRemover {
public:
  /**
	 * Constructor of the LocalDiskFileRemover
	 * @param path the path of the file to be removed
	 */
  LocalDiskFileRemover(const std::string& path);
  void remove() override;
};

/**
       * This class allows to asynchronously delete a file from a local disk
       */
class AsyncLocalDiskFileRemover : public AsyncDiskFileRemover {
public:
  /**
	 * Constructor of the asynchronous remover
	 * @param diskFileRemover the instance of the LocalDiskFileRemover that will delete the file
	 */
  AsyncLocalDiskFileRemover(const std::string& path);
  void asyncDelete() override;
  void wait() override;

private:
  std::unique_ptr<LocalDiskFileRemover> m_diskFileRemover;
};

/**
       * This class allows to asynchronously delete a file via XRootD
       */
class AsyncXRootdDiskFileRemover : public AsyncDiskFileRemover {
public:
  friend class XRootdDiskFileRemover;
  AsyncXRootdDiskFileRemover(const std::string& path);
  void asyncDelete() override;
  void wait() override;

private:
  std::unique_ptr<XRootdDiskFileRemover> m_diskFileRemover;

  class XRootdFileRemoverResponseHandler : public XrdCl::ResponseHandler {
  public:
    std::promise<void> m_deletionPromise;

  public:
    void HandleResponse(XrdCl::XRootDStatus* status, XrdCl::AnyObject* response) override;
  };

  XRootdFileRemoverResponseHandler m_responseHandler;
};

/**
       * This class allows to delete a file via XRootD (asynchronously or not)
       */
class XRootdDiskFileRemover : public DiskFileRemover {
public:
  XRootdDiskFileRemover(const std::string& path);
  /**
	 * Remove the file in a synchronous way
	 * @throws an exception if the XRootD call status is an Error
	 */
  void remove();
  /**
	 * Remove the file in an asynchronous way
	 * @param responseHandler : the structure that will be updated when the asynchronous deletion is terminated
	 * (even if it fails)
	 * @throws an exception if the XRootD call status is an Error
	 */
  void removeAsync(AsyncXRootdDiskFileRemover::XRootdFileRemoverResponseHandler& responseHandler);

private:
  XrdCl::FileSystem m_xrootFileSystem;
  std::string m_truncatedFileURL;  // root://.../ part of the path is removed
  const uint16_t c_xrootTimeout = 15;
};

class LocalDirectory : public Directory {
public:
  LocalDirectory(const std::string& path);
  void mkdir() override;
  bool exist() override;
  std::set<std::string> getFilesName() override;
  void rmdir() override;
};

class XRootdDirectory : public Directory {
public:
  XRootdDirectory(const std::string& path);
  void mkdir() override;
  bool exist() override;
  std::set<std::string> getFilesName() override;
  void rmdir() override;

private:
  XrdCl::FileSystem m_xrootFileSystem;
  std::string m_truncatedDirectoryURL;  // root://.../ part of the path is removed
  const uint16_t c_xrootTimeout = 15;
};

}  //end of namespace disk
}  // namespace cta
