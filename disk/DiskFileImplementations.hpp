/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "XrdClException.hpp"
#include "common/exception/Exception.hpp"
#include "disk/DiskFile.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "tapeserver/castor/tape/tapeserver/file/Structures.hpp"

#include <xrootd/XrdCl/XrdClFile.hh>

namespace cta::disk {
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
  explicit LocalReadFile(const std::string& path);
  size_t size() const final;
  size_t read(void* data, const size_t size) const final;
  ~LocalReadFile() noexcept final;

private:
  int m_fd;
};

class LocalWriteFile : public WriteFile {
public:
  explicit LocalWriteFile(const std::string& path);
  void write(const void* data, const size_t size) final;
  void close() final;
  ~LocalWriteFile() noexcept final;

private:
  int m_fd;
  bool m_closeTried = false;
};

//==============================================================================
// XROOT FILES
//==============================================================================
class XrootBaseReadFile : public ReadFile {
public:
  explicit XrootBaseReadFile(uint16_t timeout) : m_timeout(timeout) {}

  size_t size() const final;
  size_t read(void* data, const size_t size) const final;
  ~XrootBaseReadFile() noexcept override;

protected:
  // Access to parent's protected member...
  void setURL(const std::string& v) { m_URL = v; }

  // There is no const-correctness with XrdCl...
  mutable XrdCl::File m_xrootFile;
  mutable uint64_t m_readPosition;
  const uint16_t m_timeout;
};

class XrootReadFile : public XrootBaseReadFile {
public:
  explicit XrootReadFile(const std::string& xrootUrl, uint16_t timeout = 0);
};

class XrootBaseWriteFile : public WriteFile {
public:
  explicit XrootBaseWriteFile(uint16_t timeout) : m_timeout(timeout) {}

  void write(const void* data, const size_t size) final;
  void close() final;
  ~XrootBaseWriteFile() noexcept override;

protected:
  // Access to parent's protected member...
  void setURL(const std::string& v) { m_URL = v; }

  XrdCl::File m_xrootFile;
  uint64_t m_writePosition = 0;
  const uint16_t m_timeout;
  bool m_closeTried = false;
};

class XrootWriteFile : public XrootBaseWriteFile {
public:
  explicit XrootWriteFile(const std::string& xrootUrl, uint16_t timeout = 0);
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
  explicit LocalDiskFileRemover(const std::string& path);
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
  explicit AsyncLocalDiskFileRemover(const std::string& path);
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
  explicit AsyncXRootdDiskFileRemover(const std::string& path);
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
  explicit XRootdDiskFileRemover(const std::string& path);
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
  explicit LocalDirectory(const std::string& path);
  void mkdir() override;
  bool exist() override;
  std::set<std::string> getFilesName() override;
  void rmdir() override;
};

class XRootdDirectory : public Directory {
public:
  explicit XRootdDirectory(const std::string& path);
  void mkdir() override;
  bool exist() override;
  std::set<std::string> getFilesName() override;
  void rmdir() override;

private:
  XrdCl::FileSystem m_xrootFileSystem;
  std::string m_truncatedDirectoryURL;  // root://.../ part of the path is removed
  const uint16_t c_xrootTimeout = 15;
};
}  // namespace cta::disk
