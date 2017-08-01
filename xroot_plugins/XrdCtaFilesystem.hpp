/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "catalogue/Catalogue.hpp"
#include "common/Configuration.hpp"
#include "common/log/Logger.hpp"
#include "common/make_unique.hpp"
#include "objectstore/BackendPopulator.hpp"
#include "objectstore/BackendVFS.hpp"
#include "scheduler/OStoreDB/OStoreDBWithAgent.hpp"
#include "scheduler/Scheduler.hpp"
#include "objectstore/AgentHeartbeatThread.hpp"

#include <google/protobuf/util/json_util.h>
#include <memory>
#include <XrdSfs/XrdSfsInterface.hh>


namespace cta { namespace xroot_plugins {
/**
 * This class is the entry point for the xroot plugin: it is returned by 
 * XrdSfsGetFileSystem when the latter is called by the xroot server to load the
 * plugin. All function documentation can be found in XrdSfs/XrdSfsInterface.hh.
 */
class XrdCtaFilesystem : public ::XrdSfsFileSystem {
public:
  virtual XrdSfsDirectory *newDir(char *user=0, int MonID=0);
  virtual XrdSfsFile *newFile(char *user=0, int MonID=0);
  virtual int chksum(csFunc Func, const char *csName, const char *path, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0,const char *opaque = 0);
  virtual int chmod(const char *path, XrdSfsMode mode, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0, const char *opaque = 0);
  virtual void Disc(const XrdSecEntity *client = 0);
  virtual void EnvInfo(XrdOucEnv *envP);
  virtual int fsctl(const int cmd, const char *args, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0);
  virtual int getStats(char *buff, int blen);
  virtual const char *getVersion();
  virtual int exists(const char *path, XrdSfsFileExistence &eFlag, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0, const char *opaque = 0);
  virtual int mkdir(const char *path, XrdSfsMode mode, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0, const char *opaque = 0);
  virtual int prepare(XrdSfsPrep &pargs, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0);
  virtual int rem(const char *path, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0, const char *opaque = 0);
  virtual int remdir(const char *path, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0, const char *opaque = 0);
  virtual int rename(const char *oPath, const char *nPath, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0, const char *opaqueO = 0, const char *opaqueN = 0);
  virtual int stat(const char *Name, struct ::stat *buf, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0,const char *opaque = 0);
  virtual int stat(const char *path, mode_t &mode, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0, const char *opaque = 0);
  virtual int truncate(const char *path, XrdSfsFileOffset fsize, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0, const char *opaque = 0);
  XrdCtaFilesystem();
  ~XrdCtaFilesystem();
  
protected:

  /**
   * The XrdCtaFilesystem's own buffer pool that avoids the need to rely on the
   * implicit buffer pool created by the constructor of XrdOucBuffer.
   *
   * The constructor of XrdOucBuffer creates a globally unique buffer pool for
   * all one-time buffers.  The code used to make the buffer pool globally
   * unique is not guaranteed to be thread-safe.  The XrdCtaFilesystem therefore
   * creates its own buffer pool in a thread safe manner.
   *
   * It turns out that the C++ compiler will under certain circumstances add an
   * implicit guard around the initialisation of a static local variable.  The
   * creation of the globally unique buffer pool for all on-time buffers IS
   * THREAD SAFE on CentOS Linux release 7.3.1611 (Core) with XRootD version
   * 4.4.1-1.
   * \code
   * > cat /etc/redhat-release
   * CentOS Linux release 7.3.1611 (Core)
   * >
   * > rpm -qf /lib64/libXrdUtils.so.2
   * xrootd-libs-4.4.1-1.el7.x86_64
   * >
   * > gdb /lib64/libXrdUtils.so.2 -batch -ex 'disassemble XrdOucBuffer::XrdOucBuffer(char*, int)' -ex quit | grep nullPool | head -n 1
   * 0x0000000000033a72 <+2>:	cmpb   $0x0,0x258447(%rip)        # 0x28bec0 <_ZGVZN12XrdOucBufferC1EPciE8nullPool>
   * > c++filt _ZGVZN12XrdOucBufferC1EPciE8nullPool
   * guard variable for XrdOucBuffer::XrdOucBuffer(char*, int)::nullPool
   * >
   * \endcode
   */
  XrdOucBuffPool m_xrdOucBuffPool;
  
  /**
   * The CTA configuration
   */
  cta::common::Configuration m_ctaConf;
  
  /**
   * The VFS backend for the objectstore DB
   */
  std::unique_ptr<cta::objectstore::Backend> m_backend;
  
  /**
   * The object used to populate the backend
   */
  std::unique_ptr<cta::objectstore::BackendPopulator> m_backendPopulator;
  
  /**
   * The database or object store holding all CTA persistent objects
   */
  std::unique_ptr<cta::OStoreDBWithAgent> m_scheddb;

  /**
   * The CTA catalogue of tapes and tape files.
   */
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;

  /**
   * The scheduler.
   */
  std::unique_ptr<cta::Scheduler> m_scheduler;
  
  /**
   * The agent heartbeat thread
   */
  std::unique_ptr<objectstore::AgentHeartbeatThread> m_agentHeartbeat;
  
  /**
   * The logger.
   */
  std::unique_ptr<log::Logger> m_log;

  /**
   * A deleter for instances of the XrdOucBuffer class.
   *
   * The destructor of the XrdOucBuffer class is private.  The Recycle()
   * method can be called on an instance of the XrdOucBuffer class in order to
   * effective delete that instance.
   */
  struct XrdOucBufferDeleter {
    void operator()(XrdOucBuffer* buf) {
      buf->Recycle();
    }
  };

  /**
   * Convenience typedef for an std::unique_ptr type that can delete instances
   * of the XrdOucBuffer class.
   */
  typedef std::unique_ptr<XrdOucBuffer, XrdOucBufferDeleter> UniqueXrdOucBuffer;

  /**
   * Convenience method to allocate an XrdOucBuffer, copy in the specified data
   * and then wrap the XrdOucBuffer in a UniqueXrdOucBuffer.
   *
   * @param dataSize The size of the data to be copied into the XrdOucBuffer.
   * @param data A pointer to the data to be copied into the XrdOucBuffer.
   */
  UniqueXrdOucBuffer make_UniqueXrdOucBuffer(const size_t dataSize, const char *const data) {
    XrdOucBuffer *const xbuf = m_xrdOucBuffPool.Alloc(dataSize);
    if(nullptr == xbuf) {
      cta::exception::Exception ex;
      ex.getMessage() << __FUNCTION__ << " failed: Failed to allocate an XrdOucBuffer";
      throw ex;
    }

    memcpy(xbuf->Buffer(), data, dataSize);
    xbuf->SetLen(dataSize);
    return UniqueXrdOucBuffer(xbuf);
  }
}; // XrdCtaFilesystem

}}
