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
#include "objectstore/BackendPopulator.hpp"
#include "objectstore/BackendVFS.hpp"
#include "scheduler/OStoreDB/OStoreDBWithAgent.hpp"
#include "scheduler/Scheduler.hpp"

#include "XrdSfs/XrdSfsInterface.hh"

#include <memory>


namespace cta { namespace xrootPlugins { 
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
  virtual int FSctl(const int cmd, XrdSfsFSctl &args, XrdOucErrInfo &eInfo, const XrdSecEntity *client = 0);
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
  cta::objectstore::BackendPopulator m_backendPopulator;
  
  /**
   * The database or object store holding all CTA persistent objects
   */
  cta::OStoreDBWithAgent m_scheddb;

  /**
   * The CTA catalogue of tapes and tape files.
   */
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;

  /**
   * The scheduler.
   */
  std::unique_ptr<cta::Scheduler> m_scheduler;
  
  /**
   * The logger.
   */
  std::unique_ptr<log::Logger> m_log;

  /**
   * Processes the specified wrapper message.
   *
   * @param msg The message.
   * @param eInfo Same semantic as the XrdCtaFilesystem::FSctl() method.
   * @param client Same semantic as the XrdCtaFilesystem::FSctl() method.
   * @return Same semantic as the XrdCtaFilesystem::FSctl() method.
   */
  int processWrapperMsg(const eos::wfe::Wrapper &msg, XrdOucErrInfo &eInfo, const XrdSecEntity *client);

  /**
   * Processes the specified notification message.
   *
   * @param msg The message.
   * @param eInfo Same semantic as the XrdCtaFilesystem::FSctl() method.
   * @param client Same semantic as the XrdCtaFilesystem::FSctl() method.
   * @return Same semantic as the XrdCtaFilesystem::FSctl() method.
   */
  int processNotificationMsg(const eos::wfe::Notification &msg, XrdOucErrInfo &eInfo, const XrdSecEntity *client);

}; // XrdCtaFilesystem

}}
