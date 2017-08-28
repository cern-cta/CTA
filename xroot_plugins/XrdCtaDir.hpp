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
#include "common/log/SyslogLogger.hpp"

#include "XrdSfs/XrdSfsInterface.hh"

#include <string>
#include <vector>

namespace cta { namespace xrootPlugins {

/**
 * This class is used exclusively by EOS to get the list of EOS id's for a specific instance
 */
class XrdCtaDir : public XrdSfsDirectory {
  
public:
  
  XrdOucErrInfo  error;
  virtual int open(const char *path, const XrdSecEntity *client = 0, const char *opaque = 0);
  virtual const char *nextEntry();
  virtual int close();
  virtual const char *FName();
  XrdCtaDir(cta::catalogue::Catalogue *catalogue, cta::log::Logger *log, const char *user=0, int MonID=0);
  virtual ~XrdCtaDir();
  
protected:

  /**
   * The catalogue object pointer.
   */
  cta::catalogue::Catalogue *m_catalogue;
  
  /**
   * The scheduler object pointer
   */
  cta::log::Logger &m_log;
  
  /**
   * The archive file iterator used loop over catalogue results
   */
  catalogue::ArchiveFileItor m_itor;
  
  /**
   * The client identity info: username and host
   */
  cta::common::dataStructures::SecurityIdentity m_cliIdentity;
  
  /**
   * Checks whether client has correct permissions and fills the corresponding SecurityIdentity structure
   * 
   * @param client  The client security entity
   */
  void checkClient(const XrdSecEntity *client);
  
};

}}
