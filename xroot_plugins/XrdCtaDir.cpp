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

#include <iostream>

#include "XrdSec/XrdSecEntity.hh"

#include "catalogue/TapeFileSearchCriteria.hpp"
#include "xroot_plugins/XrdCtaDir.hpp"

namespace cta { namespace xrootPlugins {

//------------------------------------------------------------------------------
// checkClient
//------------------------------------------------------------------------------
void XrdCtaDir::checkClient(const XrdSecEntity *client) {
  if(client==nullptr || client->name==nullptr || client->host==nullptr) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+": [ERROR] XrdSecEntity from xroot contains invalid information (nullptr pointer detected!)");
  }
  std::cout << "DIR Request received from client. Username: " << client->name << " Host: " << client->host << std::endl;
  m_cliIdentity.username=client->name;
  m_cliIdentity.host=client->host;
}

//------------------------------------------------------------------------------
// open
//------------------------------------------------------------------------------
int XrdCtaDir::open(const char *path, const XrdSecEntity *client, const char *opaque) {
  try {
    checkClient(client);
    if(!path || strlen(path)<1){
      throw cta::exception::Exception(std::string(__FUNCTION__)+": [ERROR] invalid virtual directory path");
    }
    cta::catalogue::TapeFileSearchCriteria searchCriteria;    
    searchCriteria.diskInstance = path; //the path will be the disk instance that we want the disk id's of
    m_itor = m_catalogue->getArchiveFileItor(searchCriteria);
    std::list<cta::log::Param> params;
    params.push_back(cta::log::Param("USERNAME", m_cliIdentity.username));
    params.push_back(cta::log::Param("HOST", m_cliIdentity.host));
    params.push_back(cta::log::Param("DIRPATH", path));
    m_log(log::INFO, "Successful Request", params);
    return SFS_OK;
  } catch (cta::exception::Exception &ex) {
    std::list<cta::log::Param> params;
    params.push_back(cta::log::Param("USERNAME", m_cliIdentity.username));
    params.push_back(cta::log::Param("HOST", m_cliIdentity.host));
    if(path && strlen(path)) {
      params.push_back(cta::log::Param("DIRPATH", path));
    }
    params.push_back(cta::log::Param("ERROR",  ex.getMessageValue()));
    m_log(log::ERR, "Unsuccessful Request", params);
    error.setErrInfo(ENOTSUP, ex.getMessageValue().c_str());    
    return SFS_ERROR;
  } catch (std::exception &ex) {
    std::list<cta::log::Param> params;
    params.push_back(cta::log::Param("USERNAME", m_cliIdentity.username));
    params.push_back(cta::log::Param("HOST", m_cliIdentity.host));
    if(path && strlen(path)) {
      params.push_back(cta::log::Param("DIRPATH", path));
    }
    params.push_back(cta::log::Param("ERROR",  ex.what()));
    m_log(log::ERR, "Unsuccessful Request", params);
    error.setErrInfo(ENOTSUP, ex.what());    
    return SFS_ERROR;
  } catch (...) {
    std::list<cta::log::Param> params;
    params.push_back(cta::log::Param("USERNAME", m_cliIdentity.username));
    params.push_back(cta::log::Param("HOST", m_cliIdentity.host));
    if(path && strlen(path)) {
      params.push_back(cta::log::Param("DIRPATH", path));
    }
    params.push_back(cta::log::Param("ERROR",  "Unknown exception caught!"));
    m_log(log::ERR, "Unsuccessful Request", params);
    error.setErrInfo(ENOTSUP, "Unknown exception caught!");    
    return SFS_ERROR;
  }
}

//------------------------------------------------------------------------------
// nextEntry
//------------------------------------------------------------------------------
const char* XrdCtaDir::nextEntry() {
  if(!(m_itor.hasMore())) {
    return nullptr;
  }
  return m_itor.next().diskFileId.c_str();
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
int XrdCtaDir::close() {
  return SFS_OK;
}

//------------------------------------------------------------------------------
// FName
//------------------------------------------------------------------------------
const char* XrdCtaDir::FName() {
  error.setErrInfo(ENOTSUP, "Not supported.");
  return nullptr;
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
XrdCtaDir::XrdCtaDir(cta::catalogue::Catalogue *catalogue, cta::log::Logger *log, const char *user, int MonID) : error(user, MonID), m_catalogue(catalogue), m_log(*log) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
XrdCtaDir::~XrdCtaDir() {
}  
  
}}
