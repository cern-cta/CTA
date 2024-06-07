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

#include "common/exception/Exception.hpp"
#include "rdbms/wrapper/OcciConnFactory.hpp"
#include "rdbms/wrapper/OcciEnvSingleton.hpp"
#include "plugin-manager/PluginInterface.hpp"

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciConnFactory::OcciConnFactory(
  const std::string &username,
  const std::string &password,
  const std::string &database):
  m_username(username),
  m_password(password),
  m_database(database) {
}

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
std::unique_ptr<ConnWrapper> OcciConnFactory::create() {
  try {
    return OcciEnvSingleton::instance().createConn(m_username, m_password, m_database);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace cta::rdbms::wrapper

extern "C" {

void factory(cta::plugin::Interface<cta::rdbms::wrapper::ConnFactory,
    cta::plugin::Args<const std::string&>,
    cta::plugin::Args<const std::string&, const std::string&, const std::string&>>& interface) {

  interface.SET<cta::plugin::DATA::PLUGIN_NAME>("ctardbmsocci")
    .SET<cta::plugin::DATA::API_VERSION>(VERSION_API)
    .CLASS<cta::rdbms::wrapper::OcciConnFactory>("OcciConnFactory");
}

}// extern "C"
