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

#include "common/exception/Exception.hpp"
#include "common/QtCoreAppSingleton.hpp"

#include <iostream>

#include <string.h>
#include <strings.h>

std::mutex cta::common::QtCoreAppSingleton::s_mutex;
cta::common::QtCoreAppSingleton *cta::common::QtCoreAppSingleton::s_instance =
  NULL;

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::QtCoreAppSingleton::QtCoreAppSingleton() {
  m_appProgramName[0] = 'q';
  m_appProgramName[1] = 't';
  m_appProgramName[2] = '\0';
  m_appArgv[0] = m_appProgramName;
  m_appArgc = 1;

  m_app = new QCoreApplication(m_appArgc, m_appArgv);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::QtCoreAppSingleton::~QtCoreAppSingleton() {
  std::cout << "Deleting Qt core application object" << std::endl;
  delete m_app;
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool cta::common::QtCoreAppSingleton::operator==(const QtCoreAppSingleton &rhs)
  const {
  return m_app == m_app;
}

//------------------------------------------------------------------------------
// getApp
//------------------------------------------------------------------------------
QCoreApplication &cta::common::QtCoreAppSingleton::getApp() const {
  if(NULL == m_app) {
    throw exception::Exception("Failed to get QCoreApplication object"
      ": object not allocated");
  }

  return *m_app;
}

//------------------------------------------------------------------------------
// instance
//------------------------------------------------------------------------------
cta::common::QtCoreAppSingleton &cta::common::QtCoreAppSingleton::instance() {
  std::lock_guard<std::mutex> lock(s_mutex);

  if(NULL == s_instance) {
    s_instance = new QtCoreAppSingleton();
  }

  return *s_instance;
}

//------------------------------------------------------------------------------
// destroy
//------------------------------------------------------------------------------
void cta::common::QtCoreAppSingleton::destroy() {
  std::lock_guard<std::mutex> lock(s_mutex);
  delete s_instance;
  s_instance = NULL;
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const cta::common::QtCoreAppSingleton &obj) {
  os << "app=" << std::hex << &obj.getApp() << std::dec;
  return os;
}
