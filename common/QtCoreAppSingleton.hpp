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

#include <mutex>
#include <QtCore/QCoreApplication>
#include <ostream>

namespace cta {
namespace common {

/**
 * Singleton ensure that a QCoreApplication object is only created once.
 */
class QtCoreAppSingleton {
public:

  /**
   * Creates if necessary and returns a pointer to the singleton object.
   */
  static QtCoreAppSingleton &instance();

  /**
   * Deletes the singleton object if there is one.
   *
   * This method should ONLY be called once at the end of the program or even
   * not all.  This method is intended for unit test programs ran with valgrind.
   */
  static void destroy();

  /**
   * Comparison operator.
   *
   * @rhs The object on the right hand side of the operator.
   */
  bool operator==(const QtCoreAppSingleton &rhs) const;

  /**
   * Returns the QCoreApplication object.
   *
   * @return The QCoreApplication object.
   */
  QCoreApplication &getApp() const;

private:

  static std::mutex s_mutex;

  /**
   * The singleton.
   */
  static QtCoreAppSingleton *s_instance;

  /**
   * Singleton constructor must be private.
   */
  QtCoreAppSingleton();

  /**
   * Singleton destructor must be private.
   */
  ~QtCoreAppSingleton();

  /**
   * The number of dummy command-line arguments passed to the QCoreApplication
   * object.
   */
  int m_appArgc;

  /**
   * The dummy name of the program ("qt") that will be passed to the
   * QCoreApplication object.
   */
  char m_appProgramName[3];

  /**
   * A dummy argv array for the QCoreApplication object. A dummy program name
   * followed by the conventional pointer to NULL.
   */
  char *m_appArgv[2];

  /**
   * The QCoreApplication object.
   */
  QCoreApplication *m_app;

}; // QtCoreAppSingleton

} // namespace common
} // namespace castor  

/**
 * Output stream operator for QtCoreAppSingleton.
 *
 * @param os The output stream.
 * @param obj The QtCoreAppSingleton object.
 */
std::ostream &operator<<(std::ostream &os,
  const cta::common::QtCoreAppSingleton &obj);

