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
#include <string>
#include <map>
#include <type_traits>
#include <limits>
#include "DriveConfiguration.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace tape {
namespace daemon {
/**
 * Class containing all the parameters needed by the watchdog process
 * to spawn a transfer session per drive.
 */
struct GlobalConfiguration {
  static GlobalConfiguration createFromCtaConf(
          cta::log::Logger &log = gDummyLogger);
  static GlobalConfiguration createFromCtaConf(
          const std::string & generalConfigPath,
          cta::log::Logger & log = gDummyLogger);
  // Default constructor.
  GlobalConfiguration();
  std::map<std::string, DriveConfiguration> driveConfigs;

  
  /**
   * A templated class allowing the tracking of parameter with their source.
   * If the parameter is not set (implicitly defined as the source being
   * an empty string), access to the value will be denied (exception)
   */
  template<class C>
  class SourcedParameter {
  public:
    CTA_GENERATE_EXCEPTION_CLASS(ParameterNotDefined);
    SourcedParameter(const std::string & name): m_name(name) {
      if (std::is_arithmetic<C>::value) {
        m_value=std::numeric_limits<C>::max();
      }
    }
    SourcedParameter(const std::string & name, C value, const std::string & source):
      m_name(name), m_value(value), m_source(source) {}
    C operator() () {
      if (m_source.empty()) {
        throw ParameterNotDefined(std::string("In SourcedParameter::operator(): "
                "value not defined for parameter \'" + m_name + "\' :"));
      }
      return m_value;
    }
    void set(const std::string & value, const std::string & source) {
      m_value = value;
      m_source = source;
    }
    const std::string & name() { return m_name; }
    const std::string & source() { return m_source; }
  private:
    std::string m_name;
    C m_value;
    std::string m_source;
  };
  
  // The actual parameters:
  SourcedParameter<std::string> tpConfigPath;
private:
  /** A private dummy logger which will simplify the implementaion of the 
   * functions (just unconditionally log things). */
  static cta::log::DummyLogger gDummyLogger;
} ;
}
}
} // namespace cta::tape::daemon