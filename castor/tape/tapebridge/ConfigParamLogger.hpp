/******************************************************************************
 *                      castor/tape/tapebridge/ConfigParamLogger.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 * 
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_TAPEBRIDGE_CONFIGPARAMLOGGER
#define CASTOR_TAPE_TAPEBRIDGE_CONFIGPARAMLOGGER

#include "castor/dlf/Dlf.hpp"
#include "castor/tape/tapebridge/DlfMessageConstants.hpp"

namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Helper class that facilitates the logging of ConfigParamAndSource objects to
 * DLF.
 */
class ConfigParamLogger {
public:

  /**
   * Logs the specified ConfigParamAndSource object.
   */
  template<typename ConfigParamAndSourceType>
    static void writeToDlf(
      const Cuuid_t            &cuuid,
      ConfigParamAndSourceType &configParamAndSource) {
    writeToDlf(
      cuuid,
      configParamAndSource.category,
      configParamAndSource.name,
      configParamAndSource.value,
      configParamAndSource.source);
  }

  /**
   * Logs the specified components of a ConfigParamAndSource object.
   */
  template<typename ValueType>
    static void writeToDlf(
      const Cuuid_t     &cuuid,
      const std::string &category,
      const std::string &name,
      ValueType         &value,
      const std::string &source) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("category", category),
      castor::dlf::Param("name"    , name    ),
      castor::dlf::Param("value"   , value   ),
      castor::dlf::Param("source"  , source  )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_CONFIG_PARAM, params);
  }

}; // class ConfigParamLogger

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_CONFIGPARAMLOGGER
