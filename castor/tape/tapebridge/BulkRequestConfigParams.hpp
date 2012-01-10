/******************************************************************************
 *                castor/tape/tapebridge/BulkRequestConfigParams.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_BULKREQUESTCONFIGPARAMS_HPP
#define CASTOR_TAPE_TAPEBRIDGE_BULKREQUESTCONFIGPARAMS_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/tape/tapebridge/ConfigParamAndSource.hpp"
#include "castor/tape/tapebridge/ConfigParams.hpp"

#include <stdint.h>


namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Class used to determine and store the configuration parameters used by the
 * tapebridged daemon to request in bulk files for migration and recall.
 */
class BulkRequestConfigParams: public ConfigParams {
public:

  /**
   * Constructor.
   */
  BulkRequestConfigParams();

  /**
   * Determines the values of the tape-bridge configuration parameters by
   * searching the environment variables, followed by castor.conf file and then
   * by using the compile-time defaults.
   */
  void determineConfigParams() throw(castor::exception::Exception);

  /**
   * Returns the bulkRequestMigrationMaxBytes configuration parameter.
   */
  const ConfigParamAndSource<uint64_t> &getBulkRequestMigrationMaxBytes() const;

  /**
   * Returns the bulkRequestMigrationMaxFiles configuration parameter.
   */
  const ConfigParamAndSource<uint64_t> &getBulkRequestMigrationMaxFiles() const;

  /**
   * Returns the bulkRequestRecallMaxBytes configuration parameter.
   */
  const ConfigParamAndSource<uint64_t> &getBulkRequestRecallMaxBytes() const;

  /**
   * Returns the bulkRequestRecallMaxFiles configuration parameter.
   */
  const ConfigParamAndSource<uint64_t> &getBulkRequestRecallMaxFiles() const;

protected:

  /**
   * Determines the value of the bulkRequestMigrationMaxBytes configuration
   * parameter.
   *
   * All users of this class accept for unit testers should not call this
   * method, but instead call determineConfigParams().
   *
   * This method determines the required value by first reading the
   * environment variables, then if unsuccessful by reading castor.conf and
   * finally if still unsuccessfull by using the compile-time default.
   */
  void determineBulkRequestMigrationMaxBytes()
    throw(castor::exception::Exception);

  /**
   * Determines the value of the bulkRequestMigrationMaxFiles configuration
   * parameter.
   *
   * All users of this class accept for unit testers should not call this
   * method, but instead call determineConfigParams().
   *
   * This method determines the required value by first reading the
   * environment variables, then if unsuccessful by reading castor.conf and
   * finally if still unsuccessfull by using the compile-time default.
   */
  void determineBulkRequestMigrationMaxFiles()
    throw(castor::exception::Exception);

  /**
   * Determines the value of the bulkRequestRecallMaxBytes configuration
   * parameter.
   *
   * All users of this class accept for unit testers should not call this
   * method, but instead call determineConfigParams().
   *
   * This method determines the required value by first reading the
   * environment variables, then if unsuccessful by reading castor.conf and
   * finally if still unsuccessfull by using the compile-time default.
   */
  void determineBulkRequestRecallMaxBytes()
    throw(castor::exception::Exception);

  /**
   * Determines the value of the bulkRequestRecallMaxFiles configuration
   * parameter.
   *
   * All users of this class accept for unit testers should not call this
   * method, but instead call determineConfigParams().
   *
   * This method determines the required value by first reading the
   * environment variables, then if unsuccessful by reading castor.conf and
   * finally if still unsuccessfull by using the compile-time default.
   */
  void determineBulkRequestRecallMaxFiles()
    throw(castor::exception::Exception);

private:

  /**
   * When the tapegatewayd daemon is asked for a set of files to migrate to
   * tape, this configuration parameter specifies the maximum number of bytes
   * the resulting set can represent.  This number may be exceeded when the set
   * contains a single file.
   */
  ConfigParamAndSource<uint64_t> m_bulkRequestMigrationMaxBytes;

  /**
   * When the tapegatewayd daemon is asked for a set of files to migrate to
   * tape, this configuration parameter specifies the maximum number of files
   * that can be in that set.
   */
  ConfigParamAndSource<uint64_t> m_bulkRequestMigrationMaxFiles;

  /**
   * When the tapegatewayd daemon is asked for a set of files to recall from
   * tape, this configuration parameter specifies the maximum number of bytes
   * the resulting set can represent.  This number may be exceeded when the set
   * contains a single file.
   */
  ConfigParamAndSource<uint64_t> m_bulkRequestRecallMaxBytes;

  /**
   * When the tapegatewayd daemon is asked for a set of files to recall from
   * tape, this configuration parameter specifies the maximum number of files
   * that can be in that set.
   */
  ConfigParamAndSource<uint64_t> m_bulkRequestRecallMaxFiles;

}; // class BulkRequestConfigParams

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_BULKREQUESTCONFIGPARAMS_HPP
