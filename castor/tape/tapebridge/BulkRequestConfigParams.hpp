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
#include "castor/tape/tapebridge/ConfigParam.hpp"
#include "castor/tape/tapebridge/ConfigParams.hpp"

#include <stdint.h>


namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Class used to determine the configuration parameters used by the tapebridged
 * daemon to know how many files should be bulk requested per request for
 * migration to or recall from tape.
 */
class BulkRequestConfigParams: public ConfigParams {
public:

  /**
   * Default constructor.
   */
  BulkRequestConfigParams();

  /**
   * Determines the values of the tape-bridge configuration parameters by
   * searching the environment variables, followed by castor.conf file and then
   * by using the compile-time defaults.
   *
   * This method throws a castor::exception::InvalidArgument exception if the
   * source-value of one the configuration parameters for which this object
   * is responsible is invalid.
   */
  void determineConfigParams()
    throw(castor::exception::InvalidArgument, castor::exception::Exception);

  /**
   * Returns the bulkRequestMigrationMaxBytes configuration parameter.
   */
  const ConfigParam<uint64_t> &getBulkRequestMigrationMaxBytes() const;

  /**
   * Returns the bulkRequestMigrationMaxFiles configuration parameter.
   */
  const ConfigParam<uint64_t> &getBulkRequestMigrationMaxFiles() const;

  /**
   * Returns the bulkRequestRecallMaxBytes configuration parameter.
   */
  const ConfigParam<uint64_t> &getBulkRequestRecallMaxBytes() const;

  /**
   * Returns the bulkRequestRecallMaxFiles configuration parameter.
   */
  const ConfigParam<uint64_t> &getBulkRequestRecallMaxFiles() const;

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
    throw(castor::exception::InvalidArgument, castor::exception::Exception);

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
   *
   * This method throws a castor::exception::InvalidArgument exception if the
   * source-value of the configuration parameter is invalid.
   */
  void determineBulkRequestMigrationMaxFiles()
    throw(castor::exception::InvalidArgument, castor::exception::Exception);

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
   *
   * This method throws a castor::exception::InvalidArgument exception if the
   * source-value of the configuration parameter is invalid.
   */
  void determineBulkRequestRecallMaxBytes()
    throw(castor::exception::InvalidArgument, castor::exception::Exception);

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
   *
   * This method throws a castor::exception::InvalidArgument exception if the
   * source-value of the configuration parameter is invalid.
   */
  void determineBulkRequestRecallMaxFiles()
    throw(castor::exception::InvalidArgument, castor::exception::Exception);

  /**
   * Sets the bulkRequestMigrationMaxBytes configuration parameter.
   *
   * Please note that this method was developed for the sole purpose of
   * facilitating unit-testing. Apart from unit-tests, a
   * configuration-parameter of this class should only be set using the
   * appropriate determineXXXX() method.
   *
   * @param value  The value of the configuration parameter.
   * @param source The source of the configuration parameter.
   */
  void setBulkRequestMigrationMaxBytes(
    const uint64_t                value,
    const ConfigParamSource::Enum source);

  /**
   * Sets the bulkRequestMigrationMaxFiles configuration parameter.
   *
   * Please note that this method was developed for the sole purpose of
   * facilitating unit-testing. Apart from unit-tests, a
   * configuration-parameter of this class should only be set using the
   * appropriate determineXXXX() method.
   *
   * @param value  The value of the configuration parameter.
   * @param source The source of the configuration parameter.
   */
  void setBulkRequestMigrationMaxFiles(
    const uint64_t                value,
    const ConfigParamSource::Enum source);

  /**
   * Sets the bulkRequestRecallMaxBytes configuration parameter.
   *
   * Please note that this method was developed for the sole purpose of
   * facilitating unit-testing. Apart from unit-tests, a
   * configuration-parameter of this class should only be set using the
   * appropriate determineXXXX() method.
   *
   * @param value  The value of the configuration parameter.
   * @param source The source of the configuration parameter.
   */
  void setBulkRequestRecallMaxBytes(
    const uint64_t                value,
    const ConfigParamSource::Enum source);

  /**
   * Sets the bulkRequestRecallMaxFiles configuration parameter.
   *
   * Please note that this method was developed for the sole purpose of
   * facilitating unit-testing. Apart from unit-tests, a
   * configuration-parameter of this class should only be set using the
   * appropriate determineXXXX() method.
   *
   * @param value  The value of the configuration parameter.
   * @param source The source of the configuration parameter.
   */
  void setBulkRequestRecallMaxFiles(
    const uint64_t                value,
    const ConfigParamSource::Enum source);

private:

  /**
   * When the tapegatewayd daemon is asked for a set of files to migrate to
   * tape, this configuration parameter specifies the maximum number of bytes
   * the resulting set can represent.  This number may be exceeded when the set
   * contains a single file.
   */
  ConfigParam<uint64_t> m_bulkRequestMigrationMaxBytes;

  /**
   * When the tapegatewayd daemon is asked for a set of files to migrate to
   * tape, this configuration parameter specifies the maximum number of files
   * that can be in that set.
   */
  ConfigParam<uint64_t> m_bulkRequestMigrationMaxFiles;

  /**
   * When the tapegatewayd daemon is asked for a set of files to recall from
   * tape, this configuration parameter specifies the maximum number of bytes
   * the resulting set can represent.  This number may be exceeded when the set
   * contains a single file.
   */
  ConfigParam<uint64_t> m_bulkRequestRecallMaxBytes;

  /**
   * When the tapegatewayd daemon is asked for a set of files to recall from
   * tape, this configuration parameter specifies the maximum number of files
   * that can be in that set.
   */
  ConfigParam<uint64_t> m_bulkRequestRecallMaxFiles;

}; // class BulkRequestConfigParams

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_BULKREQUESTCONFIGPARAMS_HPP
