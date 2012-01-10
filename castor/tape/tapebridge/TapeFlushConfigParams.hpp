/******************************************************************************
 *                castor/tape/tapebridge/TapeFlushConfigParams.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_TAPEFLUSHCONFIGPARAMS_HPP
#define CASTOR_TAPE_TAPEBRIDGE_TAPEFLUSHCONFIGPARAMS_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/tape/tapebridge/ConfigParamAndSource.hpp"
#include "castor/tape/tapebridge/ConfigParams.hpp"

#include <stdint.h>


namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Class used to determine and store the configuration parameters used by the
 * tapebridged daemon to configure the mode of flush behaviour when writing to
 * tape.
 */
class TapeFlushConfigParams: public ConfigParams {
public:

  /**
   * Constructor.
   */
  TapeFlushConfigParams();

  /**
   * Determines the values of the tape-bridge configuration parameters by
   * searching the environment variables, followed by castor.conf file and then
   * by using the compile-time defaults.
   *
   * If the tape flush mode does not require the maximum number of bytes and
   * files before a tape flush, then the value of these configuration
   * parameters are given the value of 0 and their source set to "UNKNOWN".
   */
  void determineConfigParams() throw(castor::exception::Exception);

  /**
   * Returns the tapeFlushMode configuration parameter.
   */
  const ConfigParamAndSource<uint32_t> &getTapeFlushMode() const;

  /**
   * Returns the maxBytesBeforeFlush configuration parameter.
   */
  const ConfigParamAndSource<uint64_t> &getMaxBytesBeforeFlush() const;

  /**
   * Returns the maxFilesBeforeFlush configuration parameter.
   */
  const ConfigParamAndSource<uint64_t> &getMaxFilesBeforeFlush() const;

protected:

  /**
   * Determines the mode of the flush behaviour to be used when writing data to
   * tape.
   *
   * All users of this class accept for unit testers should not call this
   * method, but instead call determineConfigParams().
   *
   * This method determines the required value by first reading the
   * environment variables, then if unsuccessful by reading castor.conf and
   * finally if still unsuccessfull by using the compile-time default.
   */
  void determineTapeFlushMode() throw(castor::exception::Exception);

  /**
   * With respect to using buffered tape-marks over multiple files, this
   * method determines the maximum number of bytes to be written to tape before
   * a flush to tape.  Please note that the flushes occur at file boaundaries
   * so most of the time more data will be written to tape before the actual
   * flush occurs.
   *
   * All users of this class accept for unit testers should not call this
   * method, but instead call determineConfigParams().
   *
   * This method determines the required value by first reading the
   * environment variables, then if unsuccessful by reading castor.conf and
   * finally if still unsuccessfull by using the compile-time default.
   */
  void determineMaxBytesBeforeFlush() throw(castor::exception::Exception);

  /**
   * With respect to using buffered tape-marks over multiple files, this
   * method determines the maximum number of files to be written to tape before
   * a flush to tape.
   *
   * All users of this class accept for unit testers should not call this
   * method, but instead call determineConfigParams().
   *
   * This method determines the required value by first reading the
   * environment variables, then if unsuccessful by reading castor.conf and
   * finally if still unsuccessfull by using the compile-time default.
   */
  void determineMaxFilesBeforeFlush() throw(castor::exception::Exception);

private:

  /**
   * The mode of tape flush behaviour to be used when writing totape.
   */
  ConfigParamAndSource<uint32_t> m_tapeFlushMode;

  /**
   * If the mode of tape flush tape behaviour to flush every Nth file,
   * then this parameter specifies the Nth file in terms of the maximum number
   * of bytes to be written to tape before a flush.
   */
  ConfigParamAndSource<uint64_t> m_maxBytesBeforeFlush;

  /**
   * If the mode of tape flush tape behaviour to flush every Nth file,
   * then this parameter specifies the Nth file in terms of the maximum number
   * of files to be written to tape before a flush.
   */
  ConfigParamAndSource<uint64_t> m_maxFilesBeforeFlush;

}; // class TapeFlushConfigParams

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_TAPEFLUSHCONFIGPARAMS_HPP
