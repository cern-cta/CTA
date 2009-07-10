/******************************************************************************
 *                 castor/tape/tpcp/StreamHelper.hpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_TPCP_STREAMHELPER_HPP
#define CASTOR_TAPE_TPCP_STREAMHELPER_HPP 1

#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/FileMigratedNotification.hpp"
#include "castor/tape/tapegateway/FileRecalledNotification.hpp"
#include "castor/tape/tapegateway/FileToMigrate.hpp"
#include "castor/tape/tapegateway/FileToMigrateRequest.hpp"
#include "castor/tape/tapegateway/FileToRecall.hpp"
#include "castor/tape/tapegateway/FileToRecallRequest.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"

#include <ostream>


namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * Class full of static methods to faciliate working with streams.
 *
 * Note that instances of this class cannot be created.
 */
class StreamHelper {

private:

  /**
   * Private constructor to prevent instances of this class from being
   * created.
   */
  StreamHelper();


public:

  /**
   * Writes a textual representation of the specified value to the specified
   * stream.
   *
   * @param The output stream to be written to.
   * @param The value of which the textual representation is to be written.
   */
  static void write(std::ostream &os,
    const castor::tape::tapegateway::BaseFileInfo &value) throw();

  /**
   * Writes a textual representation of the specified value to the specified
   * stream.
   *
   * @param The output stream to be written to.
   * @param The value of which the textual representation is to be written.
   */
  static void write(std::ostream &os,
    const castor::tape::tapegateway::FileToMigrate &value) throw();

  /**
   * Writes a textual representation of the specified value to the specified
   * stream.
   *
   * @param The output stream to be written to.
   * @param The value of which the textual representation is to be written.
   */
  static void write(std::ostream &os,
    const castor::tape::tapegateway::FileToRecall &value) throw();

  /**
   * Writes a textual representation of the specified value to the specified
   * stream.
   *
   * @param The output stream to be written to.
   * @param The value of which the textual representation is to be written.
   */
  static void write(std::ostream &os,
    const castor::tape::tapegateway::NoMoreFiles &value) throw();

  /**
   * Writes a textual representation of the specified value to the specified
   * stream.
   *
   * @param The output stream to be written to.
   * @param The value of which the textual representation is to be written.
   */
  static void write(std::ostream &os,
    const castor::tape::tapegateway::FileToMigrateRequest &value) throw();

  /**
   * Writes a textual representation of the specified value to the specified
   * stream.
   *
   * @param The output stream to be written to.
   * @param The value of which the textual representation is to be written.
   */
  static void write(std::ostream &os,
    const castor::tape::tapegateway::FileToRecallRequest &value) throw();

  /**
   * Writes a textual representation of the specified value to the specified
   * stream.
   *
   * @param The output stream to be written to.
   * @param The value of which the textual representation is to be written.
   */
  static void write(std::ostream &os,
    const castor::tape::tapegateway::FileMigratedNotification &value) throw();

  /**
   * Writes a textual representation of the specified value to the specified
   * stream.
   *
   * @param The output stream to be written to.
   * @param The value of which the textual representation is to be written.
   */
  static void write(std::ostream &os,
    const castor::tape::tapegateway::FileRecalledNotification &value) throw();

  /**
   * Writes a textual representation of the specified value to the specified
   * stream.
   *
   * @param The output stream to be written to.
   * @param The value of which the textual representation is to be written.
   */
  static void write(std::ostream &os,
    const castor::tape::tapegateway::NotificationAcknowledge &value) throw();

  /**
   * Writes a textual representation of the specified value to the specified
   * stream.
   *
   * @param The output stream to be written to.
   * @param The value of which the textual representation is to be written.
   */
  static void write(std::ostream &os,
    const castor::tape::tapegateway::EndNotification &value) throw();

  /**
   * Writes a textual representation of the specified value to the specified
   * stream.
   *
   * @param The output stream to be written to.
   * @param The value of which the textual representation is to be written.
   */
  static void write(std::ostream &os,
    const castor::tape::tapegateway::EndNotificationErrorReport &value) throw();

}; // class StreamHelper

} // namespace tpcp
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TPCP_STREAMHELPER_HPP
