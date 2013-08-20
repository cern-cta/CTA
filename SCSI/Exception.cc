// ----------------------------------------------------------------------
// File: SCSI/Exception.hh
// ----------------------------------------------------------------------

/************************************************************************
 * Tape Server                                                          *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "Exception.hh" 

void SCSI::ExceptionLauncher(const SCSI::Structures::LinuxSGIO_t & sgio, std::string context) {
  if (SCSI::Status::GOOD != sgio.status) {
    throw Exception(sgio.status, (SCSI::Structures::senseData_t<255> *)sgio.sbp, context);
  }
}
