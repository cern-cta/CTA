/**
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

#include <exception>
#include <istream>
#include <ostream>
#include <string>

/**
 * Class implementing the business logic of the archive command-line tool.
 */
class CTACmd {
public:
  /**
   * Constructor.
   */
  CTACmd() throw();

  /**
   * The entry function of the command.
   *
   * @param argc The number of command-line arguments.
   * @param argv The command-line arguments.
   */
  int main(const int argc, char **argv) throw();

protected:
   
  /**
   * The name of the program.
   */
  const std::string m_programName;

  /**
   * Writes the command-line usage message of to the specified output stream.
   *
   * @param os Output stream to be written to.
   */
  void usage(std::ostream &os) const throw();
  
  /**
   * Sends the archive request and waits for the reply
   *
   * @param argc The number of command-line arguments.
   * @param argv The command-line arguments. 
   * @return the return code
   */
  int executeCommand(const int argc, char **argv) ;

}; // class LabelCmd
