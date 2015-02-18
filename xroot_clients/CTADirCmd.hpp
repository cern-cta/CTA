#pragma once

#include <exception>
#include <istream>
#include <ostream>
#include <string>

/**
 * Class implementing the business logic of the archive command-line tool.
 */
class CTADirCmd {
public:
  /**
   * Constructor.
   */
  CTADirCmd() throw();

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

}; // class CTADirCmd
