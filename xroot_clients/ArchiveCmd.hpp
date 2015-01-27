#pragma once

#include "xroot_utils/ParsedArchiveCmdLine.hpp"

#include <exception>
#include <istream>
#include <ostream>
#include <string>

struct wrongArgumentsException : public std::exception
{
  const char * what() const throw()
  {
    return "Wrong arguments supplied";
  }
};

/**
 * Class implementing the business logic of the archive command-line tool.
 */
class ArchiveCmd {
public:
  /**
   * Constructor.
   */
  ArchiveCmd() throw();

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
   * The command line structure
   */
  ParsedArchiveCmdLine m_cmdLine;

  /**
   * Parses the specified command-line arguments.
   *
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   */
  void parseCommandLine(const int argc, char **argv);

  /**
   * Writes the command-line usage message of to the specified output stream.
   *
   * @param os Output stream to be written to.
   */
  void usage(std::ostream &os) const throw();
  
  /**
   * Sends the archive request and waits for the reply
   * 
   * @return the return code
   */
  int executeCommand() ;

}; // class LabelCmd
