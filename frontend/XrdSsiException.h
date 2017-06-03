#ifndef __XRD_SSI_EXCEPTION_H
#define __XRD_SSI_EXCEPTION_H

// Class to convert a XRootD error into a std::exception
// Perhaps should be part of XRootD?

#include <stdexcept>
#include <XrdSsi/XrdSsiErrInfo.hh>



class XrdSsiException : public std::exception
{
public:
   XrdSsiException(const std::string &err_msg) : error_msg(err_msg)     {}
   XrdSsiException(const XrdSsiErrInfo &eInfo) : error_msg(eInfo.Get()) {}

   const char* what() const noexcept { return error_msg.c_str(); }

private:
   std::string error_msg;
};

#endif
