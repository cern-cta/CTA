#ifndef __XRD_SSI_EXCEPTION_H
#define __XRD_SSI_EXCEPTION_H

/*!
 * Class to convert a XRootD error into a std::exception
 */

#include <stdexcept>
#include <XrdSsi/XrdSsiErrInfo.hh>



class XrdSsiPbException : public std::exception
{
public:
   XrdSsiPbException(const std::string &err_msg) : m_err_msg(err_msg)     {}
   XrdSsiPbException(const XrdSsiErrInfo &eInfo) : m_err_msg(eInfo.Get()) {}

   const char* what() const noexcept { return m_err_msg.c_str(); }

private:
   std::string m_err_msg;
};

#endif
