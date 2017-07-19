# XRootD SSI + Google Protocol Buffers 3

This directory contains generic classes which bind Google Protocol Buffer definitions to the
XRootD SSI transport layer. It contains the following files:

## Client Side

* **XrdSsiPbServiceClientSide.hpp** : Wraps up the Service factory object with Protocol Buffer integration
  and synchronisation between Requests and Responses
* **XrdSsiPbRequest.hpp** : Send Requests and handle Responses

## Server Side

* **XrdSsiPbService.hpp** : Defines Service on server side: bind Request to Request Processor and Execute
* **XrdSsiPbRequestProc.hpp** : Process Request and send Response

## Both Server and Client Side

* **XrdSsiPbAlert.hpp** : Optional Alerts from Service to Client (_e.g._ log messages) 
* **XrdSsiPbException.hpp** : Convert XRootD SSI and Protocol Buffer errors to exceptions
* **XrdSsiPbDebug.hpp** : Protocol Buffer inspection functions (for debugging, not required in production)

