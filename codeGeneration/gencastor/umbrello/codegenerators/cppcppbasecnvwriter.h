#ifndef CODEGENERATORS_CPPCPPBASECNVWRITER_H 
#define CODEGENERATORS_CPPCPPBASECNVWRITER_H 1

// Include files

// Local includes
#include "cppcppwriter.h"

class CppCppBaseCnvWriter : public CppCppWriter {

 public:
  /**
   * Constructor
   */
  CppCppBaseCnvWriter(UMLDoc *parent, const char *name);

  /**
   * Destructor
   */
  ~CppCppBaseCnvWriter() {};

 protected:
  /**
   * Extracts the blobs and their lengths from the member list
   */
  QMap<QString,QString> extractBlobsFromMembers
    (MemberList& members);

  /// get the class prefix
  QString& prefix() { return m_prefix; }

  /// set the class prefix
  void setPrefix(QString p) { m_prefix = p; }

  /// writes static factory declaration
  void writeFactory();

  /// writes objtype methods
  void writeObjType();

  /// writes createRep method
  void writeCreateRep();
    
  /// writes updateRep method
  void writeUpdateRep();
    
  /// writes deleteRep method
  void writeDeleteRep();
    
  /// writes createObj method
  void writeCreateObj();

  /// writes updateObj method
  void writeUpdateObj();
    
  /// writes createRep method's content
  virtual void writeCreateRepContent() = 0;
    
  /// writes createRep method's content
  virtual void writeUpdateRepContent() = 0;
    
  /// writes deleteRep method's content
  virtual void writeDeleteRepContent() = 0;
    
  /// writes createObj method's content
  virtual void writeCreateObjContent() = 0;

  /// writes createRep method's content
  virtual void writeUpdateObjContent() = 0;
    
  /// writes deleteRep method's content
 protected:
  QString m_originalPackage;

  /// A prefix for the class names of the converters
  QString m_prefix;

};

#endif // CODEGENERATORS_CPPCPPBASECNVWRITER_H
