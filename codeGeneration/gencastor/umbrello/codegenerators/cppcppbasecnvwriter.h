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
    
  /// writes bulkCreateRep method
  void writeBulkCreateRep();
    
  /// writes updateRep method
  void writeUpdateRep();
    
  /// writes deleteRep method
  void writeDeleteRep();
    
  /// writes createObj method
  void writeCreateObj();

  /// writes bulkCreateObj method
  void writeBulkCreateObj();
    
  /// writes updateObj method
  void writeUpdateObj();
    
  /// writes createRep method's content
  virtual void writeCreateRepContent(QTextStream &stream, bool &addressUsed,
                                     bool &endTransUsed, bool &typeUsed) = 0;
    
  /// writes bulkCreateRep method's content
  virtual void writeBulkCreateRepContent(QTextStream &stream, bool &addressUsed,
                                         bool &typeUsed) = 0;
    
  /// writes updateRep method's content
  virtual void writeUpdateRepContent(QTextStream &stream,
                                     bool &addressUsed) = 0;
    
  /// writes deleteRep method's content
  virtual void writeDeleteRepContent(QTextStream &stream,
                                     bool &addressUsed) = 0;
    
  /// writes createObj method's content
  virtual void writeCreateObjContent() = 0;

  /// writes bulkCreateObj method's content
  virtual void writeBulkCreateObjContent() = 0;
    
  /// writes updateObj method's content
  virtual void writeUpdateObjContent() = 0;
    
  /// writes deleteRep method's content
 protected:
  QString m_originalPackage;

  /// A prefix for the class names of the converters
  QString m_prefix;

};

#endif // CODEGENERATORS_CPPCPPBASECNVWRITER_H
