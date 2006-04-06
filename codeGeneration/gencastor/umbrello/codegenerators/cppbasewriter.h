#ifndef CODEGENERATORS_CPPBASEWRITER_H 
#define CODEGENERATORS_CPPBASEWRITER_H 1

// Includes
#include <set>
#include <map>
#include <qstring.h>
#include <qvaluelist.h>
#include <qtextstream.h>

// Local includes
#include "cppcastorwriter.h"
#include "../classifier.h"
#include "classifierinfo.h"
#include "../umlnamespace.h"
#include "../operation.h"

/**
 * Base cpp writer.
 * Implements functionnalities for including files,
 * namespaces, forward declarations, comments,
 * type checking, indenting and operations declarations
 */
class CppBaseWriter : public CppCastorWriter {

 public:
  /** Constructor */
  CppBaseWriter(UMLDoc *parent,
                const char *name);
  /** Constructor */
  virtual ~CppBaseWriter() {};

  /***********************************/
  /* Initialization and finalization */
  /***********************************/
 public:
  /**
   * Initializes the writer. This means creating the file
   * and writing some header in it
   */
  virtual bool init(UMLClassifier* c, QString fileName);
  
  /**
   * Finalizes the writer. This means writing the footer
   * of the file and closing it
   */
  virtual bool finalize(UMLClassifier* c);
  
  /***********************/
  /* For including files */
  /***********************/ 
 public:
  /**
   * Adds a file to the set of include files
   */
  void addInclude(QString file);

 protected:

  /**
   * writes code for including a whole set of files
   */
  void writeIncludesFromSet(QTextStream &stream,
                            std::set<QString> &set);

  /**
   * writes code for including a single file
   */
  void writeInclude(QTextStream &stream,
                    const QString &file);

  /****************************/
  /* For forward declarations */
  /****************************/
 protected:
  /**
   * writes all forward declarations and leaves
   * the current namespace open
   */
  void writeNSForward(QTextStream &stream,
                      QString localNS);

  /**
   * writes forward declarations for all
   * namespaces in the given set
   */
  void writeForward(QTextStream &stream,
                    std::set<QString> fwds);

  /** writes forward declarations for the current namespace */
  void writeEasyForward(QTextStream &stream,
                        std::set<QString>& fwds);

  /******************/
  /* For Namespaces */
  /******************/
 protected:
  /** opens a namespace */
  void writeNSOpen(QTextStream &stream,
                   QString ns);

  /** closes a namespace */
  void writeNSClose(QTextStream &stream,
                    QString ns);

  /*****************/
  /* For Indenting */
  /*****************/
 public:
  /**
   * Builds an indentation string from the indentation level
   */
  QString getIndent (int l);

  /**
   * How much indent to use (current, based on amount of indentLevel).
   */
  QString getIndent ();

  /**
   * increase indentation
   */
  void incIndent() { m_indent++; }
  
  /**
   * decrease indentation
   */
  void decIndent() { m_indent--; }

  /****************/
  /* For Comments */
  /****************/
 protected:
  /**
   * Writes a doxygen style comment
   */
  void writeComment(QString text,
                    QString indent,
                    QTextStream &cpp);

  /**
   * Writes a header comment, with an extra line of stars
   */
  void writeHeaderComment(QString text,
                          QString indent,
                          QTextStream &cpp);

  /**
   * Writes a wide header comment, with an extra line
   * of "==" and 80 caracters wide
   */
  void writeWideHeaderComment(QString text,
                              QString indent,
                              QTextStream &cpp);

  /**
   * Writes a documentation comment
   */
  void writeDocumentation(QString header,
                          QString body,
                          QString end,
                          QTextStream &cpp);

  /*********************/
  /* For type checking */
  /*********************/

 public:
	/**
	 * fixes type names and updates list of includes
   * and forward declarations to be done.
   * The type fixing adds namespaces when needed
   * and deals with standard library classes
	 */
	QString fixTypeName(QString string,
                      QString typePackage,
                      QString currentPackage,
                      bool forceNamespace = false); 
  
  /**
   * tells whether last call to fixTypeName dealt with
   * a vector or not
   */
  bool isLastTypeVector() { return m_isLastTypeVector; }

  /**
   * tells whether last call to fixTypeName dealt with
   * an array or not
   */
  bool isLastTypeArray() { return m_isLastTypeArray; }

  /**
   * In case last call to fixTypeName dealt with an array,
   * return the array specific part ([5] for example)
   */
  QString arrayPart() { return m_arrayPart; }

  /**
   * In case last call to fixTypeName dealt with an array,
   * return the array length as a string
   */
  QString arrayLength() { 
    QString res = arrayPart();
    int l = res.length();
    res.remove(l-1,1);
    res.remove(0,1);
    return res;
  }

  /**
   * Gets the namespace for a given type
   */
  QString getNamespace(QString type);

  /**
   * Tells whether the given name denotes an enumeration
   */
  bool isEnum(QString name);

  /**
   * Tells whether the given object is an enumeration
   */
  static bool isEnum(UMLObject* obj);

  /******************/
  /* For operations */
  /******************/
 protected:
	/**
	 * write all operations for a given class
	 * @param c the class for which we are generating code
	 * @param i whether or not we are writing this to a source or header file
	 * @param j what type of method to write (by Visibility)
	 * @param k the stream associated with the output file
	 */
	void writeOperations
    (UMLClassifier *c,
     bool isHeaderMethod,
     Uml::Visibility permitVisibility,
     QTextStream &j,
     QValueList<std::pair<QString, int> >& alreadyGenerated);

	/**
	 * write an operation for a given class.
   * This is an abstract method that should be implemented
   * when needed.
	 * @param op the operation you want to write
	 * @param isHeaderMethod whether or not we are writing this
   * to a source or header file
	 * @param isInterfaceImpl whether or not we are implementing
   * an interface we inherit from
	 * @param j the stream associated with the output file
	 */
	void writeOperations
    (UMLOperation *op,
     bool isHeaderMethod,
     bool isInterfaceImpl,
     QTextStream &j,
     QValueList<std::pair<QString, int> >& alreadyGenerated,
     bool* firstOp,
     QString comment);

  /***************/
  /* For scoping */
  /***************/
 protected:
  /**
	 * a little method for converting scope to string value
	 */
	QString scopeToCPPDecl(Uml::Visibility scope);

  /********************************/
  /* For Members and associations */
  /********************************/
 protected:
  /**
   * A Type describing a member.
   */
  struct Member {
    QString name;
    QString typeName;
    bool abstract;
    QString stereotype;
    Member(QString n, QString tn,
           bool a = false, QString s = "") :
      name(n), typeName(tn), abstract(a), stereotype(s) {}
  };

  /**
   * A Type describing a list of members
   */
  typedef QPtrList<Member> MemberList;

  /**
   * the different possible multiplicities for a relation
   */
  typedef enum _Multiplicity {
    MULT_ONE, MULT_N, MULT_UNKNOWN
  } Multiplicity;
  
  /**
   * the different kinds of relation
   */
  typedef enum _AssocKind {
    ASSOC_UNKNOWN, ASSOC, AGGREG_CHILD, AGGREG_PARENT,
    COMPOS_CHILD, COMPOS_PARENT, IMPL_CHILD, IMPL_PARENT,
    UNI_CHILD, UNI_PARENT
  } AssocKind;
  
  /**
   * A type describing the type of an association.
   * This includes multiplicities and kind of association
   */
  struct AssocType {
    Multiplicity multiRemote;
    Multiplicity multiLocal;
    AssocKind kind;
    AssocType(Multiplicity mr, Multiplicity ml, AssocKind k) :
      multiRemote(mr), multiLocal(ml), kind(k) {}
  };
  
  /**
   * A Type describing an associations
   */
  struct Assoc {
    AssocType type;
    Member remotePart;
    Member localPart;
    Assoc(AssocType t, Member rp, Member lp) :
      type(t), remotePart(rp), localPart(lp){}
  };

  /**
   * A Type describing a list of associations
   */
  typedef QPtrList<Assoc> AssocList;

  /**
   * Creates a list of pairs describing the associations
   * of the current object. Each pair contains the name
   * of the member corresponding to an association and
   * its type.
   */
  AssocList createAssocsList();

  /**
   * Creates a list of pairs describing the members
   * of the current object. Each pair contains the name
   * of the member and its type.
   */
  MemberList createMembersList();

  /**
   * Extract the name and type of a member corresponding
   * to an association and adds it to the given list.
   * Skip the associations with multiplicity not equal to 1
   */
  void singleAssocToPairList(UMLAssociation *a,
                             AssocList &list,
                             ClassifierInfo &ci);
  
 public:
  /**
   * Parse a multiplicity definition given as a string
   */
  Multiplicity parseMulti(QString multi);

  /**
   * Parse a multiplicity definition given as a string
   */
  AssocKind parseAssocKind(Uml::Association_Type t,
                           bool isParent);

  /**********/
  /* Useful */
  /**********/
 protected:
  /** convenient method for capitalizing names */
  static QString capitalizeFirstLetter(QString string) {
    string.replace( 0, 1, string.at(0).upper());
    return string;
  }

 private:
  /**
   * check whether it's the first file inclusion
   * and issues a title if it's the case
   */
  void checkFirst(QTextStream &stream);

 public:
  /** access to the class info */
  ClassifierInfo* classInfo() { return m_classInfo; }

 protected:
  /** The class we are dealing with in this file */
  UMLClassifier* m_class;
  
  /** Information about the class we are dealing with */
  ClassifierInfo* m_classInfo;
  
  /** The file handler */
  QFile m_file;

  /** Second stream to write into the buffer */
  QTextStream* m_stream;

  /** The stream to write into the file */
  QTextStream* m_mainStream;

  /**
   * A Buffer where to put temporarily the code
   * that comes after the include statements
   */
  QString m_buffer;

  /**
   * used internally to store array extensions
   */
  QString m_arrayPart;
  
  /**
   * tells whether last call to fixTypeName dealt with
   * an array or not
   */
  bool m_isLastTypeArray;

  /**
   * tells whether last call to fixTypeName dealt with
   * a vector or not
   */
  bool m_isLastTypeVector;

  /** whether it's the first include */
  bool m_firstInclude;

  /** The current indentation level */
  int m_indent;

  /** The indentation basic step */
  static const int INDENT = 2;

  /**
   * whether we allow forward declarations instead
   * of includes when possible. Default is false.
   */
  bool m_allowForward;

	/**
	 * Map of types in the std:: namespace with the associated
   * include files
	 */
  std::map<QString, QString> m_stdMembers;

	/**
	 * List of includes
	 */
  std::set<QString> m_includes;

	/**
	 * List of forward declarations
	 */
  std::set<QString> m_forward;

  /**
   * Map of methods implementing methods. This is an easy
   * way to provide an implementation for a given method.
   * Just declare a method generating the code and add it
   * to this map, with key the name of the method and its 
   * number of arguments
   */
  std::map<std::pair<QString, int>, void(*)(CppBaseWriter*, QTextStream&)>
    m_methodImplementations;

  /**
   * Map of methods called when declaring methods.
   * This is an easy way to trigger some action when
   * declaring a method.
   * Just declare a method triggering the actiond and add
   * it to this map, with key the name of the method
   */
  std::map<QString, void(*)(CppBaseWriter*, QTextStream&)>
    m_methodTriggers;

 public:
  /**
   * Map of methods implementing print method for castor
   * types.
   */
  std::map<QString, void(*)(CppBaseWriter*, QTextStream&)>
    m_castorPrintImplementations;

  /**
   * All virtual methods that should be reimplemented in all
   * non abtract classes
   */
  std::set<QString> m_virtualOpReImpl;

};


#endif // CODEGENERATORS_CPPBASEWRITER_H
