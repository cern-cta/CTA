/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
#include "classifier.h"
#include "association.h"
#include "umlassociationlist.h"
#include "operation.h"
#include "attribute.h"
#include "stereotype.h"
#include "clipboard/idchangelog.h"
#include "umldoc.h"
#include "uml.h"
#include <kdebug.h>
#include <klocale.h>

UMLClassifier::UMLClassifier(const QString & name, int id)
  : UMLCanvasObject(name, id)
{
	init();
}

////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifier::~UMLClassifier() {
}

UMLOperation * UMLClassifier::checkOperationSignature( QString name,
																UMLAttributeList *opParams,
																UMLOperation *exemptOp)
{
	UMLObjectList list = findChildObject( Uml::ot_Operation, name );
	if( list.count() == 0 )
		return NULL;

	// there is at least one operation with the same name... compare the parameter list
	for( UMLOperation *test = dynamic_cast<UMLOperation*>(list.first());
	     test != 0;
	     test = dynamic_cast<UMLOperation*>(list.next()) )
	{
		if (test == exemptOp)
			continue;
		UMLAttributeList *testParams = test->getParmList( );
		if (!opParams) {
			if (0 == testParams->count())
				return test;
			continue;
		}
		if( testParams->count() != opParams->count() )
			continue;
		int pCount = testParams->count();
		int i = 0;
		for( ; i < pCount; ++i )
		{
			// The only criterion for equivalence is the parameter types.
			// (Default values are not considered.)
			if( testParams->at(i)->getTypeName() != opParams->at(i)->getTypeName() )
				break;
		}
		if( i == pCount )
		{//all parameters matched -> the signature is not unique
			return test;
		}
	}
	// we did not find an exact match, so the signature is unique ( acceptable )
	return NULL;
}

bool UMLClassifier::addOperation(UMLOperation* op, int position )
{
	if( m_OpsList.findRef( op ) != -1  ||
	    checkOperationSignature(op->getName(), op->getParmList()) )
		return false;

	if( op -> parent() )
		op -> parent() -> removeChild( op );
	this -> insertChild( op );
	if( position >= 0 && position <= (int)m_OpsList.count() )
		m_OpsList.insert(position,op);
	else
		m_OpsList.append( op );
	emit childObjectAdded(op);
	emit operationAdded(op);
	emit modified();
	connect(op,SIGNAL(modified()),this,SIGNAL(modified()));
	return true;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
bool UMLClassifier::addOperation(UMLOperation* Op, IDChangeLog* Log) {
	if( addOperation( Op, -1 ) )
		return true;
	else if( Log ) {
		Log->removeChangeByNewID( Op -> getID() );
	}
	return false;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
int UMLClassifier::removeOperation(UMLOperation *op) {
	if(!m_OpsList.remove(op)) {
		kdDebug() << "can't find opp given in list" << endl;
		return -1;
	}
	// disconnection needed.
	// note that we dont delete the operation, just remove it from the Classifier
	disconnect(op,SIGNAL(modified()),this,SIGNAL(modified()));
	emit childObjectRemoved(op);
	emit operationRemoved(op);
	emit modified();
	return m_OpsList.count();
}
////////////////////////////////////////////////////////////////////////////////////////////////////
bool UMLClassifier::addStereotype(UMLStereotype* newStereotype, UMLObject_Type list, IDChangeLog* log /* = 0*/) {
	QString name = newStereotype->getName();
	if (findChildObject(Uml::ot_Template, name).count() == 0) {
		if(newStereotype->parent())
			newStereotype->parent()->removeChild(newStereotype);
		this->insertChild(newStereotype);
		// What is this?? do we really want to store stereotypes in opsList?!?? -b.t.
#ifdef __GNUC__
#warning "addStereotype method needs review..conflicts with set/getStereoType in umlobject aswell as opList storage issues"
#endif
		if (list == ot_Operation) {
			m_OpsList.append(newStereotype);
			emit modified();
			emit childObjectAdded(newStereotype);
			emit stereotypeAdded(newStereotype);
			connect(newStereotype, SIGNAL(modified()), this, SIGNAL(modified()));
		} else {
			kdWarning() << "unknown list type in addStereotype()" << endl;
		}
		return true;
	} else if (log) {
		log->removeChangeByNewID( newStereotype->getID() );
		delete newStereotype;
	}
	return false;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
int UMLClassifier::removeStereotype(UMLStereotype * /* stype*/) {
#ifdef __GNUC__
#warning "removeStereotype method not implemented yet"
#endif
	kdError() << "can't find stereotype given in list" << endl;
	return -1;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
UMLObjectList UMLClassifier::findChildObject(UMLObject_Type t , QString n) {
  	UMLObjectList list;
 	if (t == ot_Association) {
		return UMLCanvasObject::findChildObject(t, n);
	} else if (t == ot_Operation) {
		UMLClassifierListItem* obj=0;
		for(obj=m_OpsList.first();obj != 0;obj=m_OpsList.next()) {
			if(obj->getBaseType() == t && obj -> getName() == n)
				list.append( obj );
		}
	} else {
		kdWarning() << "finding child object of unknown type: " << t << endl;
	}

	return list;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLObject* UMLClassifier::findChildObject(int id) {
        UMLClassifierListItem * o=0;
	for(o=m_OpsList.first();o != 0;o=m_OpsList.next()) {
		if(o->getID() == id)
			return o;
	}
	return UMLCanvasObject::findChildObject(id);
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifierList UMLClassifier::findSubClassConcepts (ClassifierType /*type*/) {
  UMLAssociationList list = this->getGeneralizations();
  UMLClassifierList inheritingConcepts;
  int myID = this->getID();
  for (UMLAssociation *a = list.first(); a; a = list.next())
  {
    // Concepts on the "A" side inherit FROM this class
    // as long as the ID of the role A class isnt US (in
    // that case, the generalization describes how we inherit
    // from another class).
    // SO check for roleA id, it DOESNT match this concepts ID,
    // then its a concept which inherits from us
    if (a->getRoleAId() != myID)
    {
      UMLObject* obj = a->getObjectA();
      UMLClassifier *concept = dynamic_cast<UMLClassifier*>(obj);
      if (concept)
        inheritingConcepts.append(concept);
    }
    
  }
  return inheritingConcepts;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifierList UMLClassifier::findSuperClassConcepts (ClassifierType /*type*/) {
  UMLAssociationList list = this->getGeneralizations();
  UMLClassifierList parentConcepts;
  int myID = this->getID();
  for (UMLAssociation *a = list.first(); a; a = list.next()) {
    // Concepts on the "B" side are parent (super) classes of this one
    // So check for roleB id, it DOESNT match this concepts ID,
    // then its a concept which we inherit from
    if (a->getRoleBId() != myID) {
      UMLObject* obj = a->getObjectB();
      UMLClassifier *concept = dynamic_cast<UMLClassifier*>(obj);
      if (concept)
        parentConcepts.append(concept);
    }
  }
  return parentConcepts;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifierList UMLClassifier::findSuperInterfaceConcepts () {
  UMLClassifierList parentConcepts;
  int myID = this->getID();
  // First the realizations, ie the actual interface implementations
  UMLAssociationList list = this->getRealizations();
  for (UMLAssociation *a = list.first(); a; a = list.next()) {
    // Concepts on the "B" side are parent (super) classes of this one
    // So check for roleB id, it DOESNT match this concepts ID,
    // then its a concept which we inherit from
    if (a->getRoleBId() != myID) {
      UMLObject* obj = a->getObjectB();
      UMLClassifier *concept = dynamic_cast<UMLClassifier*>(obj);
      if (concept)
        parentConcepts.append(concept);
    }
  }
  return parentConcepts;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifierList UMLClassifier::findAllSuperClassConcepts () {
  UMLClassifierList result = findSuperClassConcepts();
  UMLClassifierList inters = findSuperInterfaceConcepts();
  for (UMLClassifier *a = inters.first();
       0 != a;
       a = inters.next()) {
    result.append(a);
  }
  return result;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifierList UMLClassifier::findSuperAbstractConcepts () {
  int myID = this->getID();
  // First the interfaces
  UMLClassifierList parentConcepts = findSuperInterfaceConcepts();
  // Then add the generalizations of abstract objects
  UMLAssociationList list = this->getGeneralizations();
  for (UMLAssociation *a = list.first(); a; a = list.next()) {
    // Concepts on the "B" side are parent (super) classes of this one
    // So check for roleB id, it DOESNT match this concepts ID,
    // then its a concept which we inherit from
    if (a->getRoleBId() != myID) {
      UMLObject* obj = a->getObjectB();
      UMLClassifier *concept = dynamic_cast<UMLClassifier*>(obj);
      if (concept && concept->getAbstract())
        parentConcepts.append(concept);
    }
  }
  return parentConcepts;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifierList UMLClassifier::findImplementedAbstractConcepts () {
  // first get the list of interfaces we inherit from
  UMLClassifierList superint = findSuperAbstractConcepts();
  // now recursively find which interfaces we implement
  UMLClassifierList result(superint);
  for (UMLClassifier *a = superint.first(); a; a = superint.next()) {
    UMLClassifierList grandParents = a->findImplementedAbstractConcepts();
    for (UMLClassifier *b = grandParents.first(); b; b = grandParents.next()) {
      if (result.find(b) < 0) result.append(b);
    }
  }
  return result;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifierList UMLClassifier::findImplementedClasses () {
  // first get the list of classes we inherit from
  UMLClassifierList superint = findSuperClassConcepts();
  // now recursively find which classes we implement
  UMLClassifierList result(superint);
  for (UMLClassifier *a = superint.first(); a; a = superint.next()) {
    UMLClassifierList grandParents = a->findImplementedClasses();
    for (UMLClassifier *b = grandParents.first(); b; b = grandParents.next()) {
      if (result.find(b) < 0) result.append(b);
    }
  }
  return result;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifierList UMLClassifier::findAllImplementedClasses () {
  // first get the list of classes we inherit from
  UMLClassifierList superint = findAllSuperClassConcepts();
  // now recursively find which classes we implement
  UMLClassifierList result(superint);
  for (UMLClassifier *a = superint.first(); a; a = superint.next()) {
    UMLClassifierList grandParents = a->findAllImplementedClasses();
    for (UMLClassifier *b = grandParents.first(); b; b = grandParents.next()) {
      if (result.find(b) < 0) result.append(b);
    }
  }
  return result;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
bool UMLClassifier::operator==( UMLClassifier & rhs ) {
	if ( m_OpsList.count() != rhs.m_OpsList.count() ) {
		return false;
	}
	if ( &m_OpsList != &(rhs.m_OpsList) ) {
		return false;
	}
	return UMLCanvasObject::operator==(rhs);
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// this perhaps should be in UMLClass/UMLInterface classes instead.
bool UMLClassifier::acceptAssociationType(Uml::Association_Type type)
{
	switch(type)
	{
		case at_Generalization:
		case at_Aggregation:
		case at_Dependency:
		case at_Association:
		case at_Association_Self:
		case at_Implementation:
		case at_Composition:
		case at_Realization:
		case at_UniAssociation:
	 		return true;
		default:
			return false;
	}
	return false; //shutup compiler warning
}

bool UMLClassifier::hasAbstractOps () {
        UMLOperationList *opl = getFilteredOperationsList();
        for(UMLOperation *op = opl->first(); op ; op = opl->next())
                if(op->getAbstract())
                        return true;
        return false;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
int UMLClassifier::operations() {
	return m_OpsList.count();
}

////////////////////////////////////////////////////////////////////////////////////////////////////
UMLClassifierListItemList* UMLClassifier::getOpList() {
	return &m_OpsList;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
QPtrList<UMLOperation>*
UMLClassifier::getFilteredOperationsList(Scope permitScope,
                                         bool keepAbstractOnly)  {
	QPtrList<UMLOperation>* operationList = new QPtrList<UMLOperation>;
	for(UMLClassifierListItem* listItem = m_OpsList.first(); listItem;
	    listItem = m_OpsList.next())  {
		if (listItem->getBaseType() == ot_Operation) {
      UMLOperation* op = static_cast<UMLOperation*>(listItem);
      if (op->getScope() == permitScope &&
          (!keepAbstractOnly || op->getAbstract())) {
        operationList->append(op);
      }
		}
	}
	return operationList;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLOperationList* UMLClassifier::getFilteredOperationsList()  {
	UMLOperationList* operationList = new UMLOperationList;
	for(UMLClassifierListItem* listItem = m_OpsList.first(); listItem;
	    listItem = m_OpsList.next())  {
		if (listItem->getBaseType() == ot_Operation) {
			operationList->append(static_cast<UMLOperation*>(listItem));
		}
	}
	return operationList;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
void UMLClassifier::init() {
	m_BaseType = ot_UMLObject;
	m_OpsList.setAutoDelete(false);

	// make connections so that parent document is updated of list of uml objects
#ifdef __GNUC__
#warning "Cheap add/removeOperation fix for slot add/RemoveUMLObject calls. Need long-term solution"
#endif
        UMLDoc * parent = UMLApp::app()->getDocument();
        connect(this,SIGNAL(childObjectAdded(UMLObject *)),parent,SLOT(addUMLObject(UMLObject*)));
        connect(this,SIGNAL(childObjectRemoved(UMLObject *)),parent,SLOT(slotRemoveUMLObject(UMLObject*)));

}
////////////////////////////////////////////////////////////////////////////////////////////////////
bool UMLClassifier::saveToXMI( QDomDocument & qDoc, QDomElement & qElement ) {
	QDomElement classElement = qDoc.createElement("UML:Interface");
	bool status = UMLObject::saveToXMI( qDoc, classElement );
	//save operations
	UMLClassifierListItem* pOp = 0;
	for ( pOp = m_OpsList.first(); pOp != 0; pOp = m_OpsList.next() ) {
		pOp->saveToXMI(qDoc, classElement);
	}
	qElement.appendChild( classElement );
	return status;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
bool UMLClassifier::loadFromXMI( QDomElement & element ) {
	if( !UMLObject::loadFromXMI(element) ) {
		return false;
	}
	return load(element);
}

bool UMLClassifier::load(QDomElement& element) {
	QDomNode node = element.firstChild();
	QDomElement tempElement = node.toElement();
	while( !tempElement.isNull() ) {
		QString tag = tempElement.tagName();
		if (tag == "UML:Classifier.feature") {
			//CHECK: Umbrello currently assumes that nested elements
			// are features anyway.
			// Therefore the <UML:Classifier.feature> tag is of no
			// significance.
			if (! load(tempElement))
				return false;
		} else if (tag == "UML:Operation") {
			UMLOperation* op = new UMLOperation(NULL);
			if( !op->loadFromXMI(tempElement) ||
			    !this->addOperation(op) ) {
				delete op;
				return false;
			}
		}
		node = node.nextSibling();
		tempElement = node.toElement();
	}//end while
	return true;
}


#include "classifier.moc"
