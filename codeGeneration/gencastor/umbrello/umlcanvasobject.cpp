/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
#include "umlcanvasobject.h"
#include "classifier.h"
#include "association.h"
#include "attribute.h"
#include "operation.h"
#include "template.h"
#include "clipboard/idchangelog.h"
#include <kdebug.h>
#include <klocale.h>

UMLCanvasObject::UMLCanvasObject(const QString & name, int id) 
   : UMLObject(name, id) 
{
	init();
}

////////////////////////////////////////////////////////////////////////////////////////////////////
UMLCanvasObject::~UMLCanvasObject() {
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLAssociationList UMLCanvasObject::getSpecificAssocs(Uml::Association_Type assocType) {
	UMLAssociationList list;
	for (UMLAssociation* a = m_AssocsList.first(); a; a = m_AssocsList.next())
		if (a->getAssocType() == assocType)
			list.append(a);
	return list;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
bool UMLCanvasObject::addAssociation(UMLAssociation* assoc) {
	// add association only if not already present in list
	if(!hasAssociation(assoc))
	{
		m_AssocsList.append( assoc );
		emit modified();
		emit sigAssociationAdded(assoc);
		return true;
	}
	return false;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
bool UMLCanvasObject::hasAssociation(UMLAssociation* assoc) {
	if(m_AssocsList.containsRef(assoc) > 0)
		return true;
	return false;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
int UMLCanvasObject::removeAssociation(UMLAssociation * assoc) {
	if(!hasAssociation(assoc) || !m_AssocsList.remove(assoc)) {
		kdWarning() << "can't find assoc given in list" << endl;
		return -1;
	}
	emit modified();
	emit sigAssociationRemoved(assoc);
	return m_AssocsList.count();
}
////////////////////////////////////////////////////////////////////////////////////////////////////
QString UMLCanvasObject::uniqChildName(const UMLObject_Type type) {
	QString currentName;
	if (type == ot_Association) {
		currentName = i18n("new_association");
	} else {
		kdWarning() << "uniqChildName() called for unknown child type" << endl;
	}

	QString name = currentName;
	for (int number = 1; findChildObject(type, name).count(); ++number) {
	        name = currentName + "_" + QString::number(number);
	}
	return name;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLObjectList UMLCanvasObject::findChildObject(UMLObject_Type t, QString n) {
	UMLObjectList list;
	if (t == ot_Association) {
		UMLAssociation * obj=0;
		for (obj = m_AssocsList.first(); obj != 0; obj = m_AssocsList.next()) {
			if (obj->getBaseType() == t && obj -> getName() == n)
				list.append( obj );
		}
	} else {
		kdWarning() << "unknown type in findChildObject()" << endl;
	}
	return list;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
UMLObject* UMLCanvasObject::findChildObject(int id) {
	UMLAssociation * asso = 0;
	for (asso = m_AssocsList.first(); asso != 0; asso = m_AssocsList.next()) {
		if (asso->getID() == id)
			return asso;
	}
	return 0;
}

void UMLCanvasObject::init() {
	m_AssocsList.setAutoDelete(false);
}

bool UMLCanvasObject::operator==(UMLCanvasObject& rhs) {
	if (this == &rhs) {
		return true;
	}
	if ( !UMLObject::operator==(rhs) ) {
		return false;
	}
	if ( m_AssocsList.count() != rhs.m_AssocsList.count() ) {
		return false;
	}
	if ( &m_AssocsList != &(rhs.m_AssocsList) ) {
		return false;
	}
	return true;
}

int UMLCanvasObject::associations() {
	return m_AssocsList.count();
}

const UMLAssociationList& UMLCanvasObject::getAssociations() {
	return m_AssocsList;
}

UMLClassifierList UMLCanvasObject::getSuperClasses() {
	UMLClassifierList list;
	// WARNING:
	// Currently there's quite some chaos regarding role assignments in
	// generalizations. The diagram code (AssociationWidget et al.) assumes
	//    B -> A
	// i.e. the specialized class is role B and the general class is role A,
	// while the document code (i.e. UMLAssociation et al.) assumes
	//    A -> B
	// i.e. the general class is role B and the specialized class is role A,
	// Right here, we have the A->B case.
	// I hope to clean this up shortly   --okellogg 2003/12/22
	for (UMLAssociation* a = m_AssocsList.first(); a; a = m_AssocsList.next()) {
		if ( a->getAssocType() != Uml::at_Generalization ||
		     a->getRoleAId() != this->getID() )
			continue;
		UMLClassifier *c = dynamic_cast<UMLClassifier*>(a->getObjectB());
		if (c)
			list.append(c);
		else
			kdDebug() << "UMLCanvasObject::getSuperClasses: generalization's"
				  << " other end is not a UMLClassifier"
				  << " (id= " << a->getRoleBId() << ")" << endl;
	}
	return list;
}

UMLClassifierList UMLCanvasObject::getSubClasses() {
	UMLClassifierList list;
	// WARNING: See remark at getSuperClasses()
	for (UMLAssociation* a = m_AssocsList.first(); a; a = m_AssocsList.next()) {
		if ( a->getAssocType() != Uml::at_Generalization ||
		     a->getRoleBId() != this->getID() )
			continue;
		UMLClassifier *c = dynamic_cast<UMLClassifier*>(a->getObjectA());
		if (c)
			list.append(c);
		else
			kdDebug() << "UMLCanvasObject::getSubClasses: specialization's"
				  << " other end is not a UMLClassifier"
				  << " (id=" << a->getRoleAId() << ")" << endl;
	}
	return list;
}

UMLAssociationList UMLCanvasObject::getStdAssociations() {
  UMLAssociationList al =
    getSpecificAssocs(Uml::at_Association);
  UMLAssociationList ul =
    getSpecificAssocs(Uml::at_UniAssociation);
  for (unsigned int i = 0; i < ul.count(); i++) {
    al.append(ul.at(i));
  }
  return al;
}

UMLAssociationList UMLCanvasObject::getGeneralizations() {
	return getSpecificAssocs(Uml::at_Generalization);
}

UMLAssociationList UMLCanvasObject::getRealizations() {
	return getSpecificAssocs(Uml::at_Realization);
}

UMLAssociationList UMLCanvasObject::getAggregations() {
	return getSpecificAssocs(Uml::at_Aggregation);
}

UMLAssociationList UMLCanvasObject::getCompositions() {
	return getSpecificAssocs(Uml::at_Composition);
}
#include "umlcanvasobject.moc"
