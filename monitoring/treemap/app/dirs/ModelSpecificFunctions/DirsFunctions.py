from app.errors.NoDataAvailableError import NoDataAvailableError
#!!!app.dirs.models is imported at the end of the file!!!
import math

#exploits the tree structure to be much faster than Dirs.objects.get(fullname=dirname)
def getDirByName(dirname):
    depth = 0
    node = globals()['Dirs'].objects.get(depth = depth)
    pos = dirname.find('/')
    if dirname == '/': return node
    
    while pos >= 0:
        pos = dirname.find('/', pos + 1)
        depth = depth + 1
        children = list(globals()['Dirs'].objects.filter(parent=node.fileid, depth = depth))
        for child in children:
            if child.fullname == dirname:
                return child
            if child.fullname == dirname[:pos]:
                node = child
    raise NoDataAvailableError ("no such object")
        
#this import has to be here because of circular dependency with app.dirs.models!
from app.dirs.models import *