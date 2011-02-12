from app.errors.NoDataAvailableError import NoDataAvailableError
#!!!app.dirs.models is imported at the end of the file!!!
import math
import app.dirs

#exploits the tree structure to be much faster than Dirs.objects.get(fullname=dirname)
def getDirByName(dirname):
    dirsmodel = app.dirs.models.Dirs #optimize dots away
    depth = 0
    node = dirsmodel.objects.get(depth = depth)
    pos = dirname.find('/')
    if dirname == '/': return node
    dirname = str(dirname)
    fullnamehash = 0
    
    while pos >= 0:
        pos = dirname.find('/', pos + 1)
        depth = depth + 1
        children = list(dirsmodel.objects.filter(parent=node.fileid, depth = depth))
        for child in children:
            fullnamehash = hash(str(child.fullname))
            if fullnamehash == hash(dirname):
                if child.fullname == dirname:
                    return child
            if fullnamehash == hash(dirname[:pos]):
                if child.fullname == dirname[:pos]:
                    node = child

        
    raise NoDataAvailableError ("no such object")
        
