def parseline(line):
    items = line.split()
    return [items[0], int(items[1]), items[2], int(items[3]), int(items[4]), int(eval(items[5]))]

raws = [parseline(l) for l in open('currentRoutes.txt').readlines()[13:]]

routes = {}
for pool, copy, fileclass, issmall, count, size in raws:
    if pool[0] == 'v': pool = pool[1:]
    if (pool, fileclass) not in routes :
        routes[(pool, fileclass)] = [set([copy]), set([issmall]), count, size]
    else:
        oldcopy, oldsmall, oldcount, oldsize = routes[(pool, fileclass)]
        oldcopy.add(copy)
        oldsmall.add(issmall)
        oldcount += count
        oldsize += size
        routes[(pool, fileclass)] = [oldcopy, oldsmall, oldcount, oldsize]

def kcmp(k1, k2):
    pool1, fileclass1 = k1
    pool2, fileclass2 = k2
    if fileclass1 == fileclass2:
        if pool1 < pool2:
            return -1
        elif pool1 == pool2:
            return 0
        else:
            return 1
    elif fileclass1 < fileclass2:
        return -1
    else:
        return 1

def kcmp2(k1, k2):
    pool1, fileclass1 = k1
    pool2, fileclass2 = k2
    if pool1 < pool2:
        return -1
    elif pool1 == pool2:
        if fileclass1 == fileclass2:
            return 0
        elif fileclass1 < fileclass2:
            return -1
        else:
            return 1
    else:
        return 1

def nbToDataAmount(n):
    '''converts a number into a readable amount of data'''
    ext = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
    magn = 0
    while n / 1024 > 5:
        magn += 1
        n = n / 1024
    return str(n) + ext[magn]

def set2str(s):
    r = ''
    for t in s:
        r += str(t)
    return r

ks = routes.keys()
ks.sort(cmp=kcmp2)
print "%15s %15s %10s %10s %-5s %s" % ('TapePool', 'Fileclass', 'Nb files', 'Data size', 'Small', 'Copy nbs')
for pool, fileclass in ks:
    copy, small, count, size = routes[(pool, fileclass)]
    print "%15s %15s %10d %10s %-5s %s" % (pool, fileclass, count, nbToDataAmount(size), set2str(small), set2str(copy))
