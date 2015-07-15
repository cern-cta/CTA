f = open('alimdc.raw').readlines()
g = open('alimdcSmooth.raw',  'w')

win = 24
i = 0
avRead = 0
avWrite = 0
for l in f:
  ll = l.split(' ')
  time = ll[1]
  avRead = avRead + int(ll[2])
  avWrite = avWrite + int(ll[3])
  i = i + 1
  if i == win:
     g.write(time + " " + str(avRead/win) + " " + str(avWrite/win) + "\n")
     i = 0
     avRead = 0
     avWrite = 0
g.close()
