def tapepool(self):
   return self.options.get('Environment','TAPEPOOLNAME')
Setup.getTag_tapepool = tapepool

def rfioTURL(self, nb=0):
    global RfioTURLs
    RfioTURLs = ['',
                 'rfio:///castor\?path=',
                 'rfio://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT']+'/castor\?path=',
                 'rfio://',
                 'rfio://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT'],
                 'rfio:///',
                 'rfio://'+os.environ['STAGE_HOST']+':'+os.environ['STAGE_PORT']+'/']
    snb = ''
    if nb > 0: snb = str(nb)
    return (lambda test : map(lambda x : x + self.getTag(test, 'noTapeFileName' + snb), RfioTURLs))
Setup.getTag_rfioTURL = rfioTURL

