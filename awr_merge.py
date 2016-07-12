#!/usr/bin/python

from HTMLParser import HTMLParser
from os import walk
import pprint
import optparse
import logging, sys



## On utilise HTMLParser au lieu de BeautifulSoup car c'est standart.
## TelQuel le parser ne gere pas les tags de meme type imbriques mais on s'en tape y en a pas.
class AwrParser (HTMLParser):


## INIT STUFF
    def __init__(self, inConfig):
        ## Init du parser
        HTMLParser.__init__(self)

        ## Liste de tableaux
        self.config = inConfig

        self._initAwrStuff()

    def _initAwrStuff(self):
        self.arraysBuffer = []
        self._initTableStuff()
    
    def _initTableStuff(self):
        self.arrayHBuffer = []
        self.arrayDBuffer = []
        self.recordArray = False
        self.thisArrayName = ''
        self.thisArrayLevel = ''
        self._initRowStuff()

    def _initRowStuff(self):
        self.inTag = ''
        self.rowBuffer = []
        self.rowIsHeader = False    
        self.tdCountFound = 0
        self.tdCountInserted = 0
        self._initDataStuff()

    def _initDataStuff(self):
        self.dataBuffer = ''
        self.hasData=False


## HANDLERS
    def handle_starttag(self, tag, attrs):
        self.inTag = tag.upper()

    def handle_endtag(self, tag):
        self.inTag = ''

        if self.recordArray:
            if tag.upper() == 'TH':
                self.rowIsHeader = True

            ## Si on as un tag vide on n'entre pas dans le handle_data. Il faut donc gerer l'insertion dans le handle_endtag.
            if (tag.upper() == 'TD' or tag.upper() == 'TH'):
                if self.hasData:
                    self.rowBuffer.append(self.dataBuffer)
                else:
                    self.rowBuffer.append('')
				
                self._initDataStuff()
			
            # En fin de ligne, on ajoute la ligne au buffer de tableau et on reinitialise tout	
            if tag.upper() == 'TR':
                if self.rowIsHeader:
                    self.arrayHBuffer.append(self.rowBuffer)
                else:
                    self.arrayDBuffer.append(self.rowBuffer)
                self._initRowStuff()

            # En fin de table, on ajoute le tableau au buffer d'awr et on reinitialise tout	
            if tag.upper() == 'TABLE':
                logging.debug('Recording ' + self.thisArrayName + ' finished')
                self.arraysBuffer.append( { "name": self.thisArrayName, "header": self.arrayHBuffer, "data": sorted(self.arrayDBuffer, key = lambda array: array[1]) })
                self._initTableStuff()


    def handle_data(self, data):

        if self.recordArray:
            ## Enregistrer les donnees dans le buffer de ligne
            if self.inTag.upper() == 'TD' or self.inTag.upper() == 'TH':
                self.dataBuffer = data
                self.hasData = True	
        else:
            ## Verifier que le texte correspond a un des textes repere pour charger les tableaux
            for config in self.config:
                if config["titleLevel"].upper() == self.inTag.upper():
                    self.thisArrayLevel = config["titleLevel"]
					
                    if data.find(config["name"]) >= 0:
                        logging.debug('Recording ' + config["name"] + ' started')
                        self.thisArrayName = config["name"]
                        self.recordArray = True


def main():
    ### INITIALISER LES OPTIONS
    p = optparse.OptionParser( usage = "usage: %prog [options] directory", version = "%prog 00.01")
    p.add_option('--log-level', '-l', action='store', dest='logLevel', default='ERROR')
    options, arguments = p.parse_args()

    if options.logLevel == 'DEBUG':
        options.logLevel = logging.DEBUG    
    elif options.logLevel == 'INFO':
        options.logLevel = logging.INFO
    elif options.logLevel == 'ERROR':
        options.logLevel = logging.ERROR
    else:
        p.error('log level should be DEBUG or INFO, default is INFO')
		
    if len(arguments) != 1:
        p.error("incorrect number of arguments")
    else:
        # mypath="/media/sf_Capdata/python/awr/AWR/"
        directory = arguments[0]

    ### CONFIG DU LOG
    logging.basicConfig(stream=sys.stderr, level=options.logLevel, format='[%(levelname)s] [%(funcName)s]: %(message)s')

    logging.debug("Options : " + pprint.pformat(options))
    logging.debug("Arguments : " + pprint.pformat(arguments))

    ### CHARGEMENT DES AWR par repertoires
    fileList=[]
    for (dirpath, dirnames, filenames) in walk(directory):
        for name in filenames:
            fileList.append(dirpath + "/" + name)
    fileList.sort()

    result, awrList = [], []
    loadAwrs(fileList, awrList)
    mergeAwrs(awrList, result)
    resultToCsv(result)

def loadAwrs(fileList, awrList):
    ### TRAITEMENT DES FICHIERS
    logging.info("Chargement des AWRs")

    parserConf = [
        { "name": "WORKLOAD REPOSITORY report", "titleLevel": "H1" }
        ,{ "name": "Foreground Wait Events", "titleLevel": "H3" }
        ,{ "name": "Background Wait Events", "titleLevel": "H3" }
    ]

    for theFile in fileList:
        logging.info( "** "+theFile)
        parser = AwrParser(parserConf)

        file = open(theFile, 'r')
        html = file.read()
        parser.feed(html)
        file.close()
	
        awrList.append({ "name": theFile, "data": parser.arraysBuffer })


def mergeAwrs(awrList, result):
    logging.info("Merge des AWRs")

    ### MERGE Tout le tableau dans un seul tableau, prefixe pr le nom de fichier et trie par le nom d'event
    logging.debug("STEP1: Merge")
    merge = []

    for i in [1, len(awrList[0]["data"])-1]:
        tmpArray = []
        arrayName = ''

        for nawr, awr in enumerate(awrList):
            logging.debug("Tableau: " + awr["data"][i]["name"] + " File: " + awr["name"])
            arrayName = awr["data"][i]["name"]

            for line in awr["data"][i]["data"]:
                tmp = []
                tmp.append(nawr)
                tmp += line
                tmpArray.append(tmp)

        tmpArray.sort(key = lambda array: array[1])
        merge.append( { "name": arrayName, "data": tmpArray } )
    

   ### CREATION D'UN TABLEAU RECAP
    logging.debug("STEP2: combine")

    for tableau in merge:
        logging.debug("Tableau: " + tableau["name"])
        tmpArray = []
        prevName = tableau["data"][1]
        curIndex = 0
        maxIndex = len(awrList)-1
        buffer = [] 

        for line in tableau["data"]:
            def help():
                logging.debug(str.format("Gros probleme d'index dans le merge\n curIndex: {0}\n index tab(line[0]): {1}\n prevName {2}\n currName: {3}\n line: {4}"
                    ,curIndex   
                    ,line[0]
                    ,prevName
                    ,line[1]
                    ,pprint.pformat(line)
                ))
                exit(1)

            ## line[0] l'index
            ## line[1] le nom de la stat/event
            if prevName <> line[1]:	
                while curIndex <= maxIndex:
                    buffer.append([])
                    curIndex += 1	

                tmpArray.append(buffer)
                curIndex = 0
                buffer = []
                prevName = line[1]
            
            if curIndex == line[0]:
                buffer.append(line)

            elif curIndex < line[0]:
                while curIndex < line[0]:
                    buffer.append([])
                    curIndex += 1
                buffer.append(line)		

            elif curIndex > line[0]:
                help()

            curIndex += 1	
            prevName = line[1]

        result.append({ "name": tableau["name"], "data": tmpArray} )

def resultToCsv(result):
    logging.info("Conversion en CSV")

    for table in result:
        logging.info("Tableau: " + table["name"])

        for line in table["data"]:
            if len(line[0]) == 0:
                continue

            str = line[0][1]

            for srv in line:
                if len(srv) > 0:
                    str += ';' + srv[2]
                else:	
                    str += ';'

            print str+';'

if __name__ == '__main__':
    main()

exit(0)
