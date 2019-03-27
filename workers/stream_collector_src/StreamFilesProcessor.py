import json, os, sys
import multiprocessing
from utils.helper_utils import readable_time
from datetime import datetime, timedelta
import time

'''
StreamKeyProcessor, given a list of keywords, sorts files into its given folder path
    startTime  - the start-time for this Processor. This is used in case of crashes. Parent tracks start-time for each pair
                - This is a datetime object
                - StreamKeyProcessor can do the following for time processing
                    - check which file has been created in output directory. Get the next one from input directory (minute lossy)
                    - check most recent timestamp of output file, and continue from there (more granular)
                    - for each timestamp processed, register with parent. When start, parent informs about timestamp
    keywords    - a list of keywords for this StreamKeyProcessor
    rootName   - the name_lang pair for this processor, ex: "landslide_en"
'''
class StreamFilesProcessor(multiprocessing.Process):
    def __init__(self, startTime, keywords, rootName, errorQueue,messageQueue, SOCIAL_STREAMER_FILE_CHECK_COUNT):
        multiprocessing.Process.__init__(self)

        ''' set up relevant details '''
        self.keywords = keywords
        self.rootName = rootName
        self.DOWNLOAD_PREPEND = './downloads/'
        self.SOCIAL_STREAMER_FILE_CHECK_COUNT = SOCIAL_STREAMER_FILE_CHECK_COUNT

        self.timeDelta = timedelta(seconds=60)

        ''' Set up the time counter 
            Note the finishedUpToTime MUST be a datetime object '''
        if startTime is None:
            self.fishedUpToTime = None
            #First attempt to get most recent output file
            currentTime = datetime.now()
            foundFlag = 0
            while foundFlag == 0:
                filePath = self.getOutputPath(currentTime)
                if os.path.exists(filePath):
                    #we found the most recent file, and increment our counter
                    self.finishedUpToTime = currentTime+self.timeDelta
                    self.messageQueue.put(" ".join(["Found input file at",(str(self.finishedUpToTime))]))
                    foundFlag = 1
                else:
                    #if our search is too broad - i.e. we are a month behind, ignore
                    #TODO need better checks here
                    currentTime-=self.timeDelta
                    if (datetime.now() - currentTime).days > 25:
                        foundFlag = -1
            
            #If not exists, attempt to get earliest download file
            if foundFlag == -1:
                self.messageQueue.put("Did not find input file")
                currentTime = datetime.now() - timedelta(days=25)

                foundFlag = 0
                while foundFlag == 0:
                    filePath = self.getInputPath(currentTime)
                    if os.path.exists(filePath):
                        #we found the most recent file, and increment our counter
                        self.finishedUpToTime = currentTime
                        self.messageQueue.put(" ".join(["Found output file at",(str(self.finishedUpToTime))]))
                        foundFlag = 1
                    else:
                        #if our search is too broad - i.e. we are a month behind, ignore
                        #TODO need better checks here
                        currentTime+=self.timeDelta
                        if (datetime.now() - currentTime).seconds < 1:
                            foundFlag = -1

            if foundFlag == -1:
                #So nothing is there
                self.errorQueue.put("No files have been created.")


            

            #If not, crash???????

        else:
            self.fishedUpToTime = startTime
        #reset seconds to 0
        self.finishedUpToTime -= timedelta(seconds=self.finishedUpToTime.second)

        ''' Message queue for passing back errors and current times '''
        self.errorQueue = errorQueue
        self.messageQueue = messageQueue

        
    def run(self):
        ''' Starts the Processor '''
        self.messageQueue.put(" ".join(["Starting processor for",self.rootName]))
        try:
            #Run forever
            while True:
                #If we are not two minutes behind, we have to wait (to make sure the file is finished being written to)
                if (datetime.now() - self.finishedUpToTime).seconds < 120:
                    #self.messageQueue.put(" ".join(["Waiting for 2 minute delay at",readable_time()]))
                    waitTime = 120 -  (datetime.now() - self.finishedUpToTime).seconds
                    time.sleep(waitTime)
                else:
                    #We are not two minutes behind. We can start attempting to see if the file exists
                    filePath = self.getInputPath()

                    if not os.path.exists(filePath):
                        # At this point, we are at least 2 minutes behind, but the file still has not been created. So we wait for four total minutes
                        self.messageQueue.put(" ".join(["Expected file at",filePath,"has not been created at",readable_time()]))
                        waitTime = (datetime.now()-self.finishedUpToTime.second).seconds
                        #Difference is less than Four minutes
                        if waitTime < 60 * (self.SOCIAL_STREAMER_FILE_CHECK_COUNT + 1):
                            self.messageQueue.put("Waiting for four minute delay")
                            waitTime = waitTime - (60 * (self.SOCIAL_STREAMER_FILE_CHECK_COUNT + 1))
                            time.sleep(waitTime)
                        else:
                            #difference is more than four minutes - we can increment the the by one minute for the next ones
                            self.updateTime()
                    else:
                        #So at this point, we have a file and its been at least two minutes
                        
                        #Open the output writing path
                        outputPath = self.getOutputPath()
                        outputWritePath = open(outputPath, 'a')
                        with open(filePath, 'r') as fileRead:
                            
                            for line in fileRead:
                                #try to read the file if it fails skip it
                                try:
                                    jsonVersion = json.loads(line.strip())
                                    #Now we check if our keywords match
                                    for keyword in self.keywords:
                                        if keyword in jsonVersion:
                                            #write this to file
                                            outputWritePath.write(line)
                                            continue
                                    #So the previous one did not match the keyword
                                    #Now we go to the next line
                                except:
                                    #Maybe some error
                                    pass
                        #Done with file. Increment counter
                        outputWritePath.close()
                        self.updateTime()


        except Exception as e:
            self.errorQueue.put((str(e)))
            assert(1==2)



    def updateTime(self):
        self.finishedUpToTime += self.timeDelta

    def getInputPath(self, _time = None):
        if _time is None:
            pathDir = os.path.join(self.PREPEND + '%s_%s_%s' % ('tweets', 'unstructured', self.finishedUpToTime.year), '%02d' % self.finishedUpToTime.month,
                                            '%02d' % self.finishedUpToTime.day, '%02d' % self.finishedUpToTime.hour)
            filePath = os.path.join(pathDir, '%02d.json' % self.finishedUpToTime.minute)
        else:
            pathDir = os.path.join(self.PREPEND + '%s_%s_%s' % ('tweets', 'unstructured', _time.year), '%02d' % _time.month,
                                            '%02d' % _time.day, '%02d' % _time.hour)
            filePath = os.path.join(pathDir, '%02d.json' % _time.minute)
        return filePath

    def getOutputPath(self, _time = None):
        if _time is None:
            pathDir = os.path.join(self.PREPEND + '%s_%s_%s' % ('tweets', self.rootName, self.finishedUpToTime.year), '%02d' % self.finishedUpToTime.month,
                                            '%02d' % self.finishedUpToTime.day, '%02d' % self.finishedUpToTime.hour)
            filePath = os.path.join(pathDir, '%02d.json' % self.finishedUpToTime.minute)
        else:
            pathDir = os.path.join(self.PREPEND + '%s_%s_%s' % ('tweets', self.rootName, _time.year), '%02d' % _time.month,
                                            '%02d' % _time.day, '%02d' % _time.hour)
            filePath = os.path.join(pathDir, '%02d.json' % _time.minute)
        return filePath

    




