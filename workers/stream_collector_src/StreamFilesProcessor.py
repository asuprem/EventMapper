import json, os, sys, traceback
import multiprocessing
from utils.helper_utils import readable_time, std_flush
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
        
        ''' Message queue for passing back errors and current times '''
        self.errorQueue = errorQueue
        self.messageQueue = messageQueue

        ''' set up relevant details '''
        self.keywords = keywords
        self.rootName = rootName
        self.DOWNLOAD_PREPEND = './downloads/'
        self.SOCIAL_STREAMER_FILE_CHECK_COUNT = SOCIAL_STREAMER_FILE_CHECK_COUNT
        self.BACK_CHECK_FILES_DAYS = 10
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
                    #self.messageQueue.put(" ".join(["Found input file at",(str(self.finishedUpToTime))]))
                    std_flush(" ".join([self.rootName, "Found output-stream file at",(str(filePath))]))
                    foundFlag = 1
                else:
                    #if our search is too broad - i.e. we are a month behind, ignore
                    #TODO need better checks here
                    currentTime-=self.timeDelta
                    if (datetime.now() - currentTime).days > self.BACK_CHECK_FILES_DAYS:
                        foundFlag = -1
            
            #If not exists, attempt to get earliest download file
            if foundFlag == -1:
                std_flush(" ".join([self.rootName, "Did not find any output-stream files."]))
                currentTime = datetime.now() - timedelta(days=self.BACK_CHECK_FILES_DAYS)

                foundFlag = 0
                while foundFlag == 0:
                    filePath = self.getInputPath(currentTime)
                    if os.path.exists(filePath):
                        #we found the most recent file, and increment our counter
                        self.finishedUpToTime = currentTime
                        std_flush(" ".join([self.rootName, "Found input-stream file at",(str(filePath))]))
                        foundFlag = 1
                    else:
                        #if our search is too broad - i.e. we are a month behind, ignore
                        #TODO need better checks here
                        currentTime+=self.timeDelta
                        timeDeltaOutputStream = (datetime.now() - currentTime)
                        if timeDeltaOutputStream.days  == 0 and timeDeltaOutputStream.seconds  <= 1:
                            foundFlag = -1

            if foundFlag == -1:
                #So nothing is there
                std_flush(" ".join([self.rootName, "Did not find any input-stream files."]))
                #raise(self.NoStartTimeGivenAndNoFilesExist)
                assert(1==2)
            #If not, crash???????

        else:
            self.fishedUpToTime = startTime
        #reset seconds to 0
        self.finishedUpToTime -= timedelta(seconds=self.finishedUpToTime.second)
        
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
                        ''' self.messageQueue.put(" ".join(["Expected file at",filePath,"has not been created at",readable_time()])) '''
                        waitTime = (datetime.now()-self.finishedUpToTime).seconds
                        #Difference is less than Four minutes
                        if waitTime < 60 * (self.SOCIAL_STREAMER_FILE_CHECK_COUNT + 1):
                            #self.messageQueue.put("Waiting for four minute delay")
                            #std_flush(str(waitTime))
                            waitTime =  (60 * (self.SOCIAL_STREAMER_FILE_CHECK_COUNT + 1)) - waitTime
                            #std_flush(str(waitTime))
                            time.sleep(waitTime)
                        else:
                            #difference is more than four minutes - we can increment the the by one minute for the next ones
                            #self.messageQueue.put(" ".join(["Expected file at",filePath,"has not been created at",readable_time()]))
                            self.updateTime()
                    else:
                        #So at this point, we have a file and its been at least two minutes
                        
                        #Open the output writing path
                        self.makeOutputPath()
                        outputPath = self.getOutputPath()
                        outputWritePath = open(outputPath, 'a')
                        
                        with open(filePath, 'r') as fileRead:
                            
                            for line in fileRead:
                                #try to read the file if it fails skip it
                                try:
                                    jsonVersion = json.loads(line.strip())
                                    #Now we check if our keywords match
                                    keywordFoundFlag = False
                                    for keyword in self.keywords:
                                        if keyword in jsonVersion["text"]:
                                            #write this to file
                                            
                                            outputWritePath.write(line)
                                            
                                            #std_flush(" ".join(["Good:   ", jsonVersion['text']]))
                                            keywordFoundFlag = True
                                            break
                                    #So the previous one did not match the keyword
                                    #Now we go to the next line
                                    #if not keywordFoundFlag:
                                    #    std_flush(" ".join(["Bad:   ", jsonVersion['text']]))
                                except Exception as e:
                                    #Maybe some error
                                    self.messageQueue.put(" ".join(["Possible warning for", self.rootName,":",str(e) ]))
                                    pass
                        #Done with file. Increment counter
                        outputWritePath.close()
                        
                        self.updateTime()


        except Exception as e:
            traceback.print_exc(file=sys.stdout)
            self.errorQueue.put((self.rootName, str(e)))
            



    def updateTime(self):
        self.finishedUpToTime += self.timeDelta

    def getInputPath(self, _time = None):
        if _time is None:
            pathDir = os.path.join(self.DOWNLOAD_PREPEND + '%s_%s_%s' % ('tweets', 'unstructured', self.finishedUpToTime.year), '%02d' % self.finishedUpToTime.month,
                                            '%02d' % self.finishedUpToTime.day, '%02d' % self.finishedUpToTime.hour)
            filePath = os.path.join(pathDir, '%02d.json' % self.finishedUpToTime.minute)
        else:
            pathDir = os.path.join(self.DOWNLOAD_PREPEND + '%s_%s_%s' % ('tweets', 'unstructured', _time.year), '%02d' % _time.month,
                                            '%02d' % _time.day, '%02d' % _time.hour)
            filePath = os.path.join(pathDir, '%02d.json' % _time.minute)
        return filePath

    def makeOutputPath(self):
        pathDir = os.path.join(self.DOWNLOAD_PREPEND + '%s_%s_%s' % ('tweets', self.rootName, self.finishedUpToTime.year), '%02d' % self.finishedUpToTime.month,
                                            '%02d' % self.finishedUpToTime.day, '%02d' % self.finishedUpToTime.hour)
        try:
            os.makedirs(pathDir)
        except:
            pass
    def getOutputPath(self, _time = None):
        if _time is None:
            pathDir = os.path.join(self.DOWNLOAD_PREPEND + '%s_%s_%s' % ('tweets', self.rootName, self.finishedUpToTime.year), '%02d' % self.finishedUpToTime.month,
                                            '%02d' % self.finishedUpToTime.day, '%02d' % self.finishedUpToTime.hour)
            filePath = os.path.join(pathDir, '%02d.json' % self.finishedUpToTime.minute)
        else:
            pathDir = os.path.join(self.DOWNLOAD_PREPEND + '%s_%s_%s' % ('tweets', self.rootName, _time.year), '%02d' % _time.month,
                                            '%02d' % _time.day, '%02d' % _time.hour)
            filePath = os.path.join(pathDir, '%02d.json' % _time.minute)
        return filePath

    class NoStartTimeGivenAndNoFilesExist(Exception):
        pass

    




