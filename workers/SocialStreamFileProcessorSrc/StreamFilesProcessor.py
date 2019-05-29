""" This module contains the StreamFilesProcessor, which extracts relevant tweets from the data dumps given a set of keywords for a topic. """

import json, os, sys, traceback
import multiprocessing
from utils.helper_utils import readable_time, std_flush
import utils.CONSTANTS as CONSTANTS
from datetime import datetime, timedelta
import time

class StreamFilesProcessor(multiprocessing.Process):
    """ 
    
    This is the StreamFilesProcessor class. It is inherited from multiprocessing.Process. StreamKeyProcessor, given a list of keywords, sorts files into its given folder path

    ...

    Attributes
    ----------
    startTime : datetime
        The start-time for this StreamFilesProcessor object. If None, StreamFilesProcessor handles when to start by searching for either the latest generated output or the earliest input files. startTime should normally be None.
    keywords : list
        A list of keywords for this StreamFilesProcessor object. They should be utf-8 encoded. StreamFilesProcessor takes the list of keywords and searches the text content of a social post for the keywords.
    rootName : str
        rootName is used by the StreamFilesProcessor object to create output folder. The folder follows [source]_rootName_[YEAR]/[MONTH]/[DAY]/[HOUSE]/[MINUTE].json. rootName should follow this format: "[TOPIC]_[LANGUAGE]". 
    errorQueue : multiprocessing.Queue
        This Queue is used to post erros back to the parent.
    messageQueue : multiprocessing.Queue
        This Queue is used to post messages back to the parent.

    Methods
    -------
    __init__(self, startTime, keywords, rootName, errorQueue,messageQueue)
        Sets up the StreamFilesProcessor object. 

    run(self)
        This executes the core code. The StreamFilesProcessor object iterates through unread files in tweets_unstructured_2019/. Any social objects with words matching keywords are sent to the appropriate directory.

    updateTime(self)
        This increments internal time state by a single timedelta (1 minute), allowing access to the next file

    getInputPath(self, _time)
        This gets the path for the input-stream (tweets_unstructured_[YEAR]/)

    getOutputPath(self, _time)
        This gets the path for the output-stream (tweets_[TOPIC]_[LANGUAGE]_[YEAR]/)

    makeOutputPath(self)
        Creates the output-stream directory path for checks.

    """

    def __init__(self, startTime, keywords, rootName, errorQueue,messageQueue):
        multiprocessing.Process.__init__(self)
        
        ''' Message queue for passing back errors and current times '''
        self.errorQueue = errorQueue
        self.messageQueue = messageQueue

        ''' set up relevant details '''
        self.keywords = keywords
        self.rootName = rootName
        self.DOWNLOAD_PREPEND = './downloads/'
        self.STREAM_FILES_PROCESSOR_MAX_SECOND_DELAY = CONSTANTS.STREAM_FILES_PROCESSOR_MAX_SECOND_DELAY
        self.BACK_CHECK_FILES_DAYS = 10
        self.timeDelta = timedelta(seconds=CONSTANTS.STREAMING_GRANULARITY_SECONDS)

        ''' Set up the time counter 
            Note the finishedUpToTime MUST be a datetime object '''
        
        if startTime is None:
            self.fishedUpToTime = None
            # First attempt to get most recent output file
            currentTime = datetime.now()
            foundFlag = 0
            while foundFlag == 0:
                filePath = self.getOutputPath(currentTime)
                if os.path.exists(filePath):
                    # We found the most recent file, and increment our counter
                    self.finishedUpToTime = currentTime+self.timeDelta
                    std_flush(" ".join([self.rootName, "Found output-stream file at",(str(filePath))]))
                    foundFlag = 1
                else:
                    #if our search is too broad - i.e. we are a month behind, ignore
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
                        currentTime+=self.timeDelta
                        timeDeltaOutputStream = (datetime.now() - currentTime)
                        if timeDeltaOutputStream.days  == 0 and timeDeltaOutputStream.seconds  <= 1:
                            foundFlag = -1

            if foundFlag == -1:
                #So nothing is there
                std_flush(" ".join([self.rootName, "Did not find any input-stream files."]))
                #raise(self.NoStartTimeGivenAndNoFilesExist)
                raise RuntimeError()
            #If not, crash???????

        else:
            self.fishedUpToTime = startTime
        #reset seconds to 0
        self.finishedUpToTime -= timedelta(seconds=self.finishedUpToTime.second)
        self.previousMessageTime = self.finishedUpToTime

    def run(self):
        ''' Starts the Processor '''
        self.messageQueue.put(" ".join(["Starting processor for",self.rootName]))
        try:
            #Run forever
            #Message every two hours that all's going well...
            if (self.finishedUpToTime - self.previousMessageTime).total_seconds() > 7200:
                self.previousMessageTime = self.finishedUpToTime
                self.messageQueue.put("Still chugging along. Just starting to process file at : " + str(self.finishedUpToTime))
            while True:
                #If we are not two minutes behind, we have to wait (to make sure the file is finished being written to)
                if (datetime.now() - self.finishedUpToTime).total_seconds() < 120:
                    #self.messageQueue.put(" ".join(["Waiting for 2 minute delay at",readable_time()]))
                    waitTime = 120 -  (datetime.now() - self.finishedUpToTime).seconds
                    time.sleep(waitTime)
                else:
                    #We are not two minutes behind. We can start attempting to see if the file exists
                    filePath = self.getInputPath()

                    if not os.path.exists(filePath):
                        # At this point, we are at least 2 minutes behind, but the file still has not been created. So we wait for four total minutes
                        #self.messageQueue.put(" ".join(["Expected file at",filePath,"has not been created at",readable_time()]))
                        waitTime = (datetime.now()-self.finishedUpToTime).total_seconds()
                        #Difference is less than Four minutes
                        if waitTime < self.STREAM_FILES_PROCESSOR_MAX_SECOND_DELAY:
                            #self.messageQueue.put("Waiting for four minute delay")
                            #std_flush(str(waitTime))
                            waitTime = self.STREAM_FILES_PROCESSOR_MAX_SECOND_DELAY - waitTime
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
                        #badCount = 0
                        #totalCount = 0
                        with open(filePath, 'r') as fileRead:
                            for line in fileRead:
                                #try to read the file if it fails skip it
                                try:
                                    jsonVersion = json.loads(line.strip())
                                    #Now we check if our keywords match
                                    #keywordFoundFlag = False
                                    for keyword in self.keywords:
                                        if keyword in jsonVersion["text"]:
                                            #write this to file
                                            
                                            outputWritePath.write(line)
                                            
                                            #std_flush(" ".join(["Good:   ", jsonVersion['text']]))
                                            #keywordFoundFlag = True
                                            break
                                    #So the previous one did not match the keyword
                                    #Now we go to the next line
                                    #if not keywordFoundFlag:
                                    #    badCount += 1
                                    #totalCount+=1
                                except ValueError as e:
                                    #Maybe some error
                                    self.messageQueue.put(" ".join(["Possible warning for",filePath,": file for",  self.rootName,":",str(e)]))
                        #if badCount:
                        #    std_flush(" ".join([self.rootName, " : Number of skips:   ", str(badCount), " out of ", str(totalCount), " at ", str(self.finishedUpToTime)]))
                        #    badCount = 0; totalCount = 0
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

    




