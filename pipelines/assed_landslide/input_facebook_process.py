import utils.AssedDataProcessor
import os

class input_facebook_process(utils.AssedDataProcessor.AssedMessageProcessor):
    def __init__(self):
        self.DOWNLOAD_PREPEND = './downloads/'

    def process(self,data):
        write_version = {}
        write_version["id_str"] = data["id_str"]
        write_version["text"] = data["text"]
        write_version["location"] = data["location"]
        write_version["latitude"] = data["latitude"]
        write_version["longitude"] = data["longitude"]
        write_version["streamtype"] = "facebook"
        write_version["timestamp"] = data["timestamp_ms"]
        write_version["link"] = data["link"]
        return write_version
    def getInputPath(self,_time):
        pathDir = os.path.join(self.DOWNLOAD_PREPEND + '%s_%s_%s_%s' % ('facebook', 'landslide','en', _time.year), '%02d' % _time.month,
                                            '%02d' % _time.day, '%02d' % _time.hour)
        filePath = os.path.join(pathDir, '%02d.json' % _time.minute)
        return filePath

    