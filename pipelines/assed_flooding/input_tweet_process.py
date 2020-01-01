import utils.AssedMessageProcessor
import os

class input_tweet_process(utils.AssedMessageProcessor.AssedMessageProcessor):
    def __init__(self):
        self.DOWNLOAD_PREPEND = './downloads/'

    def process(self,data):
        write_version = {}
        write_version["id_str"] = data["id_str"]
        write_version["text"] = data["text"]
        write_version["location"] = data["place"]["full_name"] if data["place"] is not None else ""
        write_version["latitude"] = data["coordinates"]["coordinates"][1] if data["coordinates"] is not None else None
        write_version["longitude"] = data["coordinates"]["coordinates"][0] if data["coordinates"] is not None else None
        write_version["streamtype"] = "twitter"
        write_version["timestamp"] = data["timestamp_ms"]
        write_version["link"] = "https://twitter.com/"+data['user']["screen_name"]+"/status/"+data["id_str"]
        return write_version
    def getInputPath(self,_time):
        pathDir = os.path.join(self.DOWNLOAD_PREPEND + '%s_%s_%s_%s' % ('tweets', 'flooding','en', _time.year), '%02d' % _time.month,
                                            '%02d' % _time.day, '%02d' % _time.hour)
        filePath = os.path.join(pathDir, '%02d.json' % _time.minute)
        return filePath
