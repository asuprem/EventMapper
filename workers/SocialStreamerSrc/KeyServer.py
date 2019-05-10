


class KeyServer():

    def __init__(self,config):
        #Set up keyserver
        self.keyserver={}
        for _key in config["APIKeys"]["keys"]:
            self.keyserver[_key] = {}
            self.keyserver[_key]["key"] =  config["APIKeys"]["keys"][_key]
            self.keyserver[_key]["count"] = 0
            self.keyserver[_key]["valid"] = True
            self.keyserver[_key]["MAX"] = 2

    #Compare to config and update keyserver
    def update(self,config):
        for _key in config["APIKeys"]["keys"]:
            #new key in config that isn't in keyserver
            if _key not in self.keyserver:
                self.keyserver[_key] = {}
                self.keyserver[_key]["key"] =  config["APIKeys"]["keys"][_key]
                self.keyserver[_key]["count"] = 0
                self.keyserver[_key]["valid"] = True
                self.keyserver[_key]["MAX"] = 2
            else:
                #if key exists in keyserver - check if info has changed
                #TODO generic
                if self.keyserver[_key]["key"] != config["APIKeys"]["keys"][_key]:
                    self.keyserver[_key]["key"] = config["APIKeys"]["keys"][_key]
        #Now all keys in config have been validated. So any key in keyserver that is not in keyserver is a deleted key
        #We invalidate it
        for _key in self.keyserver:
            if _key not in config["APIKeys"]["keys"]:
                self.invalidate(_key)
        
    #get a new key
    def get_key(self,):
        for _key in self.keyserver:
            if self.keyserver[_key]["count"] < self.keyserver[_key]["MAX"] and self.keyserver[_key]["valid"]:
                self.keyserver[_key]["count"]+=1
                return (_key, self.keyserver[_key]["key"])
        raise(self.NotEnoughKeys)

    #refresh an existing key
    def refresh_key(self,_key):
        if _key in self.keyserver:
            if self.keyserver[_key]["valid"]:
                return (_key, self.keyserver[_key]["key"])
            else:
                self.keyserver[_key]["count"]-=1
                self.verify()
        #either key is not in key_server - something funky happened, ot is was but it was invalid
        return self.get_key()
        
    #Verify -- remove invalid keys with zero count
    def verify(self,):
        for _key in self.keyserver:
            if not self.keyserver[_key]["valid"]:
                #TODO check with refresh key for less than 0 valid check - at no point should key count be less than 0
                if self.keyserver[_key]["count"]<=0:
                    del self.keyserver[_key]

    def invalidate(self,_key):
        self.keyserver[_key]["valid"] = False

    class NotEnoughKeys(Exception):
        pass
