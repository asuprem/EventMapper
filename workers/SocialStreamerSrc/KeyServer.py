


class KeyServer():

    def __init__(self,config, key_mode="twitter", key_max = 1):
        """ Initialize the KeyServer.

        Takes in parameters to set up the key distribution server

        Args:
            config -- [DICT] The entire assed_config.json dict. Normally passed by using load_config('assed_config.json')
            key_mode -- which application's keys to use
            key_max -- maximum number of assignments per key. This is for applications that need to use the same key for multiple applications. Sometimes API sources limit number of concurrent connections per key.

        """
 
        self.keyserver={}
        self.key_max = key_max
        self.key_mode = key_mode
        for _key in config["APIKEYS"][self.key_mode]:
            self.keyserver[_key] = {}
            self.keyserver[_key]["key"] =  config["APIKEYS"][self.key_mode][_key]
            self.keyserver[_key]["count"] = 0
            self.keyserver[_key]["valid"] = True

    def update(self,config):
        """ Update keyserver with new configuration file """

        for _key in config["APIKEYS"][self.key_mode]:
            # For a new key in config that is not in key_server
            if _key not in self.keyserver:
                self.keyserver[_key] = {}
                self.keyserver[_key]["key"] =  config["APIKEYS"][self.key_mode][_key]
                self.keyserver[_key]["count"] = 0
                self.keyserver[_key]["valid"] = True
            else:
                # Key exists in keyserver, but may have changed...
                if self.keyserver[_key]["key"] != config["APIKEYS"][self.key_mode][_key]:
                    self.keyserver[_key]["key"] = config["APIKEYS"][self.key_mode][_key]
        # Now all keys in config have been validated.Any remaining key in keyserver not in config is a deleted key. Invalidate the key
        for _key in self.keyserver:
            if _key not in config["APIKEYS"][self.key_mode]:
                self.invalidate(_key)
        
    def get_key(self,):
        """ Get the first valid key (or tuple or what have you), with its keyname """
        for _key in self.keyserver:
            if self.keyserver[_key]["count"] <= self.key_max and self.keyserver[_key]["valid"]:
                self.keyserver[_key]["count"]+=1
                return (_key, self.keyserver[_key]["key"])
        raise(IndexError("Not enough keys available in KeyServer for key_mode='%s'"%self.key_mode))

    def refresh_key(self,_key):
        """ Refresh an existing key (i.e. if it has been changed, etc etc)
        
        Args:
            _key -- key_name provided during get_key()
        
        Raises:
            ValueError if _key not in KeyServer, which shouldn't happen unless thing go really really awry.

        """
        # The _key is still valid
        if self.keyserver[_key]["valid"]:
            return (_key, self.keyserver[_key]["key"])
        else:
            # The _key is no longer still valid
            self.keyserver[_key]["count"]-=1
            self.verify()
        # Return a new key_tuple pair.
        return self.get_key()
        
    def verify(self,):
        """ Verify -- remove invalid keys with zero count """
        for _key in self.keyserver:
            if not self.keyserver[_key]["valid"]:
                if self.keyserver[_key]["count"] == 0:
                    del self.keyserver[_key]
                elif self.keyserver[_key]["count"] < 0:
                    raise ValueError("Key count is less than 0.")
                else:
                    pass

    def invalidate(self,_key):
        """ Invalidate a key and schedule it for deletion. """

        self.keyserver[_key]["valid"] = False