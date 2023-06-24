import json, codecs

def load_config(config_file='config.json'):
    return json.load(codecs.open(config_file, encoding='utf-8'))

def topic_indexer(config, topic):
    return config["topic_names"][topic].get("index", -1)