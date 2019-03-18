import MySQLdb as mysql


def get_db_connection(config,db=None):
    if db==None:
        return mysql.connect(   host=config['database']['db_host'], 
                                port=config['database']['db_port'], 
                                user=config['database']['db_user'], 
                                passwd=config['database']['db_passwd'], 
                                db=config['database']['db_db'], 
                                charset='utf8')
    else:
        return mysql.connect(   host=config['database']['db_host'], 
                                port=config['database']['db_port'], 
                                user=config['database']['db_user'], 
                                passwd=config['database']['db_passwd'], 
                                db=db, 
                                charset='utf8')

def run_sql_file(filename, connection):
    '''
    The function takes a filename and a connection as input
    and will run the SQL query on the given connection  
    '''    
    file = open(filename, 'r')
    sql = s = " ".join(file.readlines())
    cursor = connection.cursor()
    cursor.execute(sql)    
    connection.commit()
