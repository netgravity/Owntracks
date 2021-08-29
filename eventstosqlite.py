import paho.mqtt.client as mqtt
import json
import sqlite3
from sqlite3 import Error
from datetime import datetime
import logging
from logging.config import dictConfig
import os
import yaml
import configfile

database = r"owntracks_data.db" # Database for storing events
path = 'logging_config.yml'# This is the yml file for configuring logging
if os.path.exists(path):
    with open(path, 'rt') as f:
        config = yaml.safe_load(f.read())
        dictConfig(config)
#Create Logger
logger = logging.getLogger('dev')
logger.propogate = False
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
        :param db_file: database file
        :return: Connection object or None
                            """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        logger.info(e)
    return conn

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
        """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        logger.info(e)


def on_connect(client, userdata, flags, rc):# The callback for when the client connects to the broker
    logger.info("Connected with owntracks/SunnyG-owntracks/SG/event")# Print result of connection attempt
    client.subscribe("owntracks/SunnyG-owntracks/SG/event")# Subscribe to the topic


def on_message(client, userdata, msg):  # The callback for when a PUBLISH message is received from the server.
    logger.info("Message received-> " + msg.topic + " " + str(msg.payload))  # Print a received msg
    topic = msg.topic
    try:
        logger.info("enetring try block")
        data = json.loads(msg.payload)
        conn = create_connection(database)
        with conn:
            cur = conn.cursor()
            cur.execute("select MAX(bid) from events")
            row = cur.fetchone()[0]
            if row == None:
                row = 0
            else:
                logger.info("BID has a value")
            #get current time from tst column and covnvert to datetime
            tstamp1 = datetime.fromtimestamp(data['tst'])
            if data['event'] == "enter":
                logger.info("event is enter")
                bid = row + 1;
                td_mins = 0 # this value is change in time, not req. while entering
            elif data['event'] == "leave":
                logger.info("event is leaving")
                bid = row
                """ normally tid is not 0 when leaving, maybe only
                the first time database is created and leave event is the first
                entry """
                if bid == 0:
                    logger.info("nothing to compare against")
                    td_mins = 0
                else:
                    cur.execute("select tst from events where bid=%s" % str(bid))
                    date_tstamp2 = cur.fetchone()[0] # this is the time value
                    #we need to subtract """
                    logger.info("entering time {} ".format(date_tstamp2))
                    fmt = '%Y-%m-%d %H:%M:%S'
                    tstamp2 = datetime.strptime(date_tstamp2, fmt) # convert from str to datetime
                    logger.info("leaving time  {} ".format( tstamp1))
                    td = tstamp1 - tstamp2 
                    td_mins = int(round(td.total_seconds() / 60))
                    logger.info('The difference is approx. %s minutes' % td_mins)
            event = (data['desc'],bid, data['event'], tstamp1, td_mins); """
            inserting into event , fetched desc and event, enter bid tstamp1
            and td_mins as calculated in the function"""
            logger.info(event)
            event_id = create_event(conn, event)
            logger.info ("Returned id is {0}".format(event_id))
            logger.info('successfully written to db')
    except Exception as e:
        logger.info (e)
        logger.info ("Cannot decode data on topic {0}".format(topic))

def create_event(conn, event):
    """ Create a new row in the event table
    :param conn:
    :param project:
    :return: project id
    """
    sql = ''' INSERT INTO events(desc,bid,event,tst,timediff)
    VALUES(?,?,?,?,?)'''

    cur = conn.cursor()
    cur.execute(sql, event)
    conn.commit()
    return cur.lastrowid

sql_create_events_table= """ CREATE TABLE IF NOT EXISTS events (id integer
PRIMARY KEY,desc text NOT NULL,bid integer, event text NOT NULL,tst
text NOT NULL, timediff integer);""" #time timestamp default(strftime('%s', 'now')));"""

# Create a database connection
conn = create_connection(database)
#Create table
if conn is not None:
    #Create events table
    create_table(conn, sql_create_events_table)
else:
    logger.info("Error creating database" + e)

client = mqtt.Client("owntracks_connect")  # Create instance of client with client IDt
#client.username_pw_set(user, password=password)
client.username_pw_set(configfile.username, password=configfile.password)
client.on_connect = on_connect  # Define callback function for successful connection
client.on_message = on_message # Define callback function for receipt of a message
# client.connect("m2m.eclipse.org", 1883, 60)  # Connect to (broker, port, keepalive-time)
client.tls_set('/etc/ssl/certs/DST_Root_CA_X3.pem')
#client.tls_insecure_set(True)
client.connect('enigma.netgravity.org', 48834)
client.loop_forever()  # Start networking daemon
