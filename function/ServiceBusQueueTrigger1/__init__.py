import logging
import azure.functions as func
import psycopg2
import sys
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# Get the current directory of the script
# current_dir = os.path.dirname(os.path.abspath(__file__))

# # Append the parent directory (which contains shared_module) to the sys.path
# web_path = os.path.join(current_dir, "..", "web")
# sys.path.append(web_path)

# # Now you can import your module
# from web.config import BaseConfig

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    dbname: str = os.environ.get("POSTGRES_DB")
    dbuser: str = os.environ.get("POSTGRES_USER")
    dbpass: str = os.environ.get("POSTGRES_PW")
    dbhost: str = os.environ.get("POSTGRES_URL")
    conn = psycopg2.connect(dbname=dbname,
                            user=dbuser,
                            password=dbpass,
                            host=dbhost
                            )
    cur = conn.cursor()

    try:
        
        # Get notification message and subject from database using the notification_id
        cur.execute("SELECT message, subject " + 
                    "FROM techconfdb " +
                    "WHERE id = %s" +
                    (notification_id))
        notification_message = cur.fetchall()[0]
        logging.info("Notification message: " + notification_message)
        notification_subject = cur.fetchall()[1]
        logging.info("Notification subject: " + notification_subject)

        # Get attendees email and name
        cur.execute("SELECT email, name FROM attendees")
        attendees = cur.fetchall()
        logging.info("attendees " + attendees)

        # TODO: Loop through each attendee and send an email with a personalized subject

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # Close connection
        conn.commit()

        cur.close()
        conn.close()