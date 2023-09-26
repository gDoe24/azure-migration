import logging
import azure.functions as func
import psycopg2
import sys
import os
from datetime import datetime
# from sendgrid import SendGridAPIClient
# from sendgrid.helpers.mail import Mail, Email, To, Content

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
        cur.execute("SELECT message, subject FROM notification WHERE id = %s", (notification_id,))
        notification = cur.fetchone()
        notification_message = notification[0]
        logging.info('Notification message: %s', notification_message)
        notification_subject = notification[1]
        logging.info('Notification subject: %s', notification_subject)

        # Get attendees email and name
        cur.execute("SELECT email, first_name FROM attendee;")
        attendees = cur.fetchall()

        # Loop through each attendee and send an email with a personalized subject
        for attendee in attendees:
            # attendee_email = To(attendee[0])
            # attendee_name = attendee[1]
            # subject = f'{attendee[1]}: {notification_subject}'
            # content = Content("text/plain", f"Hi {attendee_name}, \n {notification_message}")
            # Mail(Email("techconfadmin@techconf.com", ))
            # print(attendee, attendee_email, subject, content)
            logging.info("attendee: %s", str(attendee[1]))

        # Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        completed_date = datetime.utcnow()
        status = f'Notified {len(attendees)} attendees.'
        cur.execute("UPDATE notification SET status = %s, completed_date = %s WHERE id = %s", 
                    (status, completed_date, notification_id))
        conn.commit()
        

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # Close connection
        cur.close()
        conn.close()