import pika
import sys
import webbrowser
import pandas as pd
from pathlib import Path

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """
    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

def send_messages_from_csv(host: str, csv_file: Path):
    """
    Read messages from a multi-column CSV file and send each column's messages
    to the respective RabbitMQ queue.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        csv_file (Path): the path to the CSV file containing messages
    """
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_file)

        # Check if the DataFrame has at least two columns
        if df.shape[1] < 2:
            print("Error: CSV file does not have at least two columns")
            return

        # Define the column names
        column1 = df.columns[0]
        column2 = df.columns[1]

        # Include the header as the first message
        send_message(host, "task_queue2", column1)
        send_message(host, "task_queue3", column2)

        # Iterate over each row in the DataFrame and send the messages
        for index, row in df.iterrows():
            message1 = row[column1]
            message2 = row[column2]
            send_message(host, "task_queue2", str(message1))
            send_message(host, "task_queue3", str(message2))
    except Exception as e:
        print(f"Error reading CSV file: {e}")

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site()

    # Set the CSV file path
    csv_file = Path("bonus.csv")

    # Send the messages from the CSV file to the respective queues
    send_messages_from_csv("localhost", csv_file)
