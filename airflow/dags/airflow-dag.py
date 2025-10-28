from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
import httpx
import boto3
import time
from typing import List, Tuple

# airflow DAG definition
@dag(
    dag_id='sqs-message-assembler-airflow',
    description="Data Project 2 - Airflow DAG",
    schedule=None,
    start_date=datetime(2025, 10, 26),
    catchup=False,
    tags=['sqs', 'data-pipeline', 'ds3022']
)

def sqs_message_assembler():
    """
    This is the main Airflow DAG to retrieve and reassemble scattered SQS messages.
    It performs the same operations as the Prefect flow.
    """
    
    @task(task_id='populate_sqs_queue')
    def populate_sqs_queue(uvaid: str) -> str:
        """
        This task makes a POST request to the API to populate SQS queue with 21 messages.
        This should only be called once per DAG execution.
        """
        # get airflow's task instance logger
        ti = get_current_context()['ti']
        log = ti.log

        api_url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uvaid}"

        try:
            log.info(f"Making API request to populate SQS queue for UVA ID: {uvaid}")

            # makeing a POST request to the API
            response = httpx.post(api_url, timeout=30.0)
            # check if request was successful
            if response.status_code == 200:
                payload = response.json()
                sqs_url = payload.get('sqs_url')

                log.info(f"SQS Queue populated successfully with URL: {sqs_url}")
                log.info("21 messages sent with random delays between 30 and 900 seconds")
                return sqs_url
            else:
                log.error(f"Failed to populate SQS queue: HTTP Status Code {response.status_code}")
                raise Exception(f"Failed to populate SQS queue: Status code: {response.status_code}")
        
        except httpx.TimeoutException as e:
            log.error(f"Request timed out: {e}")
            raise Exception(f"Request timed out: {e}")
            
        except httpx.RequestError as e:
            log.error(f"Request error occurred: {e}")
            raise Exception(f"Request error occurred: {e}")
            
        except Exception as e:
            log.error(f"Unexpected error during API request: {e}")
            raise 

    
    @task(task_id="monitor_sqs_queue")
    def monitor_sqs_queue(sqs_url: str, expected_count: int = 21, check_interval: int = 5, max_wait: int = 900) -> None:
        """
        Monitors the SQS queue by repeatedly checking message availability.
        Uses an intelligent strategy instead of blindly waiting 900 seconds.
        """
        # Get Airflow's task instance logger
        ti = get_current_context()['ti']
        log = ti.log
        
        sqs = boto3.client('sqs', region_name='us-east-1')
        
        elapsed = 0
        log.info(f"Starting queue monitoring (checking every {check_interval} seconds)")
        log.info(f"Waiting for {expected_count} messages to become available")
        
        # Repeatedly monitor the queue
        while elapsed < max_wait:
            try:
                # Get queue attributes to check message counts
                attrs = sqs.get_queue_attributes(
                    QueueUrl=sqs_url,
                    AttributeNames=[
                        'ApproximateNumberOfMessages',
                        'ApproximateNumberOfMessagesNotVisible',
                        'ApproximateNumberOfMessagesDelayed',
                    ]
                )
                
                # Extract the three message counts
                available = int(attrs['Attributes'].get('ApproximateNumberOfMessages', 0))
                in_flight = int(attrs['Attributes'].get('ApproximateNumberOfMessagesNotVisible', 0))
                delayed = int(attrs['Attributes'].get('ApproximateNumberOfMessagesDelayed', 0))
                total = available + in_flight + delayed
                
                # Log current queue status
                log.info(f"Queue status - Available: {available}, In-Flight: {in_flight}, Delayed: {delayed}, Total: {total}")
                
                # Check if all messages are available
                if total >= expected_count and delayed == 0:
                    log.info(f"All {expected_count} messages are in queue ({available} available, {delayed} delayed)")
                    return
                
                if total >= expected_count and delayed > 0:
                    log.info(f"All {expected_count} messages in queue, but {delayed} still delayed")
                    log.info(f"Waiting for delayed messages... ({elapsed}s elapsed)")
                elif total < expected_count:
                    log.warning(f"Only {total}/{expected_count} messages in queue - waiting longer")
                
                # Wait before next check
                time.sleep(check_interval)
                elapsed += check_interval
                
            except Exception as e:
                log.error(f"Error checking queue attributes: {e}")
                time.sleep(check_interval)
                elapsed += check_interval
        
        # If we exit the loop, we've exceeded max_wait
        log.warning(f"Max wait time ({max_wait}s) reached - proceeding with available messages")

    
    @task(task_id="collect_messages")
    def collect_messages(sqs_url: str, expected_count: int = 21) -> List[Tuple[int, str]]:
        """
        Collects all messages from the SQS queue, parses their content, and deletes them.
        Fetches messages in batches and extracts order_no and word from MessageAttributes.
        """
        # Get Airflow's task instance logger
        ti = get_current_context()['ti']
        log = ti.log
        
        sqs = boto3.client('sqs', region_name='us-east-1')
        
        # Initialize list to store message data as (order_no, word) tuples
        messages_data = []
        collected_count = 0
        max_attempts = 30
        
        log.info(f"Starting message collection (expecting {expected_count} messages)")
        
        # Loop to fetch all messages
        for attempt in range(max_attempts):
            try:
                # Fetch up to 10 messages at a time
                response = sqs.receive_message(
                    QueueUrl=sqs_url,
                    MaxNumberOfMessages=10,
                    MessageAttributeNames=['All'],
                    WaitTimeSeconds=5
                )
                
                # Check if 'Messages' key exists in response
                if 'Messages' not in response:
                    log.info(f"No messages in response (attempt {attempt + 1}/{max_attempts})")
                    
                    if collected_count >= expected_count:
                        log.info(f"All {expected_count} messages collected successfully")
                        break
                    continue
                
                # Check if Messages list is empty
                if len(response['Messages']) == 0:
                    log.info(f"Empty message list (attempt {attempt + 1}/{max_attempts})")
                    
                    if collected_count >= expected_count:
                        log.info(f"All {expected_count} messages collected successfully")
                        break
                    continue
                
                batch_size = len(response['Messages'])
                log.info(f"Received {batch_size} messages in this batch")
                
                # Process each message in the batch
                for msg in response['Messages']:
                    try:
                        # Extract MessageAttributes
                        msg_attrs = msg.get('MessageAttributes', {})
                        
                        # Get order_no and word
                        order_no_str = msg_attrs.get('order_no', {}).get('StringValue')
                        word = msg_attrs.get('word', {}).get('StringValue')
                        
                        # Validate that both fields exist
                        if order_no_str is None or word is None:
                            log.warning(f"Message missing required attributes")
                            continue
                        
                        # Convert order_no from string to integer
                        order_no = int(order_no_str)
                        
                        # Store as tuple to keep order_no and word paired
                        messages_data.append((order_no, word))
                        collected_count += 1
                        
                        log.info(f"Collected message {collected_count}/{expected_count} - Order: {order_no}, Word: '{word}'")
                        
                        # Delete the message after processing
                        receipt_handle = msg.get('ReceiptHandle')
                        
                        if receipt_handle:
                            sqs.delete_message(
                                QueueUrl=sqs_url,
                                ReceiptHandle=receipt_handle
                            )
                            log.info(f"Deleted message with order_no: {order_no}")
                        
                    except ValueError as e:
                        log.error(f"Error converting order_no to integer: {e}")
                        continue
                        
                    except Exception as e:
                        log.error(f"Error processing message: {e}")
                        continue
                
                # Check if we've collected all expected messages
                if collected_count >= expected_count:
                    log.info(f"Successfully collected all {expected_count} messages")
                    break
                    
            except Exception as e:
                log.error(f"Error receiving messages (attempt {attempt + 1}/{max_attempts}): {e}")
                continue
        
        # Final validation
        if collected_count < expected_count:
            log.warning(f"Only collected {collected_count}/{expected_count} messages")
            raise Exception(f"Failed to collect all messages. Got {collected_count}/{expected_count}")
        
        log.info(f"Message collection complete: {collected_count} messages stored")
        
        return messages_data
    
    @task(task_id="reassemble_phrase")
    def reassemble_phrase(messages: List[Tuple[int, str]]) -> str:
        """
        Reassembles the collected message fragments into a complete phrase.
        Sorts messages by order_no and joins words with spaces.
        """
        # Get Airflow's task instance logger
        ti = get_current_context()['ti']
        log = ti.log
        
        try:
            log.info(f"Starting phrase reassembly with {len(messages)} message fragments")
            
            # Sort messages by order_no
            sorted_messages = sorted(messages, key=lambda x: x[0])
            
            # Log the sorted order for verification
            log.info("Sorted message order:")
            for order_no, word in sorted_messages:
                log.info(f"  Position {order_no}: '{word}'")
            
            # Extract just the words
            words = [word for order_no, word in sorted_messages]
            
            # Join words with spaces
            complete_phrase = ' '.join(words)
            
            log.info(f"Phrase reassembled successfully")
            log.info(f"Complete phrase: '{complete_phrase}'")
            
            return complete_phrase
            
        except Exception as e:
            log.error(f"Error reassembling phrase: {e}")
            raise
    
    
    @task(task_id="submit_answer")
    def submit_answer(uvaid: str, phrase: str, platform: str = "airflow") -> bool:
        """
        Submits the reassembled phrase to the instructor's SQS queue for grading.
        Sends uvaid, phrase, and platform="airflow" as MessageAttributes.
        """
        # Get Airflow's task instance logger
        ti = get_current_context()['ti']
        log = ti.log
        
        sqs = boto3.client('sqs', region_name='us-east-1')
        
        # Instructor's submission queue URL
        submission_queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
        
        try:
            log.info(f"Submitting answer to instructor queue")
            log.info(f"  UVA ID: {uvaid}")
            log.info(f"  Platform: {platform}")
            log.info(f"  Phrase: '{phrase}'")
            
            # Send message with required attributes
            response = sqs.send_message(
                QueueUrl=submission_queue_url,
                MessageBody=f"Airflow submission from {uvaid}",
                MessageAttributes={
                    'uvaid': {
                        'DataType': 'String',
                        'StringValue': uvaid
                    },
                    'phrase': {
                        'DataType': 'String',
                        'StringValue': phrase
                    },
                    'platform': {
                        'DataType': 'String',
                        'StringValue': platform
                    }
                }
            )
            
            # Check response for success
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                message_id = response.get('MessageId', 'unknown')
                log.info(f"Submission successful!")
                log.info(f"  HTTP Status: 200")
                log.info(f"  Message ID: {message_id}")
                return True
            else:
                status_code = response['ResponseMetadata']['HTTPStatusCode']
                log.error(f"Submission failed with HTTP status: {status_code}")
                raise Exception(f"Submission returned non-200 status: {status_code}")
                
        except Exception as e:
            log.error(f"Error submitting answer: {e}")
            raise
    

# DAG TASK FLOW

    # Define UVA ID
    UVAID = "qce2dp"
    
    # Task 1: Populate SQS Queue
    sqs_url = populate_sqs_queue(UVAID)
    
    # Task 2: Monitor SQS Queue
    monitor_task = monitor_sqs_queue(sqs_url)
    
    # Task 3: Collect messages
    messages = collect_messages(sqs_url)
    
    # Task 4: Reassemble phrase
    complete_phrase = reassemble_phrase(messages)
    
    # Task 5: Submit answer with platform="airflow"
    submit_task = submit_answer(UVAID, complete_phrase, platform="airflow")
    
    # Set task dependencies
    sqs_url >> monitor_task >> messages >> complete_phrase >> submit_task


# Instantiate the DAG
dag_instance = sqs_message_assembler()