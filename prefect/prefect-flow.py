from prefect import flow, task
from prefect import get_run_logger
import httpx
import boto3
import time
from typing import Dict, List, Tuple

# Task 1 - Populate SQS Queue via API Request
@task(name="populate-sqs-queue")
def populate_sqs_queue(uvaid: str) -> str:
    """
    This task makes a POST request to the API to populate SQS queue with 21 messages.
    This should only be called ONCE per pipeline execution.
    """
    logger = get_run_logger()
    api_url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/qce2dp"

    try:
        logger.info(f"Making API request to populate SQS queue for UVA ID: qce2dp")
        # making a POST request to the API
        response = httpx.post(api_url, timeout=30.0)

        # check if request was successful
        if response.status_code == 200:
            payload = response.json()
            sqs_url = payload.get('sqs_url')
            num_messages = payload.get('num_messages', 0)

            # log the SQS URL
            logger.info(f"SQS Queue populated successfully with URL: {sqs_url}")
            return sqs_url
        
        else:
            # handle non-200 responses
            logger.error(f"Failed to populate SQS Queue. Status code: {response.status_code}")
            logger.error(f"Response content: {response.text}")
            raise Exception(f"Failed to populate SQS Queue. Status code: {response.status_code}")

    # handle timeout errors
    except httpx.TimeoutException as e:
        logger.error(f"Request timed out: {e}")
        raise Exception(f"Request timed out: {e}")
    # handle request errors
    except httpx.RequestError as e:
        logger.error(f"Request error occurred: {e}")
        raise Exception(f"Request error occurred: {e}")
    # handle unexpected errors
    except Exception as e:
        logger.error(f"Unexpected error during API request: {e}")
        raise Exception(f"Unexpected error during API request: {e}")

# Task 2 - Monitor SQS Queue and Collect Messages
@task(name="monitor-sqs-queue")
def monitor_sqs_queue(sqs_url: str, expected_count: int = 21, check_interval: int = 5, max_wait: int = 900) -> None:
     """
     This task monitors the SQS queue by repeadetly checking message availability.
     It uses an intelligent strategy instead of blindly waiting 900 seconds and proceeds to next task when messages are ready.
     """
     logger = get_run_logger()
     sqs = boto3.client('sqs', region_name='us-east-1')
     
     elapsed = 0
     logger.info(f"Starting queue monitoring (checking every {check_interval} seconds)")
     logger.info(f"Waiting for {expected_count} messages to become available")

     while elapsed < max_wait:
        try:
            # we get the queue attributes to check message counts
            attrs = sqs.get_queue_attributes(
                QueueUrl = sqs_url,
                AttributeNames=[
                    'ApproximateNumberOfMessages',
                    'ApproximateNumberOfMessagesNotVisible',
                    'ApproximateNumberOfMessagesDelayed',
                ]
            )

            # extract the three message counts
            available = int(attrs['Attributes'].get('ApproximateNumberOfMessages', 0))
            in_flight = int(attrs['Attributes'].get('ApproximateNumberOfMessagesNotVisible', 0))
            delayed = int(attrs['Attributes'].get('ApproximateNumberOfMessagesDelayed', 0))
            total = available + in_flight + delayed

            # logging the current queue status
            logger.info(f"Queue status - Available: {available}, In-Flight: {in_flight}, Delayed: {delayed}, Total: {total}")
            
            # check if we have all messages in the queue (even if some are delayed)
            if total >= expected_count and delayed == 0:
                logger.info(f"All {expected_count} messages are in queue ({available} available, {delayed} delayed)")
                return
            if total >= expected_count and delayed > 0:
                logger.info(f"All {expected_count} messages are in queue, but there are {delayed} delayed messages")
                logger.info(f"Waiting for delayed messages... ({elapsed}s elapsed)")
            elif total < expected_count:
                logger.warning(f"Only {total}/{expected_count} messages are in queue - waiting longer")
            
            # wait before next check
            time.sleep(check_interval)
            elapsed += check_interval
        
        except Exception as e:
            logger.error(f"Error checking queue attributes: {e}")
            # we continue to monitor despite errors
            time.sleep(check_interval)
            elapsed += check_interval
    
     # if we exit the loop, we have exceeded max_wait time
     logger.warning(f"Max wait time ({max_wait}s) reached - proceeding with available messages")


# Task 3 - Collect, parse, and delete messages
@task(name="collect-messages")
def collect_messages(sqs_url: str, expected_count: int = 21) -> List[Tuple[int, str]]:
    """
    This task collects all messages from the SQS queue, parses their content, and deletes them.
    It fetches messages in batches and extracts order_no and word from MessageAttributes.
    """
    logger = get_run_logger()
    sqs = boto3.client('sqs', region_name='us-east-1')
    
    # Initialize list to store message data as (order_no, word) tuples
    messages_data = []
    collected_count = 0
    max_attempts = 30  # Maximum polling attempts
    
    logger.info(f"Starting message collection (expecting {expected_count} messages)")
    
    # Loop to fetch all messages
    for attempt in range(max_attempts):
        try:
            # Fetch up to 10 messages at a time (SQS maximum per request)
            response = sqs.receive_message(
                QueueUrl=sqs_url,
                MaxNumberOfMessages=10,  # SQS allows max 10 per request
                MessageAttributeNames=['All'],  # Fetch all message attributes
                WaitTimeSeconds=5  # Long polling to reduce empty responses
            )
            
            # Check if 'Messages' key exists in response
            if 'Messages' not in response:
                logger.info(f"No messages in response (attempt {attempt + 1}/{max_attempts})")
                
                # If we've collected all expected messages, we're done
                if collected_count >= expected_count:
                    logger.info(f"All {expected_count} messages collected successfully")
                    break
                
                # Otherwise, continue trying (messages may still be delayed)
                continue
            
            # Check if Messages list is empty
            if len(response['Messages']) == 0:
                logger.info(f"Empty message list (attempt {attempt + 1}/{max_attempts})")
                
                if collected_count >= expected_count:
                    logger.info(f"All {expected_count} messages collected successfully")
                    break
                
                continue
            
            batch_size = len(response['Messages'])
            logger.info(f"Received {batch_size} messages in this batch")
            
            # Process each message in the batch
            for msg in response['Messages']:
                try:
                    # Extract MessageAttributes (this contains the real data, not MessageBody)
                    msg_attrs = msg.get('MessageAttributes', {})
                    
                    # Get order_no (comes as string, must convert to int)
                    order_no_str = msg_attrs.get('order_no', {}).get('StringValue')
                    # Get word (the fragment we need)
                    word = msg_attrs.get('word', {}).get('StringValue')
                    
                    # Validate that both fields exist
                    if order_no_str is None or word is None:
                        logger.warning(f"Message missing required attributes (order_no or word)")
                        continue
                    
                    # Convert order_no from string to integer
                    order_no = int(order_no_str)
                    
                    # Store as tuple to keep order_no and word paired together
                    messages_data.append((order_no, word))
                    collected_count += 1
                    
                    logger.info(f"Collected message {collected_count}/{expected_count} - Order: {order_no}, Word: '{word}'")
                    
                    # Get the ReceiptHandle (required for deletion)
                    receipt_handle = msg.get('ReceiptHandle')
                    
                    if receipt_handle:
                        # Delete the message from queue (cleanup)
                        sqs.delete_message(
                            QueueUrl=sqs_url,
                            ReceiptHandle=receipt_handle
                        )
                        logger.info(f"Deleted message with order_no: {order_no}")
                    else:
                        logger.warning(f"No ReceiptHandle for message with order_no: {order_no}")
                    
                except ValueError as e:
                    # Handle error converting order_no to int
                    logger.error(f"Error converting order_no to integer: {e}")
                    continue
                    
                except Exception as e:
                    # Handle any other errors processing individual message
                    logger.error(f"Error processing message: {e}")
                    continue
            
            # Check if we've collected all expected messages
            if collected_count >= expected_count:
                logger.info(f"Successfully collected all {expected_count} messages")
                break
                
        except Exception as e:
            # Handle errors receiving messages
            logger.error(f"Error receiving messages (attempt {attempt + 1}/{max_attempts}): {e}")
            continue
    
    # Final validation
    if collected_count < expected_count:
        logger.warning(f"Only collected {collected_count}/{expected_count} messages")
        raise Exception(f"Failed to collect all messages. Got {collected_count}/{expected_count}")
    
    logger.info(f"Message collection complete: {collected_count} messages stored as (order_no, word) tuples")
    
    return messages_data


# Task 4 - Reassemble Messages and Submit Answer
@task(name="reassemble-messages")
def reassemble_phrase(messages: List[Tuple[int, str]]) -> str:
    """
    This task assembles the collected messages fragments into a complete phrase.
    It sorts the messages by order_no and concatenates the words in the correct order.
    """
    logger = get_run_logger()

    try:
        logger.info(f"Starting phrase reassembly with {len(messages)} message fragments")
        # sort messages by order_no (first element of tuple)
        sorted_message = sorted(messages, key=lambda x: x[0])
        # log the sorted order for verification
        logger.info(f"Sorted message order:")
        for order_no, word in sorted_message:
            logger.info(f"Position {order_no}: {word}")

        # extract just the words (second element of each tuple)
        words = [word for order_no, word in sorted_message]
        # join words with spaces to create complete phrase
        complete_phrase = ' '.join(words)

        logger.info(f"Phrase reassembled successfully")
        logger.info(f"Complete phrase: '{complete_phrase}'")

        return complete_phrase
    
    except Exception as e:
        logger.error(f"Error reassembling phrase: {e}")
        raise Exception(f"failed to reassemble phrase: {e}")

# Task 5 - Submit Answer to Neal's Queue
@task(name="submit-answer")
def submit_answer(uvaid: str, phrase: str, platform: str = "prefect") -> bool:
    """
    This task submits the reassembled phrase to Neal's queue for grading.
    It sends uvaid, phrase, and platform as message attributes.
    """
    logger = get_run_logger()
    sqs = boto3.client('sqs', region_name='us-east-1')

    # Neal's queue URL
    submission_queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

    try:
        logger.info(f"Submitting answer to Neal's queue")
        logger.info(f"UVA ID: {uvaid}")
        logger.info(f"Phrase: '{phrase}'")
        logger.info(f"Platform: {platform}")

        # send message with required attributes
        response = sqs.send_message(
            QueueUrl = submission_queue_url,
            MessageBody = f"Submission from {uvaid}",
            MessageAttributes = {
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

        # check if response was successful
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            message_id = response.get('MessageId', 'unknown')
            logger.info(f"Answer submitted successfully to Neal's queue with message ID: {message_id}")
            return True
        else:
            status_code = response['ResponseMetadata']['HTTPStatusCode']
            logger.error(f"Submission failed with HTTP Status Code: {status_code}")
            raise Exception(f"Submission returned non-200 status code: {status_code}")

    except Exception as e:
        logger.error(f"Error submitting answer: {e}")
        raise Exception(f"failed to submit answer: {e}")
        

# MAIN FLOW
@flow(name="sqs-message-assembler")
def main_flow(uvaid: str):
    """
    Main Prefect flow to retrieve and reassemble scattered SQS messages
    """
    logger = get_run_logger()
    logger.info("Starting SQS Message Assembler Pipeline")

    # Task 1 - Populate SQS Queue
    sqs_url = populate_sqs_queue(uvaid)
    logger.info("Pipeline Task 1 completed successfully")

    # Task 2 - Monitor SQS Queue
    monitor_sqs_queue(sqs_url)
    logger.info("Pipeline Task 2 completed successfully")

    # Task 3 - Collect, parse, and delete messages
    messages = collect_messages(sqs_url)
    logger.info(f"Pipeline Task 3 completed successfully - collected {len(messages)} messages")

    # Task 4 - Reassemble messages into complete phrase
    complete_phrase = reassemble_phrase(messages)
    logger.info(f"Pipeline Task 4 completed successfully - reassembled phrase: '{complete_phrase}'")

    # Task 5 - Submit answer to Neal's queue
    success = submit_answer(uvaid, complete_phrase, platform="prefect")
    if success:
        logger.info(f"Pipeline Task 5 completed successfully - answer submitted to Neal's queue")
    
    logger.info("End-to-end pipeline completed successfully")

if __name__ == "__main__":
    UVAID = "qce2dp"
    main_flow(uvaid=UVAID)