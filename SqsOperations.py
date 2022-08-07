import time

import boto3
import json

class SqsOperations:
    def __init__(self):
        self.sqs = boto3.resource('sqs')
        self.input_queue = self.sqs.get_queue_by_name(QueueName='CrawlerInput')
        self.output_queue = self.sqs.get_queue_by_name(QueueName='CrawlerOutput')
        return


    def insert_message_into_queue(self,message):
        try:
            response = self.input_queue.send_message(MessageBody= json.dumps(message))
            return response
        except Exception as e:
            print ("Exception writing in into sqs:- ",e)
            return None


    def get_message_from_queue(self):
        final_res = []
        try:
            try_count = 1
            while try_count <10:

                messages = self.input_queue.receive_messages(
                    MessageAttributeNames=['All'],
                    MaxNumberOfMessages=10
                )
                if type(messages) ==list:
                    if len(messages) >0:
                        final_res.extend(messages)
                    else:
                        time.sleep(1)
                        try_count+=1
                else:
                    time.sleep(1)
                    try_count +=1

            return final_res
        except Exception as e:
            print ("Exception in reading msgs from queue:- ",e)
            return []



