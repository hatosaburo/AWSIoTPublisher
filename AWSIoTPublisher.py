import os
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import logging_util
logger = logging_util.get_logger(__name__)

class AWSIoTPublisher:

    def __init__(self, config_awsiot, clientId = "hemsController"):
        self.__host = config_awsiot['host']
        self.__rootCAPath = os.path.join(os.path.dirname(__file__), config_awsiot['root_ca'])
        self.__certificatePath = os.path.join(os.path.dirname(__file__), config_awsiot['cert_pem'])
        self.__privateKeyPath = os.path.join(os.path.dirname(__file__), config_awsiot['private_key'])
        self.__clientId = clientId
        # Port defaults
        port = 8883

        # Init AWSIoTMQTTClient
        self.__myAWSIoTMQTTClient = AWSIoTMQTTClient(self.__clientId)
        self.__myAWSIoTMQTTClient.configureEndpoint(self.__host, port)
        self.__myAWSIoTMQTTClient.configureCredentials(self.__rootCAPath, self.__privateKeyPath, self.__certificatePath)

        # AWSIoTMQTTClient connection configuration
        self.__myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
        self.__myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        self.__myAWSIoTMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
        self.__myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
        self.__myAWSIoTMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec

    def publishMessages(self, file_list):
        import json

        try:
            self.__myAWSIoTMQTTClient.connect()
        except Exception as e:
            logger.warning('AWS connect failed : {0}'.format(e))


        for file in file_list:
            try:
                with open(file) as f:
                    json_data = json.load(f)

                    message = json.dumps(json_data['message'])

                    try:
                        self.__myAWSIoTMQTTClient.publish(json_data['topic'], message, 1)
                    except Exception as e:
                        logger.warning('AWSIoT publish failed : {0}'.format(e))
                        break
                    
                    logger.info('Published topic %s: %s' % (json_data['topic'], message))
                       
                os.remove(file)
            except Exception as e:
                logger.warning('Error occured at {0} : {1}'.format(file, e))
        
        self.__myAWSIoTMQTTClient.disconnect()
