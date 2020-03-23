import os
import time
import schedule
import AWSIoTPublisher
import logging_util

logging_util.configure_logging('logging_config_main.yaml')
logger = logging_util.get_logger(__name__)

CONFIG_FILE = 'config.yaml'

def publishScheduleJob(aws_iot, data_dir):
    import glob
    file_list = glob.glob(os.path.join(data_dir, '*.json')) 
    aws_iot.publishMessages(file_list)

def load_config(config_path):
    import yaml
    with open(config_path, 'r') as f:
        return yaml.safe_load(f.read())

def main():
    config = load_config(os.path.join(os.path.dirname(__file__), CONFIG_FILE))
    aws_iot = AWSIoTPublisher.AWSIoTPublisher(config['awsiot'])

    logger.info('Start AWSIoTPublisher')
    # AWSIoT(DynamoDB)にアップするスケジュール
    schedule.every(config['general']['interval']).minutes.at(":00").do(publishScheduleJob, aws_iot, config['general']['data_dir'])

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()