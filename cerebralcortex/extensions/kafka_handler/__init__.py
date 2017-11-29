import argparse

def get_args():
    parser = argparse.ArgumentParser(description='CerebralCortex Kafka Message Handler.')
    parser.add_argument("-c", "--config_filepath", help="Configuration file path", required=True)
    parser.add_argument("-d", "--data_dir", help="Directory path where all the gz files are stored by API-Server",
                        required=True)
    parser.add_argument("-bd", "--batch_duration",
                        help="How frequent kafka messages shall be checked (duration in seconds)", required=False)
    parser.add_argument("-b", "--broker_list",
                        help="Kafka brokers ip:port. Use comma if there are more than one broker. (e.g., 127.0.0.1:9092)",
                        required=False)

    return vars(parser.parse_args())