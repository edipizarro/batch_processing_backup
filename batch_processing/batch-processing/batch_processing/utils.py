from datetime import datetime, timedelta
import logging
import os

class Utils:
    @staticmethod
    def date_to_mjd(date: datetime) -> str:
        """
        Receives a date and transforms it to MJD.
        Possible date formats:
        https://docs.python.org/3/library/datetime.html#format-codes

        :param date:  date string
        :param format: string indicating date format
        :return: the date as MJD
        """
        reference_date = datetime(1858, 11, 17)
        mjd = (date - reference_date).days
        return str(mjd)

    @staticmethod
    def str_to_date(date: str, format: str ="%Y/%m/%d"):
        return datetime.strptime(date, format)
    
    @staticmethod
    def dates_between_generator(start_date, end_date):
        for n in range(int((end_date - start_date).days) + 1):
            yield start_date + timedelta(n)

    @staticmethod
    def configure_logger():
        # Configure logger
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        # Create a stream handler and set its level to INFO
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)

        # Create a formatter and set it to the handler
        formatter = logging.Formatter('%(levelname)s - %(message)s')
        stream_handler.setFormatter(formatter)

        # Add the stream handler to the logger
        logger.addHandler(stream_handler)

        return logger

    @staticmethod
    def _init_path(directory: str):
        # Check if the directory exists, create it if not
        if not os.path.exists(directory):
            os.makedirs(directory)

    @staticmethod
    def _rm_directory_or_file( path):
        if os.path.isfile(path):
            os.remove(path)
        elif os.path.isdir(path):
            # Get the list of files in the folder
            entries = os.listdir(path)

            # Iterate through the files and remove them
            for entry in entries:
                entry_path = os.path.join(path, entry)
                try:
                    if os.path.isfile(entry_path):
                        os.remove(entry_path)
                    elif os.path.isdir(entry_path):
                        # Recursively call _rm_directory_or_file for subdirectories
                        Utils._rm_directory_or_file(entry_path)

                except Exception as e:
                    print(f"Error: {e}")

            # rm folder
            os.rmdir(path)