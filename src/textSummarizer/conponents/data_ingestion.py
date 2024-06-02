import os
import urllib.request as request
import zipfile
import logging
from src.textSummarizer.logging import logger
from src.textSummarizer.utils.common import get_size
from pathlib import Path
from src.textSummarizer.entity import DataIngestionConfig

class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config


    
    def download_file(self):
        if not os.path.exists(self.config.local_data_file):
            filename, headers = request.urlretrieve(
                url = self.config.source_URL,
                filename = self.config.local_data_file
            )
            logger.info(f"{filename} download! with following info: \n{headers}")
        else:
            logger.info(f"File already exists of size: {get_size(Path(self.config.local_data_file))}")  

        
    
    def download_file(self):
        # Implement download logic if needed
        pass

    def extract_zip_file(self):
        try:
            logging.info(f"Attempting to extract file: {self.config.local_data_file}")
            if not os.path.isfile(self.config.local_data_file):
                logging.error(f"File does not exist: {self.config.local_data_file}")
                return
            
            # Check if the file is actually a zip file
            if not zipfile.is_zipfile(self.config.local_data_file):
                logging.error(f"File is not a zip file: {self.config.local_data_file}")
                raise zipfile.BadZipFile(f"File is not a zip file: {self.config.local_data_file}")

            with zipfile.ZipFile(self.config.local_data_file, 'r') as zip_ref:
                zip_ref.extractall(self.config.extract_to)
            logging.info(f"File extracted successfully to {self.config.extract_to}")
        except zipfile.BadZipFile as e:
            logging.error(e)
            raise e

# Example usage
if __name__ == "__main__":
    config = {
        "local_data_file": "artifacts/data_ingestion/data.zip",
        "extract_to": "artifacts/data_ingestion/"
    }
    data_ingestion = DataIngestion(config)
    data_ingestion.extract_zip_file()

