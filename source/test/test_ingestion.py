import os
import shutil


from source.main.ingestion.ingestion_pipeline import get_dataset_from_source

SOURCE_URL = "https://github.com/aavail/ai-workflow-capstone/archive/refs/heads/master.zip"
TEMP_DIR = "./temp/"


class TestIngestion:
    def test_load_dataset(self):
        if not os.path.exists(TEMP_DIR):
            os.mkdir(TEMP_DIR)
        get_dataset_from_source(SOURCE_URL, TEMP_DIR + "zipfile.zip")
        assert os.listdir(TEMP_DIR) == ['zipfile.zip']
        shutil.rmtree(TEMP_DIR)




