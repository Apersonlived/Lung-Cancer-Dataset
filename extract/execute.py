import os, sys, requests
from zipfile import ZipFile

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logging, format_time
import time

def download_zip_file(url, output_dir, logger):
    response = requests.get(url, stream=True)
    os.makedirs(output_dir, exist_ok=True)
    if response.status_code == 200:
        filename = os.path.join(output_dir, "downloaded.zip")
        with open(filename, "wb") as f:
            for chunk in response .iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        logger.info(f"Downloaded zip file : {filename}")
        return filename
    else:
        raise Exception(f"Failed to download file. Status code: {response.status_code}")


def extract_zip_file(zip_filename, output_dir, logger):
    with ZipFile(zip_filename, "r") as zip_file:
        zip_file.extractall(output_dir)

    logger.info(f"Extracted files written to: {output_dir}")
    logger.info("Removing the zip file")
    os.remove(zip_filename)

if __name__=="__main__":
    logger = setup_logging("extract.log")

    if len(sys.argv) < 2:
        logger.warning("Extraction path is required")
        logger.info("Exam Usage:")
        logger.info("python3 execute.py /home/ekatabaral/Data/Extraction")
    else:
        try:
            logger.info("Starting Extraction Engine...")
            start = time.time()

            EXTRACT_PATH = sys.argv[1]
            KAGGLE_URL = "https://storage.googleapis.com/kaggle-data-sets/2636109/4510352/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20250820%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20250820T025218Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=90f9fa0053c2940ca35e8a28ddfbae0c8e88898e6c853375e038cdfcdaed3ab7469e53cb176d326f2c83c984d2ee513d1081d4e4870e7609e8ee3cc6e39652423aea29bf7214213d8bee354701797198e2225847cc25f78433fb297d91a051fe98d16a93679f927a7188dcfdfffa118fb0f31f70b8831b011a46016865162d3e30439a6b505701cd82224378f43b381aa6e4f4d0036fae5faac0f59d909c909d0112f1f19b7ca2aed2824393ed7d5a54bbbd56b33000e14c1e289a531cded21f42154b4fcac51a1b6863921e2902c53f2b77c68962a64eb80cdddcbd5fb6d2e58b8113b5e6412316f3a90de010c8bb158df9266c6c4c277411ab48f28aa91888"
            zip_filename = download_zip_file(KAGGLE_URL, EXTRACT_PATH, logger)
            extract_zip_file(zip_filename, EXTRACT_PATH, logger)

            end = time.time()
            logger.info("Extraction successfully completed!!")
            logger.info(f"Total time taken {format_time(end-start)}")
        except Exception as e:
            logger.error(f"Error: {e}")                          