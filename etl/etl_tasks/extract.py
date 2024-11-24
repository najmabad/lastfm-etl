import logging.config
import os
import tarfile
import urllib.request

from tqdm import tqdm

logging.config.fileConfig(
    fname="config/logging.config.ini", disable_existing_loggers=False
)
logger = logging.getLogger(__name__)


class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)


def download_file(url: str, dest_path: str) -> None:
    """Download a file from a URL to a destination path with a progress bar"""
    logger.info(f"Downloading the data from {url}")

    with DownloadProgressBar(
        unit="B", unit_scale=True, miniters=1, desc=url.split("/")[-1]
    ) as t:
        urllib.request.urlretrieve(url, filename=dest_path, reporthook=t.update_to)

    logger.info("Download completed")


def extract_file(tar_path: str, extract_path: str) -> None:
    """Extract a tar.gz file to a specified path with a progress bar"""
    logger.info("Unzipping the tar.gz file")

    with tarfile.open(tar_path, "r:gz") as tar:
        total = len(tar.getmembers())

        with tqdm(total=total, unit="files", desc="Extracting files") as progress:
            for member in tar.getmembers():
                print(f"Extracting: {member.name}")
                tar.extract(member, path=extract_path)
                progress.update(1)

    logger.info("Files extracted")


def is_data_extracted(data_dir: str, expected_files: list) -> bool:
    """Check if the data directory exists and contains the specific .tsv files.

    Creates the directory if it does not exist. Returns True if both .tsv files are present,
    False otherwise.
    """
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    # check if all required files exist
    return all(os.path.isfile(file_path) for file_path in expected_files)


def extract(data_dir: str, url: str, expected_files: list) -> None:
    """Checks if the data directory contains required .tsv files. If not, downloads and extracts the datasets"""

    if not is_data_extracted(data_dir, expected_files):
        logger.info(f"Required files are missing in {data_dir}")
        filename = url.split("/")[-1]
        file_path = os.path.join(data_dir, filename)
        download_file(url, file_path)
        extract_file(file_path, data_dir)
    else:
        logger.info("All required files are present. Skipping download.")
