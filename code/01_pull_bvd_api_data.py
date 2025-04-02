import json
import os
from urllib import request
import humanize.filesize
import requests
from tqdm import tqdm
import humanize

from dotenv import load_dotenv
load_dotenv('firm_structure.env')

BVD_API_KEY = os.environ["BVD_API_KEY"]
BVD_API_URL = "https://trr266.wiwi.hu-berlin.de/bvd"

import logging
if os.environ["LOG_FILE"] == "stdout" or os.environ["LOG_FILE"] == "":
    log_file = None
else:
    log_file = os.environ["LOG_FILE"]

logging.basicConfig(
    filename=log_file, level=logging.INFO,
    format="%(levelname)s [%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

versions = json.load(request.urlopen(f'{BVD_API_URL}/versions/{BVD_API_KEY}'))

def download_pfiles(version, folder, file, chunk_size=1024 * 1024):
    fpath = os.path.join("data", "pulled")
    fname = os.path.join(fpath, file)

    if os.path.exists(fname):
        logging.info(
            f"File {file} already exists. Touching it to update the timestamp."
        )
        os.utime(fname)
        return
    
    url = f'{BVD_API_URL}/data/{BVD_API_KEY}/{version}/{folder}/{file}'
    temp_file = fname + ".part"  #
    resume_header = {}
    downloaded_size = 0
    if os.path.exists(temp_file):
        downloaded_size = os.path.getsize(temp_file)
        resume_header = {"Range": f"bytes={downloaded_size}-"} 
        logging.info(
            f"Incomplete download of {file} found. "
            f"Trying to continue after {humanize.naturalsize(downloaded_size)}..."
        )

    while True:
        with requests.get(url, headers=resume_header, stream=True) as response:
            response.raise_for_status()
            total_size = int(response.headers.get("content-length", 0)) + downloaded_size

            with tqdm(
                total=total_size, initial=downloaded_size, unit='B', 
                unit_scale=True, desc="Downloading"
            ) as progress_bar:            
                with open(temp_file, "ab") as f:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:  # Filter out keep-alive chunks
                            f.write(chunk)
                            progress_bar.update(len(chunk))
        downloaded_size = os.path.getsize(temp_file)
        if downloaded_size == total_size:
            break
        else:
            logging.warning(
                f"Download of {file} stopped after downloading "
                f"{humanize.naturalsize(downloaded_size)}. Trying to continue..."
            )
            resume_header = {"Range": f"bytes={downloaded_size}-"} 

    # Rename the file after successful download
    os.rename(temp_file, fname)
    logging.info(f"Download completed: {file}")


if __name__ == '__main__':
    # List of files to download
    files_to_download = [

        #('2024-12', 'descriptives', 'legal_info.parquet')
        #('2024-12', 'financials_global', 'industry_global_financials_and_ratios_eur.parquet'),
        #('2024-12', 'descriptives', 'contact_info.parquet'),
        #('2024-12', 'ownership_current', 'controlling_shareholders.parquet'),



        # Historic owner information
        ('2024-12', 'ownership_historic', 'links_2021.parquet'),
        ('2024-12', 'ownership_historic', 'links_2020.parquet'),
        ('2024-12', 'ownership_historic', 'links_2019.parquet'),
        ('2024-12', 'ownership_historic', 'links_2018.parquet'),
        ('2024-12', 'ownership_historic', 'links_2017.parquet'),
        ('2024-12', 'ownership_historic', 'links_2016.parquet'),
        ('2024-12', 'ownership_historic', 'links_2015.parquet'),
        ('2024-12', 'ownership_historic', 'links_2014.parquet'),
        ('2024-12', 'ownership_historic', 'links_2013.parquet')
        ('2024-12', 'ownership_historic', 'links_2012.parquet'),
        ('2024-12', 'ownership_historic', 'links_2011.parquet'),
        ('2024-12', 'ownership_historic', 'links_2010.parquet')
    ]

    # Sequentially download each file
    for version, folder, file in files_to_download:
        download_pfiles(version, folder, file)

    
