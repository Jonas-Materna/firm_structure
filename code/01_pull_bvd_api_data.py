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

def download_pfiles(version, folder, file, output_path=None, chunk_size=1024 * 1024):
    if output_path is None:
        fpath = os.path.join("data", "pulled")
        os.makedirs(fpath, exist_ok=True)
        output_path = os.path.join(fpath, file)
    else:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if os.path.exists(output_path):
        logging.info(
            f"File {output_path} already exists. Touching it to update the timestamp."
        )
        os.utime(output_path)
        return
    
    url = f'{BVD_API_URL}/data/{BVD_API_KEY}/{version}/{folder}/{file}'
    temp_file = output_path + ".part"
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
                        if chunk:
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

    os.rename(temp_file, output_path)


if __name__ == '__main__':
    # List of files to download
    files_to_download = [

        # Get legal, financial, and contact info
        ('2024-12', 'descriptives', 'legal_info.parquet', "data/pulled/legal_info.parquet"),
        ('2024-12', 'financials_global', 'industry_global_financials_and_ratios_eur.parquet', "data/pulled/industry_global_financials_and_ratios_eur.parquet"),
        ('2024-12', 'descriptives', 'contact_info.parquet', "data/pulled/contact_info.parquet"),
        ('2024-12', 'ownership_current', 'controlling_shareholders.parquet', "data/pulled/controlling_shareholders.parquet"),
        ('2024-12', 'descriptives', 'industry_classifications.parquet', "data/pulled/industry_classifications.parquet"),
        
        # Get owner information for all years 
        ('2024-12', 'ownership_current', 'links_current.parquet', "data/pulled/links_2024.parquet"),
        ('2024-12', 'ownership_historic', 'links_2023.parquet', "data/pulled/links_2023.parquet"),
        ('2024-12', 'ownership_historic', 'links_2022.parquet', "data/pulled/links_2022.parquet"),
        ('2024-12', 'ownership_historic', 'links_2021.parquet', "data/pulled/links_2021.parquet"),
        ('2024-12', 'ownership_historic', 'links_2020.parquet', "data/pulled/links_2020.parquet"),
        ('2024-12', 'ownership_historic', 'links_2019.parquet', "data/pulled/links_2019.parquet"),
        ('2024-12', 'ownership_historic', 'links_2018.parquet', "data/pulled/links_2018.parquet"),
        ('2024-12', 'ownership_historic', 'links_2017.parquet', "data/pulled/links_2017.parquet"),
        ('2024-12', 'ownership_historic', 'links_2016.parquet', "data/pulled/links_2016.parquet"),
        ('2024-12', 'ownership_historic', 'links_2015.parquet', "data/pulled/links_2015.parquet"),
        ('2024-12', 'ownership_historic', 'links_2014.parquet', "data/pulled/links_2014.parquet"),
        ('2024-12', 'ownership_historic', 'links_2013.parquet', "data/pulled/links_2013.parquet"),
        ('2024-12', 'ownership_historic', 'links_2012.parquet', "data/pulled/links_2012.parquet"),
        ('2024-12', 'ownership_historic', 'links_2011.parquet', "data/pulled/links_2011.parquet"),
        ('2024-12', 'ownership_historic', 'links_2010.parquet', "data/pulled/links_2010.parquet"),
        ('2024-12', 'ownership_historic', 'links_2009.parquet', "data/pulled/links_2009.parquet"),
        ('2024-12', 'ownership_historic', 'links_2008.parquet', "data/pulled/links_2008.parquet"),
        ('2024-12', 'ownership_historic', 'links_2007.parquet', "data/pulled/links_2007.parquet")
    ]

    

    # Sequentially download each file
    for version, folder, file, output_path in files_to_download:
        download_pfiles(version, folder, file, output_path)

    
