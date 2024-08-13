import logging
import os
import zipfile

import requests
from filesplit.split import Split

from utils import connect_preprod, return_file_date

cursor, db = connect_preprod()


format_str = "[%(levelname)s: %(lineno)d] %(message)s"
logging.basicConfig(level=logging.INFO, format=format_str)
logger = logging.getLogger(__name__)





def process_download(filestring: str) -> str:
    """check for file, if not exists, download, unzip and split"""

    # build the url for the request, by appending filestring var to files_url
    files_url = 'https://files.data.gouv.fr/insee-sirene/'
    request_url = files_url + filestring

    # check for
    list_of_files = os.listdir()
    downloaded = 0
    for file in list_of_files:
        if '.zip' in file and file == filestring:
            logger.info('{} has been found'.format(filestring))
            downloaded = 1

    # if a zip file has not been found, download one
    if downloaded == 0:

        # send a request to recieve the file
        r = requests.get(request_url, stream=True, verify=False)

        # if we recieve a 200, that files exists and we can continue
        if r.status_code != 200:
            logger.error('status code: {}'.format(r.status_code))
            raise requests.exceptions.HTTPError

        # create a new file, and write in the data from the request
        with open(filestring, 'wb') as f:
            chunkcount = 0
            for chunk in r.iter_content(chunk_size=50000):
                chunkcount += 1

                f.write(chunk)
                if chunkcount % 100 == 0:
                    logger.info(chunkcount)

            logger.info('file successfully downloaded')
    return filestring
def unzip_file(filestring: str) -> str:
    # unzip the file and delete the zip file
    with zipfile.ZipFile(filestring, 'r') as zip_ref:
        zip_ref.extractall()
        infolist = zip_ref.infolist()
        if infolist:
            unzipped_file_name = infolist[0].filename
        zip_ref.close()

    logger.info('file extracted successfully')
    logger.info('outputfile = {}'.format(unzipped_file_name))

    # remove zip file here
    os.remove(filestring)

    return unzipped_file_name

def split_file(unzipped_file_name: str) -> None:
    # we use filesplit.split Split to divide the file into
    # smaller batches of 50,000 lines
    split = Split(unzipped_file_name, 'fragments')
    split.bylinecount(linecount=50000, includeheader=True)
    os.remove('fragments/manifest')

    # once this is done, we can delete the unzipped csv
    os.remove(unzipped_file_name)


