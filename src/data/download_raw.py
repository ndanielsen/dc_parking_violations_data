# -*- coding: utf-8 -*-
import os
import click
import logging
import json
import requests
import time
from dotenv import find_dotenv, load_dotenv


@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
def main(input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('Downloading data set from DC Open data')

    with open(input_filepath, 'r') as f:
        parking_violations = json.load(f)

    for fullname, csv in parking_violations.items():
        download_file =  csv + '.csv'
        local_filename = '_'.join(name.lower() for name in fullname.split() ) + '.csv'
        local_filename = os.path.join(output_filepath, local_filename)
        if not os.path.isfile(local_filename):
            time.sleep(5)
            r = requests.get(download_file)
            if not b'"status":"Processing","generating":{}' in r.content:
                with open(local_filename, 'wb') as f:
                    f.write(r.content)
                logger.info(local_filename)
            else:
                logger.warning('Cannot download {0}'.format(local_filename))


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
