# -*- coding: utf-8 -*-
import os
import click
import logging
import pandas as pd
import glob
from dotenv import find_dotenv, load_dotenv


@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
def main(input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('Combining data set from raw data')

    files = glob.glob(os.path.join(input_filepath, '**'))

    parking_violations = [f for f in files if 'parking_violations' in f]

    ### Combine all csvs into one data frame

    logger.info('starting merge')
    list_ = []
    for file_ in parking_violations[:]:
        df = pd.read_csv(file_,index_col=None, header=0)
        filename = file_[len(input_filepath):]
        df['filename'] = filename
        list_.append(df)

    frame = pd.concat(list_)

    # assert frame.filename.nunique() == len(parking_violations)
    frame.columns = [col.lower() for col in frame.columns]
    frame = frame.reset_index(drop=True)
    frame.to_csv(output_filepath, sep='\t', index=False)
    logger.info('Combined all files')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
