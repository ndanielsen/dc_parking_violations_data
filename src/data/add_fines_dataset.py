# -*- coding: utf-8 -*-
import os
import click
import logging
import pandas as pd
import glob
from dotenv import find_dotenv, load_dotenv

@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('fine_csv_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
def main(input_filepath, fine_csv_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('Enriching Dataset')

    fines_df = pd.read_csv(fine_csv_filepath, encoding="utf-8-sig")
    fines_df.columns = [col.lower() for col in fines_df.columns]
    fines_df.drop('shortdesc', axis=1, inplace=True)
    fines_df.rename(columns={'code': 'violation_code'}, inplace=True)

    df = pd.read_csv(input_filepath, sep='\t')
    df = df.merge(fines_df) # on='violation_code')

    columns_to_drop = ['desc']
    df.drop(columns_to_drop, inplace=True, axis=1, errors='ignore')

    logger.info('Saving Enriched Dataset')
    df.to_csv(output_filepath, sep='\t', index=False)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
