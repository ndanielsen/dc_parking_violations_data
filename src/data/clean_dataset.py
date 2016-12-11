# -*- coding: utf-8 -*-
import os
import click
import logging
import pandas as pd
import glob
from dotenv import find_dotenv, load_dotenv

def mil_to_time(x):
    "Convert messy issue_time to datetime object based upon length of issue_time string"
    if x == 'nan':
        return '00:00:00.000Z'

    x = x.split('.')[0]
    lg = len(x)

    if lg == 4:
        t = x[:2] + ':' + x[2:] + ':00.000Z'

    elif lg == 3:
        t = '0' + x[0] + ':' + x[1:] + ':00.000Z'

    elif lg == 2:
        t = '0' + '0' + ':' + x + ':00.000Z'

    elif lg == 1:
        t = '0' + '0' + ':' + '0' + x + ':00.000Z'

    else:
        t = '00:00.000Z'

    # correction for timedate if one element is greater than 5.
    # double check this
    if int(t[3]) > 5:
        t = t[:2]+ ':' + '5' + t[4:]

    return t


@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
def main(input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('Cleaning Raw Combined Dataset')

    df = pd.read_csv(input_filepath, sep='\t')

    logger.info('Processing datetimes')
    df['issue_time_military'] = df.issue_time.apply(str).apply(mil_to_time)
    dates = df.ticket_issue_date.str[:10] + 'T'
    df['ticket_issue_datetime'] = dates + df.issue_time_military

    df['holiday'] = df.holiday != 0

    columns_to_drop = ['day_of_week', 'month_of_year', 'week_of_year', 'issue_time', 'issue_time_military', 'ticket_issue_date' ]
    df.drop(columns_to_drop, inplace=True, axis=1, errors='ignore')

    df.streetsegid.fillna(0, inplace=True)

    logger.info('Dumping entire cleaned csv to: {0}'.format(output_filepath))
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
