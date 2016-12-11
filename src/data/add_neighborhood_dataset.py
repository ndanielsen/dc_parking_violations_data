# -*- coding: utf-8 -*-
import os
import click
import logging
import pandas as pd
import geopandas as gpd
import shapely
import numpy as np
import matplotlib.pyplot as plt
from dotenv import find_dotenv, load_dotenv

@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('enrichment_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
def main(input_filepath, enrichment_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('Loading Dataset')

    df = pd.read_csv(input_filepath, sep='\t', parse_dates=['ticket_issue_datetime'])
    neighborhood_clusters = gpd.read_file(enrichment_filepath)

    logger.info('Converting Point Fields')
    dc_df = gpd.GeoDataFrame(df, geometry=df.apply(
            lambda srs: shapely.geometry.Point(srs['x'], srs['y']), axis='columns'
        ))

    # use closure to create custom higher level functions here
    def assign_geo_designation_tract(row):
        """ Takes in a geopandas frame and creates a new column categorizing which geoshape a row contains"""
        bools = [geom.contains(row['geometry']) for geom in neighborhood_clusters['geometry']]
        if True in bools:
            return neighborhood_clusters.iloc[bools.index(True)]['NBH_NAMES']
        else:
            return np.nan

    logger.info('Adding Geo-labeling')
    dc_df['neighborhood'] = dc_df.apply(assign_geo_designation_tract, axis='columns')

    logger.info('Saving Enriched Dataset')
    dc_df.to_csv(output_filepath, sep='\t', index=False, chunksize=250000)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
