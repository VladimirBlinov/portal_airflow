from finam import Exporter, Market, Timeframe
import pandas as pd
import logging
from csv import writer


def get_instruments(market=25):
    exporter = Exporter()
    market_instruments = exporter.lookup(market=market)
    return market_instruments


def get_extracted(file_path):
    try:
        df = pd.read_csv(file_path, sep=';', header=None)
        return df
    except Exception as e:
        logging.info('Exception:', e)
        return pd.DataFrame()


def compare_df(ref_df, new_df):
    set_ref = set(ref_df.iloc[:, 2])
    set_new = set(new_df.iloc[:, 2])
    set_diff = set_new - set_ref
    diff_df = new_df[new_df.iloc[:, 2].isin(set_diff)]
    logging.info(f'{diff_df}')
    return diff_df


def get_daily_data(row, start_time, end_time):
    TIMEFRAME_ID = 7
    ticker_data = pd.DataFrame()
    instrument_id = row[0]
    marketplace = row[3]
    try:
        logging.info('Trying exporter...')
        exporter = Exporter()
        try:
            logging.info('Trying download...')
            ticker_data = exporter.download(instrument_id, market=Market.USA, start_date=start_time,
                                            end_date=end_time, timeframe=Timeframe.DAILY)
        except Exception as e:
            logging.info(f'Exception: {e}')
    except Exception as e:
        logging.info(f'Exception: {e}')

    if not ticker_data.empty:
        date_time = ticker_data.iloc[0, 0]
        open = ticker_data.iloc[0, 2]
        high = ticker_data.iloc[0, 3]
        low = ticker_data.iloc[0, 4]
        close = ticker_data.iloc[0, 5]
        volume = ticker_data.iloc[0, 6]
        return instrument_id, open, high, low, close, volume, date_time, TIMEFRAME_ID, marketplace
    else:
        return None


def list_to_csv_as_row(file_name, list_of_elem):
    # Open file in append mode
    with open(file_name, 'a+', newline='') as write_obj:
        # Create a writer object from csv module
        csv_writer = writer(write_obj, delimiter=';')
        # Add contents of list as last row in the csv file
        csv_writer.writerow(list_of_elem)
