# -*- coding: utf-8 -*-
import pandas as pd
import pytz

from sqlalchemy import create_engine
from sqlalchemy.types import DateTime


def get_engine(uri: str):
    return create_engine(uri, pool_recycle=3600)


def get_connection(uri: str):
    return get_engine(uri).connect()


def get_df(uri, table_name):
    """Get dataframe and the first time create DB."""
    with get_connection(uri) as connection:
        try:
            df = pd.read_sql_table(
                table_name=table_name,
                con=connection,
                index_col='index',
                columns=['Token', 'Text', 'Link', 'Datetime discover', 'Datetime announcement'],
            )
        except ValueError:
            df = None
    return df


def save_df(df, uri, table_name):
    """Save dataframe on DB."""
    with get_connection(uri) as connection:
        df.to_sql(
            name=table_name,
            con=connection,
            index=True,
            index_label='index',
            if_exists='replace',
            dtype={"Datetime discover": DateTime(timezone=pytz.utc),
                   "Datetime announcement": DateTime(timezone=pytz.utc)}
        )
