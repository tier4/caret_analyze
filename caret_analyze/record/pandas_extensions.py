from typing import Optional, List
import pandas as pd
import numpy as np

from ..exceptions import InvalidArgumentError
from .record_factory import RecordFactory, RecordsFactory
from .interface import RecordsInterface
from .record import merge_sequencial
from .column import Column


class PandasExtensions:

    @staticmethod
    def merge_sequencial(
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        left_stamp_key: str,
        right_stamp_key: str,
        join_left_key: Optional[str],
        join_right_key: Optional[str],
        how: str
    ) -> pd.DataFrame:
        left_records = PandasExtensions.to_records(left_df)
        right_records = PandasExtensions.to_records(right_df)
        left_df = left_records.to_dataframe()
        right_df = right_records.to_dataframe()
        merged = merge_sequencial(
            left_records=left_records,
            right_records=right_records,
            left_stamp_key=left_stamp_key,
            right_stamp_key=right_stamp_key,
            join_left_key=join_left_key,
            join_right_key=join_right_key,
            how=how
        )
        return merged.to_dataframe()

    @staticmethod
    def to_records(
        df: pd.DataFrame
    ) -> RecordsInterface:
        if (set(df.dtypes) - {pd.Int64Dtype()}):
            raise InvalidArgumentError('to_records only supports Int64 dtype.')

        rows = []
        for _, row in df.iterrows():
            rows.append(
                RecordFactory.create_instance(
                    {
                        column: row[column]
                        for column
                        in df.columns
                        if row[column] is not pd.NA and not np.isnan(row[column])
                    }
                )
            )
        columns = [Column(c) for c in df.columns]
        records = RecordsFactory.create_instance(rows, columns)
        return records
