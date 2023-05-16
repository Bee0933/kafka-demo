from typing import List, Dict
# from datetime import datetime
from decimal import Decimal


class Rides:
    def __init__(self, arr: list) -> None:
        if not isinstance(arr, List):
            raise TypeError("input list type only")

        self.VendorID = arr[0]
        self.tpep_pickup_datetime = arr[
            1
        ]  # datetime.strptime(arr[1], "%Y-%m-%d %H:%M:%S") - uses parquet data schema already
        self.tpep_dropoff_datetime = arr[
            2
        ]  # datetime.strptime(arr[2], "%Y-%m-%d %H:%M:%S")
        self.passenger_count = int(arr[3])
        self.trip_distance = Decimal(arr[4])
        self.RatecodeID = int(arr[5])
        self.store_and_fwd_flag = str(arr[6])
        self.PULocationID = int(arr[7])
        self.DOLocationID = int(arr[8])
        self.payment_type = int(arr[9])
        self.fare_amount = Decimal(arr[10])
        self.extra = Decimal(arr[11])
        self.mta_tax = Decimal(arr[12])
        self.tip_amount = Decimal(arr[13])
        self.tolls_amount = Decimal(arr[14])
        self.improvement_surcharge = Decimal(arr[15])
        self.total_amount = Decimal(arr[16])
        self.congestion_surcharge = Decimal(arr[17])
        self.airport_fee = Decimal(arr[18])

    @classmethod
    def from_dict(cls, d: Dict):
        return cls(
            arr=[
                d["VendorID"],
                d["tpep_pickup_datetime"][0],
                d["tpep_dropoff_datetime"][0],
                d["passenger_count"],
                d["trip_distance"],
                d["RatecodeID"],
                d["store_and_fwd_flag"],
                d["PULocationID"],
                d["DOLocationID"],
                d["payment_type"],
                d["fare_amount"],
                d["extra"],
                d["mta_tax"],
                d["tip_amount"],
                d["tolls_amount"],
                d["improvement_surcharge"],
                d["total_amount"],
                d["congestion_surcharge"],
                d["airport_fee"],
            ]
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} : {self.__dict__}"
