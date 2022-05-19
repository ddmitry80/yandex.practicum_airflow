from typing import Optional


def validate_date(export_date: str, data_date: str, date_fmt: str) -> Optional[str]:
    """
    Checking the correctness of the date received from the api 
    and recording this data in accordance with the selected data format
    :param export_date: date of data upload specified by the user
    :param data_date: date when data was uploaded from the api
    :param date_fmt: the data format specified by the user (date: %Y-%m-%d, datetime: %Y-%m-%dT%H:%m:%S)
    """
    _export_date = export_date.split("T")[0]

    # create datetime
    _raw_export_datetime = export_date.split(".")[0].split("T")
    _export_datetime = " ".join(_raw_export_datetime)

    if _export_date == data_date:
        date_formats = {"date": _export_date, "datetime": _export_datetime}
        return date_formats[date_fmt]
    raise ValueError("The dates don't match")


def validate_rate(rate: float) -> Optional[float]:
    """
    Checking the correctness of the rate received from the api
    :param rate: rate when data was uploaded from the api
    """
    if isinstance(rate, float):
        return rate
    raise ValueError("Rate is not a float type number")
