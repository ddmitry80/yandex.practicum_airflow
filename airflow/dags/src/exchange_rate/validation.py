from typing import Optional


def validate_date(export_date: str, data_date: str, date_fmt: str) -> Optional[str]:
    _export_date = export_date.split("T")[0]

    # create datetime
    _raw_export_datetime = export_date.split(".")[0].split("T")
    _export_datetime = " ".join(_raw_export_datetime)

    if _export_date == data_date:
        date_formats = {"date": _export_date, "datetime": _export_datetime}
        return date_formats[date_fmt]
    raise ValueError("The dates don't match")


def validate_rate(rate: float) -> Optional[float]:
    if isinstance(rate, float):
        return rate
    raise ValueError("Rate is not a float type number")
