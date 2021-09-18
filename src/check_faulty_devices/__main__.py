import os
from datetime import date, timedelta

from check_faulty_devices.check_faulty_devices import process_day


DATE = os.getenv('MONITORING_DATE')

if not DATE:
    monitoring_date = date.today() - timedelta(days=1)
else:
    monitoring_date = date.fromisoformat(DATE)

process_day(monitoring_date)
