import json
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Dict, Tuple

import numpy as np
import pandas as pd

from datetime import date, timedelta
from pathlib import Path
import os
import re

from check_faulty_devices.plot_utils import plot_device_data

BASE_DIR = Path(os.getenv('BUCKET_PATH', 'raw_bucket'))
WORKERS = os.getenv('WORKERS')

# This standard deviation values should be appropriately set
# by someone who knows their stuff
TEMPERATURE_STD_THRESHOLD = 200
PPG_STD_WRIST_ON_THRESHOLD = 3000
PPG_STD_WRIST_OFF_THRESHOLD = 500

PPG_SAMPLING_RATE = 64
ON_WRIST_SAMPLING_RATE = 1
TEMPERATURE_SAMPLING_RATE = 4

TEMPERATURE_WRIST_ON_VALUE_THRESHOLD = range(2700, 3700)
TEMPERATURE_WRIST_OFF_LOWER_THRESHOLD = 2700


def load_data(path: Path) -> pd.DataFrame():
    """
    Load data from a path.

    We expect 3 csv datasets containing a time series each.
    Because of the difference in the sampling rate, we normalize the timeseries to 64 Hz, so that
    1 sample from on_wrist becomes 64 samples with the same value, and 1 sample from temperature
    becomes 16 samples with the same value.

    Then, we create a single dataset with the 3 time series and cut their length to the minimum length of the 3,
    so that our dataset has a unique size.
    """
    csvs = [file_path for file_path in path.iterdir() if str(file_path).endswith('csv')]
    dfs = [pd.read_csv(file_path, header=None) for file_path in csvs]

    on_wrist = dfs[0].loc[dfs[0].index.repeat(PPG_SAMPLING_RATE / ON_WRIST_SAMPLING_RATE)].reset_index(drop=True)
    temperature = dfs[1].loc[dfs[1].index.repeat(PPG_SAMPLING_RATE / TEMPERATURE_SAMPLING_RATE)].reset_index(drop=True)
    ppg = dfs[2]

    wrist_samples = len(on_wrist)
    temp_samples = len(temperature)
    ppg_samples = len(ppg)

    cut_off = min(wrist_samples, temp_samples, ppg_samples)

    complete = pd.DataFrame({
        'on_wrist': on_wrist[0][:cut_off],
        'temperature': temperature[0][:cut_off],
        'ppg': ppg[0][:cut_off]

    })
    return complete


def is_device_faulty_wrist_on(df: pd.DataFrame, indices) -> Dict:
    """
    While the device is worn we expect the standard deviation of the two sensors
    to be reasonable
    """
    is_faulty = {}

    for group, index in indices.items():
        df_on_sequence = df.iloc[index]
        # Windows based on when the sensors are actually worn
        windows = df_on_sequence.reset_index(drop=True).rolling(window=16)

        temp_std: pd.DataFrame = windows.temperature.std()
        ppg_std = windows.ppg.std()

        temp_outside_range = df_on_sequence[
                                     ~df_on_sequence.temperature.isin(TEMPERATURE_WRIST_ON_VALUE_THRESHOLD)
                                 ]

        temp_over_std_threshold = temp_std[temp_std > TEMPERATURE_STD_THRESHOLD]
        ppg_over_std_threshold = ppg_std[ppg_std > PPG_STD_WRIST_ON_THRESHOLD]

        is_faulty[group] = {
            'temperature_over_std_threshold': len(temp_over_std_threshold) > 16,
            'ppg_over_std_threshold': len(ppg_over_std_threshold) > 64,
            'temperature_outside_range': len(temp_outside_range) > 16
        }

    return is_faulty


def dict_contains_any_true(d: Dict) -> bool:
    for key, value in d.items():
        if any(value.values()):
            return True

    return False


def is_device_faulty_wrist_off(df: pd.DataFrame, indices: Dict) -> Dict:
    """

    """
    is_faulty = {}

    for group, index in indices.items():

        # Take in consideration only segments longer than 1 second
        if len(index) > 64:
            df_off_sequence = df.iloc[index]
            # Windows based on when the sensors are actually not worn
            windows = df_off_sequence.reset_index(drop=True).rolling(window=16)
            ppg_std = windows.ppg.std()
            ppg_over_threshold = ppg_std[ppg_std > PPG_STD_WRIST_OFF_THRESHOLD]

            temperature_gradient = np.gradient(df_off_sequence.temperature, 4)
            ppg_gradient = np.gradient(df_off_sequence.ppg, 1)

            is_ppg_decreasing = ppg_gradient.sum() <= 0
            is_temperature_decreasing = temperature_gradient.sum() <= 0

            is_faulty[group] = {
                'ppg_over_threshold': len(ppg_over_threshold) > 64,
                'is_temperature_increasing': not is_temperature_decreasing,
                'is_ppg_increasing': not is_ppg_decreasing
            }

    return is_faulty


def is_device_faulty(df: pd.DataFrame) -> Tuple[bool, Dict]:
    wrist_on = df[df['on_wrist'] == 1].drop(columns=['on_wrist'])
    wrist_off = df[df['on_wrist'] == 0].drop(columns=['on_wrist'])

    indices_wrist_on = wrist_on.groupby(wrist_on.index.to_series().diff().ne(1).cumsum()).groups
    indices_wrist_off = wrist_off.groupby(wrist_off.index.to_series().diff().ne(1).cumsum()).groups

    is_device_faulty_on = is_device_faulty_wrist_on(df.copy(), indices_wrist_on)
    is_device_faulty_off = is_device_faulty_wrist_off(df.copy(), indices_wrist_off)

    is_faulty = dict_contains_any_true(is_device_faulty_on) or dict_contains_any_true(is_device_faulty_off)

    return is_faulty, {'wrist_on': is_device_faulty_on, 'wrist_off': is_device_faulty_off}


def send_alert(df: pd.DataFrame, device_name: str, explanation: Dict, with_plot=False):
    """
    A proper alert would integrate with the monitoring tools used, instead of printing to stdout.
    Possibly to a message broker or a proper table used by dashboard tool.
    """
    print(f"""Device {device_name} is malfunctioning!
Explanation:
{json.dumps(explanation, indent=4)}
-------------
""")
    if with_plot:
        plot_device_data(df, figsize=(10, 5))


def device_alert(device_path: Path):
    device_name = device_path.name
    df = load_data(device_path)

    faulty, explanation = is_device_faulty(df)

    if faulty:
        send_alert(df, device_name, explanation)


def process_day(day: date = date.today() - timedelta(days=1)):
    today_dir = Path(os.path.join(BASE_DIR, day.strftime('%Y/%m/%d')))

    if today_dir.exists():
        # Obviously, if using a bucket (gcs, s3 etc.), I'd use the proper call for directory listing
        devices = [device for device in today_dir.iterdir() if re.match(r'device_\d{3}', device.name)]

        if devices:
            with ThreadPoolExecutor(max_workers=WORKERS or len(devices)) as executor:
                results = executor.map(device_alert, devices)
        else:
            print("No devices available!")
    else:
        print(f"No data available for date {day}")


if __name__ == '__main__':
    days = ['2021-02-02', '2021-02-03', '2021-02-04']

    for day in days:
        process_day(date.fromisoformat(day))
