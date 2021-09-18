import numpy as np
import pandas as pd

from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path
import os
import re

from check_faulty_devices.plot_utils import plot_multi

BASE_DIR = Path(os.getenv('BUCKET_PATH')) or Path('raw_bucket')
WORKERS = os.getenv('WORKERS')

TEMPERATURE_STD_THRESHOLD = 200
PPG_STD_WRIST_ON_THRESHOLD = 3000
PPG_STD_WRIST_OFF_THRESHOLD = 200

PPG_SAMPLING_RATE = 64
ON_WRIST_SAMPLING_RATE = 1
TEMPERATURE_SAMPLING_RATE = 4

TEMPERATURE_WRIST_ON_THRESHOLD = (2700, 3700)
TEMPERATURE_WRIST_OFF_LOWER_THRESHOLD = 2700


def load_data(path: Path) -> pd.DataFrame():
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


def is_device_faulty_wrist_on(df: pd.DataFrame, indices) -> bool:
    """
    While the device is worn we expect that standard deviation of the two sensors
    is reasonable
    """
    is_faulty = False

    for group, index in indices.items():
        df_on_sequence = df.iloc[index]
        # Windows based on when the sensors are actually worn
        windows = df_on_sequence.reset_index(drop=True).rolling(window=16)

        temp_std: pd.DataFrame = windows.temperature.std()
        ppg_std = windows.ppg.std()

        temp_over_threshold = temp_std[temp_std > TEMPERATURE_STD_THRESHOLD]
        ppg_over_threshold = ppg_std[ppg_std > PPG_STD_WRIST_ON_THRESHOLD]

        is_faulty = is_faulty or (len(temp_over_threshold) > 16) or (len(ppg_over_threshold) > 64)

    return is_faulty


def is_device_faulty_wrist_off(df: pd.DataFrame, indices) -> bool:
    """

    """
    is_faulty = False

    for group, index in indices.items():

        if len(index) > 64:
            df_off_sequence = df.iloc[index]
            # Windows based on when the sensors are actually not worn
            windows = df_off_sequence.reset_index(drop=True).rolling(window=16)
            ppg_std = windows.ppg.std()
            ppg_over_threshold = ppg_std[ppg_std > PPG_STD_WRIST_OFF_THRESHOLD]

            temperature_under_threshold = df_off_sequence[
                df_off_sequence['temperature'] > TEMPERATURE_WRIST_OFF_LOWER_THRESHOLD
                ]
            temperature_gradient = np.gradient(df_off_sequence.temperature, 4)
            ppg_gradient = np.gradient(df_off_sequence.ppg, 1)
            is_ppg_decreasing = ppg_gradient.sum() <= 0
            is_temperature_decreasing = temperature_gradient.sum() <= 0

            is_faulty = is_faulty or len(ppg_over_threshold) > 64 # not is_temperature_decreasing # or not is_ppg_decreasing

    return is_faulty


def is_device_faulty(df: pd.DataFrame) -> bool:
    wrist_on = df[df['on_wrist'] == 1].drop(columns=['on_wrist'])
    wrist_off = df[df['on_wrist'] == 0].drop(columns=['on_wrist'])

    indices_wrist_on = wrist_on.groupby(wrist_on.index.to_series().diff().ne(1).cumsum()).groups
    indices_wrist_off = wrist_off.groupby(wrist_off.index.to_series().diff().ne(1).cumsum()).groups

    is_device_faulty_on = is_device_faulty_wrist_on(df.copy(), indices_wrist_on)
    is_device_faulty_off = is_device_faulty_wrist_off(df.copy(), indices_wrist_off)

    return is_device_faulty_on or is_device_faulty_off


def send_alert(df, device_name):
    print("Device", device_name, "is malfunctioning!")
    plot_multi(df, figsize=(15, 10))


def device_alert(device_path: Path):
    device_name = device_path.name
    df = load_data(device_path)

    if is_device_faulty(df):
        send_alert(df, device_name)


def process_day(day: date = date.today() - timedelta(days=1)):
    today_dir = Path(os.path.join(BASE_DIR, day.strftime('%Y/%m/%d')))

    # Obviously, if using a bucket (gcs, s3 etc.), I'd use the proper call for directory listing
    devices = [device for device in today_dir.iterdir() if re.match(r'device_\d{3}', device.name)]

    with ThreadPoolExecutor(max_workers=WORKERS or len(devices)) as executor:
        results = executor.map(device_alert, devices)


if __name__ == '__main__':
    days = ['2021-02-02', '2021-02-03', '2021-02-04']

    for day in days:
        process_day(date.fromisoformat(day))
