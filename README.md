## Requirements

* `git`
* a version of Python >= 3.7
* [Optional] `jupyter`, if you want to run the `exploration.ipynb` notebook


## How to install

```shell
# 1. Clone the repository and get in the repo folder
git clone https://github.com/fedexist/unhealthy-wearables
cd unhealthy-wearables


# 2. Download raw_bucket.zip inside the 'unhealthy-wearables' folder and unzip it
unzip raw_bucket.zip

# 3. Create a virtual environment and activate it
python3.9 -m venv venv
source venv/bin/activate

# 4. Install the package
pip install -e .

# 5. [Optional] If you want to check out the exploration.ipynb notebook
pip install ipykernel
## Create an ipykernel based on the virtual environment activated
python -m ipykernel install --user --name=emp39
## Run the notebook
jupyter-notebook exploration.ipynb

# 6.a Run the package on the 3 days available within the zip
python -m check_faulty_devices.check_devices

# 6.b or Run it on yesterday (this would be the default entrypoint of a container)
python -m check_faulty_devices

# 6.c or run it on the day you want to specify (iso format)
export MONITORING_DATE=2021-02-02
python -m check_faulty_devices
```

## How does it all work?

The problem at hand is finding out which devices might be malfunctioning, given the assumptions that

* `on_wrist` data is always correct
* `ppg` and `temperature` data may show malfunctionings

After a bit of reasoning and taking a look at the data:

```shell
jupyter-notebook exploration.ipynb
```

```python
import os
import re
import pandas as pd
from check_faulty_devices.plot_utils import plot_device_data

for root, d, files in os.walk('raw_bucket'):
    if files and re.match(r'raw_bucket/\d{4}/\d{2}/\d{2}/device_\d{3}', root):
        csvs = [os.path.join(root, file) for file in files if file.endswith('csv')]
        dfs = [pd.read_csv(path, header=None) for path in csvs]

        on_wrist = dfs[0].loc[dfs[0].index.repeat(64)].reset_index(drop=True)
        temperature = dfs[1].loc[dfs[1].index.repeat(16)].reset_index(drop=True)
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
        device_name = root.split('/')[-1]
        plot_device_data(complete, device_name, figsize=(15, 10))
```

We can make some assumptions and set some expectations on how a correct functioning may be, 
differentiating two different datasets:

* When the device is worn (`on_wrist == 1`):
    * we expect temperature within a certain value range
    * we expect temperature not to have spikes and, therefore, we can set a reasonable threshold for
    the expected standard deviation
    * we expect ppg not to vary wildly and therefore set a reasonable threshold for its standard deviation
    
* When the device is not worn (`on_wrist == 0`):
    * we expect the temperature measurements to be decreasing
    * we expect the ppg not to vary wildly and set a reasonable threshold for its standard deviation (in this
      case, the threshold would be lower than the one considered while the device is worn)

Possibly, these values should be based on a broader set of observations and decided by someone competent in the domain.

### 1. Loading the data

The 3 data sets have a different sample rate, so we resample them to 64 Hz:

* 1 sample from on_wrist (1 Hz) becomes 64 samples with the same value 
* 1 sample from temperature (4 Hz) becomes 16 samples with the same value.

after this, we make sure we have the same number of samples for all of 3 time series, cutting off the sequence 
at the minimum among the length of the 3 datasets.

N.B: we could have taken the directly opposite approach, resampling to 1 Hz or 4 Hz, but that would have meant losing 
information, because we'd have to aggregate our data points. This loss of information could be acceptable though.


### 2. Check our data for both cases (wrist on/off)

For each continuous segment of data, in both cases if the devices is worn or not, we check our conditions and see 
if at least one of them is true:

```python
def is_device_faulty(df: pd.DataFrame) -> Tuple[bool, Dict]:
    wrist_on = df[df['on_wrist'] == 1].drop(columns=['on_wrist'])
    wrist_off = df[df['on_wrist'] == 0].drop(columns=['on_wrist'])

    indices_wrist_on = wrist_on.groupby(wrist_on.index.to_series().diff().ne(1).cumsum()).groups
    indices_wrist_off = wrist_off.groupby(wrist_off.index.to_series().diff().ne(1).cumsum()).groups

    is_device_faulty_on = is_device_faulty_wrist_on(df.copy(), indices_wrist_on)
    is_device_faulty_off = is_device_faulty_wrist_off(df.copy(), indices_wrist_off)

    is_faulty = dict_contains_any_true(is_device_faulty_on) or dict_contains_any_true(is_device_faulty_off)

    return is_faulty, {'wrist_on': is_device_faulty_on, 'wrist_off': is_device_faulty_off}
```

### 3. Send alerts

Based on the results of our checks, we send the alerts. 

More on this, in the next paragraph

## Monitoring

In this toy use case, the monitoring is simply based on the output on stdout:

```python
def send_alert(df: pd.DataFrame, device_name: str, explanation: Dict, with_plot=False):
    """
    A proper alert would integrate with the monitoring tools used, instead of printing to stdout.
    Possibly to a message broker or a proper table used by dashboard tool.
    """
    print(f"""
    Device {device_name} is malfunctioning!
    Explanation:
    {json.dumps(explanation, indent=4)}
    -------------
    """)
    if with_plot:
        plot_device_data(df, figsize=(10, 5))
```

In a real world scenario, this function would integrate with the monitoring tools already in place, that could be:

* monitoring dashboards based on SQL/DWH/NoSQL tables displaying the key metrics for faulty devices
* a message broker (e.g: Kinesis, PubSub, Kafka etc.) that would receive the alert on a specified topic, together with
  a stream processor for this topic, in case of real time needs

## Production deployment

A production deployment in a cloud infrastructure could be designed in various ways:
* a scheduled CronJob on a Kubernetes Cluster
* a DAG in a scheduler like Airflow (Composer on GCP, MWAA on AWS) or other schedulers
* a scheduled serverless function (Lambda, Google Cloud Function)

this depends on the costs and on the infrastructure already available (e.g. it's unreasonable to create a Kubernetes cluster
if no other use case needs one). Usually, a serverless solution is the most cost effective, even if it may lack in flexibility.
