import pandas as pd
import pytest
import random


@pytest.fixture
def on_wrist_dataset():
    return pd.DataFrame([1] * 60)


@pytest.fixture
def temperature_dataset():
    return pd.DataFrame([random.randint(2700, 3700) for _ in range(4 * 61)])


@pytest.fixture
def ppg_dataset():
    return pd.DataFrame([random.randint(1500, 5500) for _ in range(64 * 60 + 5)])
