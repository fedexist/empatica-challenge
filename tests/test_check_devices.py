import pytest

from check_faulty_devices.check_devices import dict_contains_any_true, merge_data, is_device_faulty_wrist_on


def test_dict_contains_any_true():

    dict_with_true = {
        'a': {
            'b': False,
        },
        'c': {
            'd': True
        }
    }

    assert dict_contains_any_true(dict_with_true)

    dict_without_true = {
        'a': {
            'b': False,
        }
    }

    assert not dict_contains_any_true(dict_without_true)

    dict_raising_exception = {
        'a': 'not_a_dict'
    }

    with pytest.raises(AttributeError, match="'str' object has no attribute 'values'"):
        dict_contains_any_true(dict_raising_exception)


def test_merge_data(on_wrist_dataset, temperature_dataset, ppg_dataset):

    complete = merge_data(on_wrist_dataset, temperature_dataset, ppg_dataset)

    assert list(complete.columns) == ['on_wrist', 'temperature', 'ppg']

    assert len(complete) == 3840
