
from dagster import build_op_context
from analytics.ops.weather import get_cities, transform_weather, CitiesConfig

def test_get_cities():
    cities = get_cities(
        context=build_op_context(),
        config=CitiesConfig(city_path="analytics_tests/fixtures/csv/australian_capital_cities.csv"))
    assert cities == [
        "canberra",
        "sydney",
        "darwin"
    ]

def test_transform_weather():
    input_data = [
        {"id": 123, "dt": 123456780, "name": "Perth", "main.temp": 29.5, "some_other_column": "blah"},
        {"id": 124, "dt": 123456781, "name": "Sydney", "main.temp": 14.7, "some_other_column": "blah"},
        {"id": 125, "dt": 123456782, "name": "Melbourne", "main.temp": 20.3, "some_other_column": "blah"}
    ]
    expected_data = [
        {"id": 123, "datetime": 123456780, "name": "Perth", "temperature": 29.5},
        {"id": 124, "datetime": 123456781, "name": "Sydney", "temperature": 14.7},
        {"id": 125, "datetime": 123456782, "name": "Melbourne", "temperature": 20.3}
    ]
    actual_data = transform_weather(context=build_op_context(), data=input_data)
    assert actual_data == expected_data
