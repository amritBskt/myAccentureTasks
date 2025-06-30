import pytest
import requests
import src.main2 as my_functions
from Exceptions.my_exceptions import *
from unittest.mock import patch, Mock

@pytest.fixture
def mock_success_response():
    return {'coord': {'lon': 77.2167, 'lat': 28.6667}, 
            'weather': [{'id': 803, 'main': 'Clouds', 'description': 'broken clouds', 'icon': '04d'}], 
            'base': 'stations', 
            'main': {
                'temp': 302.04, 
                'feels_like': 303.03, 
                'temp_min': 302.04, 
                'temp_max': 302.04, 
                'pressure': 996, 
                'humidity': 53, 
                'sea_level': 996, 
                'grnd_level': 970}, 
            'visibility': 10000, 
            'wind': {'speed': 2.08, 'deg': 107, 'gust': 3.32}, 
            'clouds': {'all': 72}, 
            'dt': 1750239667, 
            'sys': {'country': 'IN', 'sunrise': 1750204394, 'sunset': 1750254675}, 
            'timezone': 19800, 'id': 1273294, 'name': 'Delhi', 'cod': 200
            }


def test_DefaultResponse(mock_success_response):
    result = my_functions.fetch_weather()
    
    assert isinstance(result, dict)

    expected_keys = ["city", "weather", "temp", "feels_like", "temp_min",
                        "temp_max", "pressure", "humidity", "sea_level", "grnd_level"]
    
    for key in expected_keys:
        assert key in result


def test_ParameterizedResponse(mock_success_response):
    result = my_functions.fetch_weather(
            key="b1b15e88fa797225412429c1c50c122a1",
            city="Delhi",
            time_out=3,
            retries=5,
            delay=5
        )
        
    assert isinstance(result, dict)
    assert result["city"] == "Delhi"

def test_ClientErrors():
    with patch('requests.post') as mock_post:
        mock_post.return_value.headers = {"Content-Type": "application/json; charset=utf-8"}

        mock_post.return_value.status_code = 401
        mock_post.return_value.json.return_value = {"message": "Invalid API key"}

        with pytest.raises(ClientError):
            result = my_functions.fetch_weather(key="invalid_api_key")
        
        mock_post.return_value.status_code = 404
        mock_post.return_value.json.return_value = {"message": "city not found"}
        
        with pytest.raises(ClientError):
            result = my_functions.fetch_weather(city="NonexistentCity")

def test_TimeOutError():
    with patch('requests.post') as mock_post:
        mock_post.side_effect = requests.exceptions.Timeout

        with pytest.raises(requests.exceptions.Timeout):
            result = my_functions.fetch_weather(time_out=0.0001)


def test_RedirectionError():
    with patch('requests.post') as mock_post:
        mock_post.return_value.headers = {"Content-Type": "application/json; charset=utf-8"}

        mock_post.return_value.status_code = 301
        mock_post.return_value.json.return_value = {"message": "Moved Permanently"}
        
        with pytest.raises(RedirectionError):
            result = my_functions.fetch_weather(city="Chennai")


def test_ServerError():
    with patch('requests.post') as mock_post:
        mock_post.return_value.headers = {"Content-Type": "application/json; charset=utf-8"}

        mock_post.return_value.status_code = 500
        mock_post.return_value.json.return_value = {"message": "Internal Server Error"}
        
        with pytest.raises(ServerError):
            result = my_functions.fetch_weather(city="Chennai")


def test_UnexpectedError():
    with patch('requests.post') as mock_post:
        mock_post.return_value.headers = {"Content-Type": "application/json; charset=utf-8"}

        mock_post.return_value.status_code = 600 
        mock_post.return_value.json.return_value = {"message": "Unexpected Error"}
        
        with pytest.raises(UnexpectedError):
            result = my_functions.fetch_weather(city="Chennai")

def test_retry_mechanism(mock_success_response):
    server_error_response = Mock()
    server_error_response.status_code = 500
    server_error_response.json.return_value = {"message": "Internal Server Error"}
    server_error_response.headers = {"Content-Type": "application/json; charset=utf-8"}

    success_response = Mock()
    success_response.status_code = 200
    success_response.json.return_value = mock_success_response
    success_response.headers = {"Content-Type": "application/json; charset=utf-8"}

    responses = [server_error_response, server_error_response, success_response]

    with patch('requests.post', side_effect=responses) as mock_post:
        result = my_functions.fetch_weather(retries=3, delay=0) 

        assert isinstance(result, dict)
        assert mock_post.call_count == 3