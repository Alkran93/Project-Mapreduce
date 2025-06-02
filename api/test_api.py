import pytest
from app import app
 
@pytest.fixture
def client():
    # Configure app for testing and create test client
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client
 
def test_health_endpoint(client):
    # Test the /health endpoint returns status 200 and includes api_status key
    resp = client.get('/health')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'api_status' in data
 
def test_temperature_no_filters(client):
    # Test /data/temperature without filters returns 200 or 404 if no data
    resp = client.get('/data/temperature')
    assert resp.status_code in (200, 404)
    data = resp.get_json()
    if resp.status_code == 200:
        # Check that response contains data or error key
        assert 'data' in data or 'error' in data
 
def test_temperature_valid_filter(client):
    # Test /data/temperature with valid city and year filters
    resp = client.get('/data/temperature?city=Medellin&year=2023')
    assert resp.status_code in (200, 404)
    data = resp.get_json()
    if resp.status_code == 200:
        # Verify all records match filter criteria
        for record in data.get('data', []):
            assert 'Medellin'.lower() in record.get('city', '').lower()
            assert record.get('year') == 2023
 
def test_temperature_invalid_year(client):
    # Test /data/temperature with invalid year filter returns 400 error
    resp = client.get('/data/temperature?year=invalid')
    assert resp.status_code == 400
 
def test_precipitation_with_filters(client):
    # Test /data/precipitation with city and season filters returns expected results
    resp = client.get('/data/precipitation?city=Bogota&season=Invierno')
    assert resp.status_code in (200, 404)
    data = resp.get_json()
    if resp.status_code == 200:
        # Verify all records match filter criteria
        for record in data.get('data', []):
            assert 'Bogota'.lower() in record.get('city', '').lower()
            assert 'Invierno'.lower() in record.get('season', '').lower()