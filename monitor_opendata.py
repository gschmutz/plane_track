import time
import requests
import logging
import argparse
from datetime import datetime
from flight_avro_producer import MyProducer, Flight


# Flag to turn logging on or off
ENABLE_LOGGING = True
if ENABLE_LOGGING:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
else:
    logging.basicConfig(level=logging.ERROR)
# Configuration Parameters


POLLING_INTERVAL = 10  # Time between API calls in seconds
dist = 5


airport_location = {
    # lat, lon
    'SYD': (-33.9401302, 151.175371),
    'LHR': (  51.470020,  -0.454295),
    'LAX': (  33.942791,-118.410042),
    'ATL': (  33.640411, -84.419853),
}




def get_flights(point_lat, point_lon, dist):
    url = f'https://opendata.adsb.fi/api/v2/lat/{point_lat}/lon/{point_lon}/dist/{dist}'
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get('aircraft', [])

def process_flight(file_name, flight, prd):
    # Ignore anything with a ground speed below 50 knots
    if (flight.get('gs') is None or flight.get('gs') < 50):
        return
    
    formatted_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data = f'{formatted_timestamp},{flight.get('hex')},{flight.get('flight')},{flight.get('r')},{flight.get('t')},{flight.get('alt_baro')},{flight.get('alt_geom')},{flight.get('gs')},{flight.get('track')},{flight.get('lat')},{flight.get('lon')},{flight.get('squawk')},{flight.get('emergency')}'
    with open(file_name, 'a') as f:
        f.write(f'{data}\n')

    if prd is not None:
        flight = Flight(callsign=flight.get('flight').rstrip(),
                        latitude=flight.get('lat'),
                        longtitude=flight.get('lon'),
                        altitude=flight.get('alt_geom'),
                        icao=flight.get('hex'), 
                        speed=flight.get('gs'),
                        airport='' ,
                        track=flight.get('track'),
                        sqwark=flight.get('squawk'),
                        emergency=flight.get('emergency'),
                        )
        prd.do_produce(flight)

def monitor_flights(args):
    airport = args.airport
    file_name = f'./logs/{datetime.now().strftime('%Y%m%d%H%M%S')}_{airport}.csv'
    logging.info(f'Started scanning for {airport} - filename {file_name}')
    point_lat = airport_location[airport][0]
    point_lon = airport_location[airport][1]

    if (args.kafkaproducer):
        prd = MyProducer(topic='flights', schema_file='avro/flight.avsc')
    else:
        prd = None


    with open(file_name, 'a') as f:
        f.write(f'tm,hex,flight,registration,aircraft_type,altb,altg,gspeed,track,lat,lon,squawk,emergency\n')

    while True:
        try:
            flights = get_flights(point_lat, point_lon, dist)
            logging.debug(f'Retrieved {len(flights)} flights.')
            for flight in flights:
                process_flight(file_name, flight, prd)

            # Sleep after processing all flights
            time.sleep(POLLING_INTERVAL)
        except Exception as e:
            logging.error(f'Error: {e}')
            time.sleep(POLLING_INTERVAL)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--airport', choices=['SYD', 'LHR', 'LAX', 'ATL'], required=True, help='Airport of operation.')
    parser.add_argument('--kafkaproducer', action='store_true', help='Enable Kafka producer')

    args = parser.parse_args()
    monitor_flights(args)