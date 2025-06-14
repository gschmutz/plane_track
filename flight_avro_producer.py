from uuid import uuid4
import time


from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

def get_string(s):
    ret = ''
    try:
        ret = str(s)
    except Exception as e:
        print(f"The error is: {e} for input {s}")    
    return(ret)

def get_int(i):
    ret = 0
    try:
        ret = int(0 if i is None else i)
    except Exception as e:
        print(f"The error is: {e} for input {i}")    
    return(ret)

def get_float(f):
    ret = 0.0
    try:
        ret = float(f)
    except Exception as e:
        print(f"The error is: {e} for input {f}")    
    return(ret)

class Flight(object):
    def __init__(self, callsign, latitude, longtitude, altitude, icao, speed, track, sqwark='', emergency='', airport='', flightts=0):
        self.callsign = get_string(callsign)
        self.latitude = get_float(latitude)
        self.longtitude = get_float(longtitude)
        self.altitude = get_int(altitude)
        self.sqwark = get_string(sqwark)
        self.icao = get_string(icao)
        self.speed = get_int(speed)
        self.track = get_int(track)
        self.emergency = get_string(emergency)
        self.airport = get_string(airport)
        self.eventts = time.time() * 1000
        if flightts==0:
            self.flightts = self.eventts
        elif flightts < 1890691200: # 2030 in seconds
            # in seconds - change to milliseconds
            self.flightts = get_int(flightts) * 1000
        else:
            self.flightts = get_int(flightts)


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    # print('User record {} successfully produced to {} [{}] at offset {}'.format(
    #     msg.key(), msg.topic(), msg.partition(), msg.offset()))

def flight_to_dict(flight, ctx):
    return dict(callsign=flight.callsign,
                latitude=flight.latitude,
                longtitude=flight.longtitude,
                altitude=flight.altitude,
                sqwark=flight.sqwark,
                eventts=flight.eventts,
                icao=flight.icao,
                speed=flight.speed,
                track=flight.track,
                airport=flight.airport,
                emergency=flight.emergency,
                flightts=flight.flightts,
                )



class MyProducer(object):
    def __init__(self, topic, schema_file, schema_registry='http://localhost:8081', bootstrap_servers='PLAINTEXT://localhost:9092'):
        self.topic = topic

        with open(schema_file) as f:
            schema_str = f.read()

        schema_registry_conf = {'url': schema_registry}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        self.avro_serializer = AvroSerializer(schema_registry_client,
                                        schema_str,
                                        flight_to_dict)

        self.string_serializer = StringSerializer('utf_8')
        producer_conf = {'bootstrap.servers': bootstrap_servers}
        self.producer = Producer(producer_conf)


    def do_produce(self, flight):
        self.producer.produce(topic=self.topic,
                            key=self.string_serializer(str(uuid4())),
                            value=self.avro_serializer(flight, SerializationContext(self.topic, MessageField.VALUE)),
                            on_delivery=delivery_report,
                            )
    def do_flush(self):
        self.producer.flush()
        


