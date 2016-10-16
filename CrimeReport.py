import logging,operator,urllib,json
import request

logging.basicConfig(level=logging.DEBUG)

from spyne import Application, rpc, ServiceBase, \
    Integer, Unicode

from spyne import Iterable

from spyne.protocol.http import HttpRpc
from spyne.protocol.json import JsonDocument

from spyne.server.wsgi import WsgiApplication
import urllib, json, operator

class HelloWorldService(ServiceBase):
    @rpc(Unicode, Unicode, Unicode, _returns=Iterable(Unicode))
    #@rpc(_returns=Unicode)
    def checkcrime(ctx,lat,lon,radius):
        resp = {"total_crime": 0, "the_most_dangerous_streets": [], "crime_type_count": {}, "event_time_count": {}}
        url = "https://api.spotcrime.com/crimes.json?lat="+lat+"&lon="+lon+"&radius="+radius+"&key=."
        response = urllib.urlopen(url)
        json_obj = json.loads(response.read())
        #json_obj=request.GET('https://api.spotcrime.com/crimes.json?lat=37.334164&lon=-121.884301&radius=0.02&key=.').json()
        json_obj1 = json_obj['crimes']
        dict_crimeType = {}
        dict_address = {}
        dict_time = {"12:01am-3am": 0, "3:01am-6am": 0, "6:01am-9am": 0, "9:01am-12noon": 0, "12:01pm-3pm": 0,
                     "3:01pm-6pm": 0, "6:01pm-9pm": 0, "9:01pm-12midnight": 0}
        for json_obj2 in json_obj1:
            # extract crimetype from the input
            crimeType = json_obj2['type']
            if dict_crimeType.has_key(crimeType):
                dict_crimeType[crimeType] += 1
            else:
                dict_crimeType2 = {crimeType: 1}
                dict_crimeType.update(dict_crimeType2)
            # extract crimeAddress from the input

            crimeAddress = str(json_obj2['address'])
            if crimeAddress.find("OF") >= 0:
                street = crimeAddress.split(" OF ", 1)[1]
                if dict_address.has_key(street):
                    dict_address[street] += 1
                else:
                    dict_address2 = {street: 1}
                    dict_address.update(dict_address2)
            elif crimeAddress.find("&") >= 0:
                street = crimeAddress.split(" & ", 1)[0]
                if dict_address.has_key(street):
                    dict_address[street] += 1
                else:
                    dict_address2 = {street: 1}
                    dict_address.update(dict_address2)
                street = crimeAddress.split(" & ", 1)[1]
                if dict_address.has_key(street):
                    dict_address[street] += 1
                else:
                    dict_address2 = {street: 1}
                    dict_address.update(dict_address2)
            elif crimeAddress.find("BLOCK") >= 0:
                street = crimeAddress.split(" BLOCK ", 1)[1]
                if street.find("BLOCK") >= 0:
                    street1 = street.split("BLOCK ", 1)[1]
                else:
                    street1 = street
                if dict_address.has_key(street1):
                    dict_address[street1] += 1
                else:
                    dict_address2 = {street1: 1}
                    dict_address.update(dict_address2)
            else:
                if dict_address.has_key(crimeAddress):
                    dict_address[crimeAddress] += 1
                else:
                    dict_address2 = {crimeAddress: 1}
                    dict_address.update(dict_address2)
            # extract crimeTime from the input
            crimeDate = str(json_obj2['date'])
            crimeTime = crimeDate.split(" ", 1)[1]
            time = crimeTime.split(" ", 1)[0]
            time2 = crimeTime.split(" ", 1)[1]
            hours = int(time.split(":", 1)[0])
            mins = int(time.split(":", 1)[1])
            if (hours >= 12 and mins > 0) or (hours < 3 and mins > 0) or (hours == 3 and mins == 0):
                if time2 == 'AM':
                    dict_time['12:01am-3am'] += 1
                else:
                    dict_time['12:01pm-3pm'] += 1
            elif (hours >= 3 and mins > 0) or (hours < 6 and mins > 0) or (hours == 6 and mins == 0):
                if time2 == 'AM':
                    dict_time['3:01am-6am'] += 1
                else:
                    dict_time['3:01pm-6pm'] += 1
            elif (hours >= 6 and mins > 0) or (hours < 9 and min > 0) or (hours == 9 and mins == 0):
                if time2 == 'AM':
                    dict_time['6:01am-9am'] += 1
                else:
                    dict_time['6:01pm-9pm'] += 1
            elif (hours >= 9 and mins > 0) or (hours < 12 and min > 0):
                if time2 == 'AM':
                    dict_time['9:01am-12noon'] += 1
                else:
                    dict_time['9:01pm-12midnight'] += 1
            else:
                if time2 == 'AM':
                    dict_time['9:01pm-12midnight'] += 1
                else:
                    dict_time['9:01am-12noon'] += 1

        totalCrimes = sum(dict_crimeType.values())
        sorted_dict_address = sorted(dict_address.items(), key=operator.itemgetter(1), reverse=True)
        top_three_dangerous_streets = []

        for k, v in sorted_dict_address[:3]:
            top_three_dangerous_streets.append(k)

        resp["total_crime"] = totalCrimes
        resp["the_most_dangerous_streets"] = top_three_dangerous_streets
        resp["crime_type_count"] = dict_crimeType
        resp["event_time_count"] = dict_time
        yield resp


application = Application([HelloWorldService],
                          tns='spyne.examples.hello',
                          in_protocol=HttpRpc(validator='soft'),
                          out_protocol=JsonDocument()
                          )

if __name__ == '__main__':
    # You can use any Wsgi server. Here, we chose
    # Python's built-in wsgi server but you're not
    # supposed to use it in production.
    from wsgiref.simple_server import make_server

    wsgi_app = WsgiApplication(application)
    server = make_server('0.0.0.0', 8000, wsgi_app)
    server.serve_forever()
