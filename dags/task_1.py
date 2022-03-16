import requests
import datetime
import csv
def write_csv():
    url = "https://community-open-weather-map.p.rapidapi.com/weather"

    headers = {
        'x-rapidapi-host': "community-open-weather-map.p.rapidapi.com",
        'x-rapidapi-key': "c80d8e612emsha50372cf8510eadp15af9ajsn4c3afd249563"
    }

    cities = ["Gandhinagar","Surat","Mumbai","Goa","Hyderabad","Delhi","chandigarh","Jaipur","Pune","Bhopal"]

    data_to_write = []

    print(cities)
    for city in cities:
        querystring = {"q":f"{city},india","lat":"0","lon":"0","id":"2172797","lang":"null","units":"imperial","mode":"JSON"}
        response = requests.request("GET", url, headers=headers, params=querystring)
        data = response.json()
        print(data)
        list_item = [data["name"] ,data["weather"][0]["description"],data["main"]["temp"], data["main"]["feels_like"],
                     data["main"]["temp_min"], data["main"]["temp_max"], data["main"]["humidity"], data["clouds"]["all"] ]
        print(list_item)
        data_to_write.append(list_item)

    header = ['State', 'Description', 'Temperature', 'Feels_Like_Temperature', 'Min_Temperature', 'Max_Temperature', 'Humidity', 'Clouds']
    try:
        with open('/usr/local/airflow/store_files_airflow/Weather_Data.csv', 'w', encoding='UTF8') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(data_to_write)
        print("Done Successfully")
    except:
        print("connection error...")
