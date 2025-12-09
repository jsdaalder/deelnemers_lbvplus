import requests
import pandas as pd
import time

search_csv = 'path/to/.csv' # <------ Zet hier de path naar je .csv waarop gezocht moet worden neer

def myround(x, base=50):
    return base * round(x/base)

payload = ""
headers = {
    "cookie": "TS01be00eb=014252a75b1ec45cb8afc9acec8a7d15d093024333a38bbab8830fe590847e57e3071520d957576eff51297ad8da1896843e4b2417",
    "Accept": "application/json, application/hal+json",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Origin": "https://www.kvk.nl",
    "Priority": "u=4",
    "profileId": "5C10A89D-635E-49CC-94B8-042DD533B64A",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:145.0) Gecko/20100101 Firefox/145.0"
}

name_list = []
status_list = []
kvk_list = []
vorm_list = []
street_list = []
streetnr_list = []
town_list = []
postal_list = []
results_list = []
bzk_town_list = []
bzk_postal_list = []
bzk_street_list = []
bzk_streetnr_list = []
sbi1_list = []
sbi2_list = []
sbi3_list = []

csvdata = pd.read_csv('C:/Users/Reemy/FTM/Stallenonderzoek/srv_deelnemers.csv', sep=';')
search_postal = csvdata["KOLOMNAAM OM TE ZOEKEN"].tolist() # <----- Verander deze kolomnaam naar de header van de kolom waarop gezocht moet worden

for a in range(0,len(search_postal)):

    print(f'Dit is {search_postal[a]}, kvknummer: #{a}')

    #Data ophalen
    querystring = {"q":search_postal[a],"language":"nl","site":"kvk2014","size":"50","start":"0","inschrijvingsstatus":"ingeschreven"}
    time.sleep(1)
    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
    data = response.json()

    #Aantal hits / resultaten en loops bepalen
    results = data['data']['numberOfHits']
    rounded = int(myround(results)/50)
    print(rounded)

    for b in range(0,rounded+1):

        #Nieuwe query voor elke pagina resultaten
        startnr = b*50
        querystring = {"q":search_postal[a],"language":"nl","site":"kvk2014","size":"50","start":startnr,"inschrijvingsstatus":"ingeschreven"}
        time.sleep(1)
        response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
        data = response.json()

        for y in range(0,50):

            try:
                company_name = data['data']['items'][int(f"{y}")]['naam']
            except KeyError:
                company_name = "Ontbreekt"
            except IndexError:
                company_name = "Ontbreekt"

            try:
                status = data['data']['items'][int(f"{y}")]['actief']
            except KeyError:
                status = "Ontbreekt"
            except IndexError:
                status = "Ontbreekt"

            try:
                kvk_number = data['data']['items'][int(f"{y}")]['kvkNummer']
            except KeyError:
                kvk_number = "Ontreekt"
            except IndexError:
                kvk_number = "Ontbreekt"

            try:
                rechtsvorm = data['data']['items'][int(f"{y}")]['rechtsvormCode']
            except KeyError:
                rechtsvorm = "Ontbreekt"
            except IndexError:
                rechtsvorm = "Ontbreekt"

            try:
                street = data['data']['items'][int(f"{y}")]['bezoeklocatie']['straat']
            except KeyError:
                street = "Ontbreekt"
            except IndexError:
                street = "Ontbreekt"

            try:
                town = data['data']['items'][int(f"{y}")]['bezoeklocatie']['plaats']
            except KeyError:
                town = "Ontbreekt"
            except IndexError:
                town = "Ontbreekt"

            try:
                streetnumber = data['data']['items'][int(f"{y}")]['bezoeklocatie']['huisnummer']
            except KeyError:
                streetnumber = "Ontbreekt"
            except IndexError:
                streetnumber = "Ontbreekt"

            try:    
                sbi1 = data['data']['items'][int(f"{y}")]['activiteiten'][0]['code']
            except KeyError:
                sbi1 = "Ontbreekt"
            except IndexError:
                sbi1 = "Ontbreekt"

            try:    
                sbi2 = data['data']['items'][int(f"{y}")]['activiteiten'][1]['code']
            except KeyError:
                sbi2 = "Ontbreekt"
            except IndexError:
                sbi2 = "Ontbreekt"

            try:    
                sbi3 = data['data']['items'][int(f"{y}")]['activiteiten'][0]['code']
            except KeyError:
                sbi3 = "Ontbreekt"
            except IndexError:
                sbi3 = "Ontbreekt"

            try:    
                postal = data['data']['items'][int(f"{y}")]['bezoeklocatie']['postcode']
            except KeyError:
                postal = "Ontbreekt"
            except IndexError:
                postal = "Ontbreekt"

            try:
                bzk_street = data['data']['items'][int(f"{y}")]['postlocatie']['straat']
            except KeyError:
                bzk_street = "Ontbreekt"
            except IndexError:
                bzk_street = "Ontbreekt"

            try:
                bzk_town = data['data']['items'][int(f"{y}")]['postlocatie']['plaats']
            except KeyError:
                bzk_town = "Ontbreekt"
            except IndexError:
                bzk_town = "Ontbreekt"

            try:
                bzk_streetnumber = data['data']['items'][int(f"{y}")]['postlocatie']['huisnummer']
            except KeyError:
                bzk_streetnumber = "Ontbreekt"
            except IndexError:
                bzk_streetnumber = "Ontbreekt"

            try:
                bzk_postal = data['data']['items'][int(f"{y}")]['postlocatie']['postcode']
            except KeyError:
                bzk_postal = "Ontbreekt"
            except IndexError:
                bzk_postal = "Ontbreekt"


            name_list.append(company_name)
            status_list.append(status)
            kvk_list.append(kvk_number)
            vorm_list.append(rechtsvorm)
            street_list.append(street)
            streetnr_list.append(streetnumber)
            town_list.append(town)
            postal_list.append(postal)
            bzk_street_list.append(bzk_street)
            bzk_streetnr_list.append(bzk_streetnumber)
            bzk_town_list.append(bzk_town)
            bzk_postal_list.append(bzk_postal)
            sbi1_list.append(sbi1)
            sbi2_list.append(sbi2)
            sbi3_list.append(sbi3)


dataset_complete = pd.DataFrame(
    {'status_list': status_list,
    'Kvk nummer': kvk_list,
    'Bedrijfsnaam': name_list,
    'vorm_list': vorm_list,
    'street_list': street_list,
    'streetnr_list': streetnr_list,
    'town_list': town_list,
    'postal_list': postal_list,
    'bzk_street_list': bzk_street_list,
    'bzk_streetnr_list': bzk_streetnr_list,
    'bzk_town_list': bzk_town_list,
    'bzk_postal_list': bzk_postal_list,
    'sbi1_list': sbi1_list,
    'sbi2_list': sbi2_list,
    'sbi3_list': sbi3_list,
    'results_list': results
    })

dataset_complete.to_csv('api_results.csv', mode='a', encoding='utf-8')