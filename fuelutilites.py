import requests

class Asda:
    URL = "https://storelocator.asda.com/fuel_prices_data.json"

    @staticmethod
    def get_data_json():
        asda_url = Asda.URL
        asda_data = requests.get(asda_url).json()
        return asda_data

    @staticmethod
    def process_and_write_to_db(asda_data, db_cursor, TotalEntries, db):

        stations = asda_data['stations']
        print("Detected Stations For AsdaData:", len(stations))
        TotalEntries = TotalEntries + len(stations)    


        for station in stations:
            last_updated = asda_data['last_updated']
            site_id = station['site_id']
            brand = station['brand']
            address = station['address']
            latitude = float(station['location']['latitude'])
            longitude = float(station['location']['longitude'])
            postcode = station['postcode']

            e10_price = float(station['prices'].get('E10', 999))
            e5_price = float(station['prices'].get('E5', 999))# This value seems to be missing in the original code
            b7_price = float(station['prices'].get('B7', 999))

            db_cursor.execute("""
                INSERT INTO all_fuel_price_stations (DataTime, site_id, brand, address, postcode, latitude, longitude, E10, E5, B7) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(site_id) DO UPDATE SET 
                DataTime = excluded.DataTime, 
                brand = excluded.brand, 
                address = excluded.address, 
                postcode = excluded.postcode, 
                latitude = excluded.latitude, 
                longitude = excluded.longitude, 
                E10 = excluded.E10, 
                E5 = excluded.E5, 
                B7 = excluded.B7
            """, (last_updated, site_id, brand, address, postcode, latitude, longitude, e10_price, e5_price, b7_price))

        db.commit()
        return TotalEntries

class bp:
    URL = "	https://www.bp.com/en_gb/united-kingdom/home/fuelprices/fuel_prices_data.json"

    @staticmethod
    def get_data_json():
        bp_url = bp.URL
        bp_data = requests.get(bp_url).json()
        return bp_data

    @staticmethod
    def process_and_write_to_db(bp_data, db_cursor, TotalEntries, db):
        stations = bp_data['stations']
        print("Detected Stations For BPData:", len(stations))
        TotalEntries = TotalEntries + len(stations)

        for station in stations:
            last_updated = bp_data['last_updated']
            site_id = station['site_id']
            brand = station['brand']
            address = station['address']
            latitude = float(station['location']['latitude'])
            longitude = float(station['location']['longitude'])
            postcode = station['postcode']

            e10_price = float(station['prices'].get('E10', 999))
            e5_price = float(station['prices'].get('E5', 999))# This value seems to be missing in the original code
            b7_price = float(station['prices'].get('B7', 999))

            db_cursor.execute("""
                INSERT INTO all_fuel_price_stations (DataTime, site_id, brand, address, postcode, latitude, longitude, E10, E5, B7) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(site_id) DO UPDATE SET 
                DataTime = excluded.DataTime, 
                brand = excluded.brand, 
                address = excluded.address, 
                postcode = excluded.postcode, 
                latitude = excluded.latitude, 
                longitude = excluded.longitude, 
                E10 = excluded.E10, 
                E5 = excluded.E5, 
                B7 = excluded.B7
            """, (last_updated, site_id, brand, address, postcode, latitude, longitude, e10_price, e5_price, b7_price))

        db.commit()
        return TotalEntries

class AppleGreens:
    URL = "https://applegreenstores.com/fuel-prices/data.json"

    @staticmethod
    def get_data_json():
        apple_url = AppleGreens.URL
        apple_data = requests.get(apple_url).json()
        return apple_data

    @staticmethod
    def process_and_write_to_db(apple_data, db_cursor, TotalEntries, db):
        stations = apple_data['stations']
        print("Detected Stations For AppleGreensData:", len(stations))
        TotalEntries = TotalEntries + len(stations)


        for station in stations:
            last_updated = apple_data['last_updated']
            site_id = station['site_id']
            brand = station['brand']
            address = station['address']
            latitude = float(station['location']['latitude'])
            longitude = float(station['location']['longitude'])
            postcode = station['postcode']

            e10_price = float(station['prices'].get('E10', 999))
            e5_price = float(station['prices'].get('E5', 999))
            b7_price = float(station['prices'].get('B7', 999))

            db_cursor.execute("""
                INSERT INTO all_fuel_price_stations (DataTime, site_id, brand, address, postcode, latitude, longitude, E10, E5, B7) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(site_id) DO UPDATE SET 
                DataTime = excluded.DataTime, 
                brand = excluded.brand, 
                address = excluded.address, 
                postcode = excluded.postcode, 
                latitude = excluded.latitude, 
                longitude = excluded.longitude, 
                E10 = excluded.E10, 
                E5 = excluded.E5, 
                B7 = excluded.B7
            """, (last_updated, site_id, brand, address, postcode, latitude, longitude, e10_price, e5_price, b7_price))

           

        db.commit()
        return TotalEntries

class Ascona:
    URL = "https://fuelprices.asconagroup.co.uk/newfuel.json"

    @staticmethod
    def get_data_json():
        ascona_url = Ascona.URL
        ascona_data = requests.get(ascona_url).json()
        return ascona_data
    @staticmethod
    def process_and_write_to_db(ascona_data, db_cursor, TotalEntries, db):
        stations = ascona_data['stations']
        print("Detected Stations For AsconaData:", len(stations))
        TotalEntries = TotalEntries + len(stations)

        for station in stations:
            last_updated = ascona_data['last_updated']
            site_id = station['site_id']
            brand = station['brand']
            address = station['address']
            latitude = float(station['location']['latitude'])
            longitude = float(station['location']['longitude'])
            postcode = station['postcode']

            e10_price = float(station['prices'].get('E10', 999))
            e5_price = float(station['prices'].get('E5', 999))  # This value seems to be missing in the original code
            b7_price = float(station['prices'].get('B7', 999))

            db_cursor.execute("""
                INSERT INTO all_fuel_price_stations (DataTime, site_id, brand, address, postcode, latitude, longitude, E10, E5, B7) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (site_id) DO UPDATE SET 
                DataTime = excluded.DataTime, 
                brand = excluded.brand, 
                address = excluded.address, 
                postcode = excluded.postcode, 
                latitude = excluded.latitude, 
                longitude = excluded.longitude, 
                E10 = excluded.E10, 
                E5 = excluded.E5, 
                B7 = excluded.B7
            """, (last_updated, site_id, brand, address, postcode, latitude, longitude, e10_price, e5_price, b7_price))

        db.commit()
        return TotalEntries

class Esso:
    URL = "https://fuelprices.esso.co.uk/latestdata.json"

    @staticmethod
    def get_data_json():
        esso_url = Esso.URL
        esso_data = requests.get(esso_url).json()
        return esso_data
    @staticmethod
    def process_and_write_to_db(esso_data, db_cursor, TotalEntries, db):
        stations = esso_data['stations']
        print("Detected Stations For EssoData:", len(stations))
        TotalEntries = TotalEntries + len(stations)

        for station in stations:
            last_updated = esso_data['last_updated']
            site_id = station['site_id']
            brand = station['brand']
            address = station['address']
            latitude = float(station['location']['latitude'])
            longitude = float(station['location']['longitude'])
            postcode = station['postcode']                                                           

            e10_price = float(station['prices'].get('E10', 999))
            e5_price = float(station['prices'].get('E5', 999))  # This value seems to be missing in the original code
            b7_price = float(station['prices'].get('B7', 999))

            db_cursor.execute("""
                INSERT INTO all_fuel_price_stations (DataTime, site_id, brand, address, postcode, latitude, longitude, E10, E5, B7) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (site_id) DO UPDATE SET 
                DataTime = excluded.DataTime, 
                brand = excluded.brand, 
                address = excluded.address, 
                postcode = excluded.postcode, 
                latitude = excluded.latitude, 
                longitude = excluded.longitude, 
                E10 = excluded.E10, 
                E5 = excluded.E5, 
                B7 = excluded.B7
            """, (last_updated, site_id, brand, address, postcode, latitude, longitude, e10_price, e5_price, b7_price))

        db.commit()
        return TotalEntries

class Morrisions:
    URL = "	https://www.morrisons.com/fuel-prices/fuel.json"

    @staticmethod
    def get_data_json():
        Morrisions_url = Morrisions.URL
        Morrisions_data = requests.get(Morrisions_url).json()
        return Morrisions_data
    @staticmethod
    def process_and_write_to_db(Morisions_data, db_cursor, TotalEntries, db):
        stations = Morisions_data['stations']
        print("Detected Stations For Morrisions:", len(stations))
        TotalEntries = TotalEntries + len(stations)

        for station in stations:
            last_updated = Morisions_data['last_updated']
            site_id = station['site_id']
            brand = station['brand']
            address = station['address']
            latitude = float(station['location']['latitude'])
            longitude = float(station['location']['longitude'])
            postcode = station['postcode']

            e10_price = float(station['prices'].get('E10', 999))
            e5_price = float(station['prices'].get('E5', 999))  # This value seems to be missing in the original code
            b7_price = float(station['prices'].get('B7', 999))

            db_cursor.execute("""
                INSERT INTO all_fuel_price_stations (DataTime, site_id, brand, address, postcode, latitude, longitude, E10, E5, B7) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (site_id) DO UPDATE SET 
                DataTime = excluded.DataTime, 
                brand = excluded.brand, 
                address = excluded.address, 
                postcode = excluded.postcode, 
                latitude = excluded.latitude, 
                longitude = excluded.longitude, 
                E10 = excluded.E10, 
                E5 = excluded.E5, 
                B7 = excluded.B7
            """, (last_updated, site_id, brand, address, postcode, latitude, longitude, e10_price, e5_price, b7_price))

        db.commit()
        return TotalEntries

class MFG:
    URL = "https://fuelprices.esso.co.uk/latestdata.json"

    @staticmethod
    def get_data_json():
        MFG_url = Esso.URL
        MFG_data = requests.get(MFG_url).json()
        return MFG_data
    @staticmethod
    def process_and_write_to_db(MFG_data, db_cursor, TotalEntries, db):
        stations = MFG_data['stations']
        print("Detected Stations For MFGData:", len(stations))
        TotalEntries = TotalEntries + len(stations)


        for station in stations:
            last_updated = MFG_data['last_updated']
            site_id = station['site_id']
            brand = station['brand']
            address = station['address']
            latitude = float(station['location']['latitude'])
            longitude = float(station['location']['longitude'])
            postcode = station['postcode']

            e10_price = float(station['prices'].get('E10', 999))
            e5_price = float(station['prices'].get('E5', 999))  # This value seems to be missing in the original code
            b7_price = float(station['prices'].get('B7', 999))

            db_cursor.execute("""
                INSERT INTO all_fuel_price_stations (DataTime, site_id, brand, address, postcode, latitude, longitude, E10, E5, B7) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (site_id) DO UPDATE SET 
                DataTime = excluded.DataTime, 
                brand = excluded.brand, 
                address = excluded.address, 
                postcode = excluded.postcode, 
                latitude = excluded.latitude, 
                longitude = excluded.longitude, 
                E10 = excluded.E10, 
                E5 = excluded.E5, 
                B7 = excluded.B7
            """, (last_updated, site_id, brand, address, postcode, latitude, longitude, e10_price, e5_price, b7_price))

        db.commit()
        return TotalEntries