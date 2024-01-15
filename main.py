import requests
import sqlite3
import fuelutilites as fu


# Initialize the SQL Connection
db = sqlite3.connect("FuelPriceDB.db")
dbCursor = db.cursor()

#Declare Varible
TotalEntries = 0
Version = "v1"
dbFile = "FuelPriceDB"
BuildType = "Development"

#Declare Code Info
print("Program Versions: ", Version)
print("Connected Database: ", dbFile)
print("Build Type: ", BuildType)

#Try And Except Rules For all Functions
print("------------------------------------------------------------")
try:
    print("Retrieving Asda Data...")
    asda_data = fu.Asda.get_data_json()

    if asda_data:
        print("Data Retrieved From Asda")
        TotalEntries = fu.Asda.process_and_write_to_db(asda_data, dbCursor, TotalEntries, db)
        print("Asda Data Written to DB")
    else:
        print("Data Request To Asda API Failed")
except Exception as e:
    print(f"An error occurred While Connecting To Asda API: {e}")
print("------------------------------------------------------------")
try:
    print("Retrieving MFG Data...")
    MFG_data = fu.MFG.get_data_json()

    if MFG_data:
        print("Data Retrieved From MFG")
        TotalEntries = fu.MFG.process_and_write_to_db(MFG_data, dbCursor, TotalEntries, db)
        print("MFG Data Written to DB")
    else:
        print("Data Request To MFG API Failed")
except Exception as e:
    print("Data Request To Morisions API Failed")
print("------------------------------------------------------------")
try:
    print("Retrieving Morrisions Data...")
    Morrisions_data = fu.Morrisions.get_data_json()

    if Morrisions_data:
        print("Data Retrieved From Morrisions")
        TotalEntries = fu.Morrisions.process_and_write_to_db(Morrisions_data, dbCursor, TotalEntries, db)
        print("Morisions Data Written to DB")
    else:
        print("Data Request To Morisions API Failed")
except Exception as e:
    print(f"An error occurred While Connecting To Morrisions API: {e}")
print("------------------------------------------------------------")
try:
    print("Retrieving esso Data...")
    Esso_data = fu.Esso.get_data_json()

    if Esso_data:
        print("Data Retrieved From Esso")
        TotalEntries = fu.Esso.process_and_write_to_db(Esso_data, dbCursor, TotalEntries, db)
        print("Esso Data Written to DB")
    else:
        print("Data Request To Esso API Failed")
except Exception as e:
    print(f"An error occurred While Connecting To Esso API: {e}")
print("------------------------------------------------------------")
try:
    print("Retrieving bp Data...")
    bp_data = fu.bp.get_data_json()

    if bp_data:
        print("Data Retrieved From bp")
        TotalEntries = fu.bp.process_and_write_to_db(bp_data, dbCursor, TotalEntries, db)
        print("bp Data Written to DB")
    else:
        print("Data Request To bp API Failed")
except Exception as e:
    print(f"An error occurred While Connecting To bp API: {e}")
print("------------------------------------------------------------")
try:
    print("Retrieving Ascona Data...")
    ascona_data = fu.Ascona.get_data_json()
    if ascona_data:
        print("Data Retrieved From Ascona")
        TotalEntries = fu.Ascona.process_and_write_to_db(ascona_data, dbCursor, TotalEntries, db)
        print("Ascona Data Written to DB")
    else:
        print("Data Request To AsconaAPI Failed")
except Exception as e:
    print(f"An error occurred While Connecting To Ascona API: {e}")
print("------------------------------------------------------------")
try:
    print("Retrieving Apple Greens Data...")
    apple_data = fu.AppleGreens.get_data_json()

    if apple_data:
        print("Data Retrieved From Apple Greens")
        TotalEntries = fu.AppleGreens.process_and_write_to_db(apple_data, dbCursor, TotalEntries, db)
        print("Written to DB")
    else:
        print("Data Request To Apple Greens Failed")
    print("------------------------------------------------------------")
except Exception as e:
    print(f"An error occurred While Connecting to Appple Greens API: {e}")
    print("------------------------------------------------------------")
finally:
    #Code To End The Try And Except
    db.close()
    print("Database Connection Closed Succesfully")
    print("Total Database Entrie's Writen:", TotalEntries)
    print("Exiting Program")
    exit(1)