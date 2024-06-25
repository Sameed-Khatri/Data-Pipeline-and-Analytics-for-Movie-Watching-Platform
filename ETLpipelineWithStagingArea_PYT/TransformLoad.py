import pandas as pd
import numpy as np
import mysql.connector

# SECOND PART OF THE ETL PIPELINE WHERE THE DATA IS FETCHED FROM STAGING AREA, PROCESSED INTO THE RELEVANT DIMENSIONS, PUSHED INTO THE STAR SCHEMA DIMENSIONS.
# THEN CREATES A FACT TABLE DATAFRAME TO APPEND THE FOREIGN KEYS OF DIMENSIONS AND THEN PUSH THE FACT TABLE DATAFRAME TO THE FACT TABLE IN STAR SCHEMA


# LIVE
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="contentfactmartlive"
)
cursor = mydb.cursor()

# ARCHIVE
mydb2 = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="contentfactmart"
)
cursor2 = mydb2.cursor()


def copyDataToArchive():
    tables = ['contentfact', 'countrylist', 'genrelist', 'film', 'serie', 'country', 'date']
    try:
        mydb2.start_transaction()
        for table in tables:
            copy_sql = f"INSERT INTO {table} SELECT * FROM contentfactmartlive.{table}"
            cursor2.execute(copy_sql)
        
        mydb2.commit()
        print("Data copied successfully from live to archive database.\n")
    except mysql.connector.Error as err:
        print("Failed to copy data to archive database, rolling back transaction.")
        print(f"Error: {err}")
        mydb2.rollback()

def dumpLiveDB():
    mydb2.start_transaction()

    tables = ['contentfact', 'countrylist', 'genrelist', 'film', 'serie', 'country', 'date']
    try:
        for table in tables:
            sql = f"DELETE FROM {table}"
            cursor2.execute(sql)

        mydb2.commit()
        print("All tables cleared successfully.\n")
        copyDataToArchive()
    except mysql.connector.Error as err:
        print("Failed to clear tables, rolling back transaction.")
        print(f"Error: {err}")
        mydb2.rollback() 


def emptyLiveDB():
    mydb.start_transaction()

    tables = ['contentfact', 'countrylist', 'genrelist', 'film', 'serie', 'country', 'date']
    try:
        for table in tables:
            sql = f"DELETE FROM {table}"
            cursor.execute(sql)

        mydb.commit()
        print("All tables cleared successfully.\n")
    except mysql.connector.Error as err:
        print("Failed to clear tables, rolling back transaction.")
        print(f"Error: {err}")
        mydb.rollback()


def loadStarSchema():
    print("DUMPING TO ARCHIVE ....\n")
    # dumpLiveDB()
    emptyLiveDB()
    print("DONE DUMPING TO ARCHIVE\n")

    print("STARTING TO PUSH IN START SCHEMA ....\n")
    # LOAD FILM
    dfFilm=pd.read_csv('Film.csv')
    # Create a new DataFrame with just serieID and countryID
    dfFilmIDCountry = dfFilm[['filmID', 'countryID']].copy()
    dfFilmIDAddDate = dfFilm[['filmID', 'addDate']].copy()
    # Drop the countryID column from the main DataFrame (dfSerie)
    dfFilm = dfFilm.drop(columns=['countryID','addDate'])
    print("push film ....\n")
    sql_insert = """
        INSERT INTO film (filmID, title, duration, productionYear, ageCategoryName, filmDescription)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    for index, row in dfFilm.iterrows():
        cursor.execute(sql_insert, (
            row['filmID'],
            row['title'],
            row['duration'],
            row['productionYear'],
            row['ageCategory'],
            row['filmDescription']
        ))
    mydb.commit()
    print("done\n")
    # LOAD SERIE
    dfSerie=pd.read_csv('Serie.csv')
    # Create a new DataFrame with just serieID and countryID
    dfSerieIDCountry = dfSerie[['serieID', 'countryID']].copy()
    dfSerieIDAddDate = dfSerie[['serieID', 'addDate']].copy()
    # Drop the countryID column from the main DataFrame (dfSerie)
    dfSerie = dfSerie.drop(columns=['countryID','addDate'])
    print("push serie ....\n")
    sql_insert = """
        INSERT INTO serie (serieID, title, productionYear, seasons, serieDescription, ageCategoryName, avgEpisodeDuration)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    for index, row in dfSerie.iterrows():
        cursor.execute(sql_insert, (
            row['serieID'],
            row['title'],
            row['productionYear'],
            row['seasons'],
            row['serieDescription'],
            row['ageCategory'],
            row['avgEpisodeDuration']
        ))
    mydb.commit()
    print("done\n")

    # LOAD COUNTRY
    dfCountry=pd.read_csv('Country.csv')
    print("push country ....\n")
    sql_insert = """
        INSERT INTO country (countryID, name, continent, region)
        VALUES (%s, %s, %s, %s)
    """
    for index, row in dfCountry.iterrows():
        cursor.execute(sql_insert, (
            row['countryID'],
            row['name'],
            row['continent'],
            row['region']
        ))
    mydb.commit()
    print("done\n")

    # LOAD DATE
    dfDate=pd.read_csv('Date.csv')
    print("push date ....\n")
    sql_insert = """
        INSERT INTO date (dateID, date, year, month, day, qtr)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    for index, row in dfDate.iterrows():
        cursor.execute(sql_insert, (
            row['dateID'],
            row['date'],
            row['year'],
            row['month'],
            row['day'],
            row['qtr']
        ))
    mydb.commit()
    print("done\n")

    # LOAD COUNTRY LIST
    dfCountryList=pd.read_csv('CountryList.csv')
    dfCountryList['countryList'] = dfCountryList['countryList'].apply(
        lambda x: x.strip("[]").replace("'", "")
    )
    # DataFrame where filmID is not null
    dfCountryFilm = dfCountryList[dfCountryList['filmID'].notnull()][['countryListID', 'filmID']]
    dfCountryFilm['filmID'] = dfCountryFilm['filmID'].astype('Int64')
    # DataFrame where serieID is not null
    dfCountrySerie = dfCountryList[dfCountryList['serieID'].notnull()][['countryListID', 'serieID']]
    dfCountrySerie['serieID'] = dfCountrySerie['serieID'].astype('Int64')
    dfLoadCountryList=dfCountryList[['countryListID','countryList']].copy()
    print("push country list ....\n")
    sql_insert = """
        INSERT INTO countrylist (countryListID, countryList)
        VALUES (%s, %s)
    """
    for index, row in dfLoadCountryList.iterrows():
        cursor.execute(sql_insert, (
            row['countryListID'],
            row['countryList']
        ))
    mydb.commit()
    print("done\n")

    # LOAD GENRE LIST
    dfGenreList=pd.read_csv('GenreList.csv')
    dfGenreList['genreList'] = dfGenreList['genreList'].apply(
        lambda x: x.strip("[]").replace("'", "")
    )
    # DataFrame where filmID is not null
    dfGenreFilm = dfGenreList[dfGenreList['filmID'].notnull()][['genreListID', 'filmID']]
    dfGenreFilm['filmID'] = dfGenreFilm['filmID'].astype('Int64')
    # DataFrame where serieID is not null
    dfGenreSerie = dfGenreList[dfGenreList['serieID'].notnull()][['genreListID', 'serieID']]
    dfGenreSerie['serieID'] = dfGenreSerie['serieID'].astype('Int64')
    dfLoadGenreList=dfGenreList[['genreListID','genreList']].copy()
    print("push genre list ....\n")
    sql_insert = """
        INSERT INTO genrelist (genreListID, genreList)
        VALUES (%s, %s)
    """
    for index, row in dfLoadGenreList.iterrows():
        cursor.execute(sql_insert, (
            row['genreListID'],
            row['genreList']
        ))
    mydb.commit()
    print("done\n")

    # IMPORT STATS/FACTS
    dfFilmViewsTotal=pd.read_csv('FilmViewsTotal.csv')
    dfSerieViewsTotal=pd.read_csv('SerieViewsTotal.csv')

    dfFilmAvgRatings=pd.read_csv('FilmAvgRatings.csv')
    dfSerieAvgRatings=pd.read_csv('SerieAvgRatings.csv')

    dfFilmTotalRatings=pd.read_csv('FilmTotalRatings.csv')
    dfSerieTotalRatings=pd.read_csv('SerieTotalRatings.csv')

    dfFilmTotalWatchTime=pd.read_csv('FilmTotalWatchTime.csv')
    dfSerieTotalWatchTime=pd.read_csv('SerieTotalWatchTime.csv')

    dfFilmPeakWatchDate=pd.read_csv('FilmPeakWatchDate.csv')
    dfSeriePeakWatchDate=pd.read_csv('SeriePeakWatchDate.csv')

    # LOAD CONTENTFACT (FACT TABLE)
    columns = ['filmID', 'serieID', 'dateID', 'countryID', 'genreListID', 'countryListID',
           'totalViews', 'averageRating', 'totalRatingsCount', 'totalWatchTime', 'peakViewDate']

    # Initialize empty DataFrame
    dfFact = pd.DataFrame(columns=columns)
    dfFilm['serieID'] = np.nan
    dfSerie['filmID'] = np.nan

    # Use pd.concat to append dfFilm data to dfFact
    dfFact = pd.concat([dfFact, dfFilm[['filmID', 'serieID']]], ignore_index=True)
    # Use pd.concat to append dfSerie data to dfFact
    dfFact = pd.concat([dfFact, dfSerie[['filmID', 'serieID']]], ignore_index=True)
    # Convert the 'addDate' in dfFilmIDAddDate and 'date' in dfDate to datetime if not already
    dfFilmIDAddDate['addDate'] = pd.to_datetime(dfFilmIDAddDate['addDate'])
    dfDate['date'] = pd.to_datetime(dfDate['date'])

    # Merge on the date columns
    merged_df = pd.merge(dfFilmIDAddDate, dfDate, left_on='addDate', right_on='date', how='left')
    # Create a new DataFrame with just 'filmID' and 'dateID'
    df_film_date_id = merged_df[['filmID', 'dateID']]

    result = pd.merge(dfFact, df_film_date_id, on='filmID', how='left', suffixes=('', '_new'))
    # Update dateID in dfFact with the new values from the merge where applicable
    dfFact['dateID'] = result['dateID_new'].fillna(dfFact['dateID'])
    # Convert the 'addDate' in dfSerieIDAddDate and 'date' in dfDate to datetime if not already
    dfSerieIDAddDate['addDate'] = pd.to_datetime(dfSerieIDAddDate['addDate'])
    dfDate['date'] = pd.to_datetime(dfDate['date'])

    # Merge on the date columns
    merged_df = pd.merge(dfSerieIDAddDate, dfDate, left_on='addDate', right_on='date', how='left')
    # Create a new DataFrame with just 'filmID' and 'dateID'
    df_serie_date_id = merged_df[['serieID', 'dateID']]

    result = pd.merge(dfFact, df_serie_date_id, on='serieID', how='left', suffixes=('', '_new'))
    # Update dateID in dfFact with the new values from the merge where applicable
    dfFact['dateID'] = result['dateID_new'].fillna(dfFact['dateID'])

    # ADD COUNTRYID
    result1 = pd.merge(dfFact, dfSerieIDCountry, on='serieID', how='left', suffixes=('', '_new'))
    result2 = pd.merge(dfFact, dfFilmIDCountry, on='filmID', how='left', suffixes=('', '_new'))

    # Update dateID in dfFact with the new values from the merge where applicable
    dfFact['countryID'] = result1['countryID_new'].fillna(dfFact['countryID'])
    dfFact['countryID'] = result2['countryID_new'].fillna(dfFact['countryID'])

    # ADD GENRE LIST ID
    result1 = pd.merge(dfFact, dfGenreSerie, on='serieID', how='left', suffixes=('', '_new'))
    result2 = pd.merge(dfFact, dfGenreFilm, on='filmID', how='left', suffixes=('', '_new'))

    # Update genreListID in dfFact with the new values from the merge where applicable
    dfFact['genreListID'] = result1['genreListID_new'].fillna(dfFact['genreListID'])
    dfFact['genreListID'] = result2['genreListID_new'].fillna(dfFact['genreListID'])

    # ADD COUNTRY LIST ID
    result1 = pd.merge(dfFact, dfCountrySerie, on='serieID', how='left', suffixes=('', '_new'))
    result2 = pd.merge(dfFact, dfCountryFilm, on='filmID', how='left', suffixes=('', '_new'))

    # Update genreListID in dfFact with the new values from the merge where applicable
    dfFact['countryListID'] = result1['countryListID_new'].fillna(dfFact['countryListID'])
    dfFact['countryListID'] = result2['countryListID_new'].fillna(dfFact['countryListID'])

    # ADD TOTAL VIEWS
    result1 = pd.merge(dfFact, dfSerieViewsTotal, on='serieID', how='left', suffixes=('', '_new'))
    result2 = pd.merge(dfFact, dfFilmViewsTotal, on='filmID', how='left', suffixes=('', '_new'))
    # Update genreListID in dfFact with the new values from the merge where applicable
    dfFact['totalViews'] = result1['totalViews_new'].fillna(dfFact['totalViews'])
    dfFact['totalViews'] = result2['totalViews_new'].fillna(dfFact['totalViews'])

    # ADD AVERAGE RATING
    result1 = pd.merge(dfFact, dfSerieAvgRatings, on='serieID', how='left', suffixes=('', '_new'))
    result2 = pd.merge(dfFact, dfFilmAvgRatings, on='filmID', how='left', suffixes=('', '_new'))
    # Update genreListID in dfFact with the new values from the merge where applicable
    dfFact['averageRating'] = result1['avgRating'].fillna(dfFact['averageRating'])
    dfFact['averageRating'] = result2['avgRating'].fillna(dfFact['averageRating'])

    # ADD TOTAL RATINGS COUNT
    result1 = pd.merge(dfFact, dfSerieTotalRatings, on='serieID', how='left', suffixes=('', '_new'))
    result2 = pd.merge(dfFact, dfFilmTotalRatings, on='filmID', how='left', suffixes=('', '_new'))
    # Update genreListID in dfFact with the new values from the merge where applicable
    dfFact['totalRatingsCount'] = result1['totalRatings'].fillna(dfFact['totalRatingsCount'])
    dfFact['totalRatingsCount'] = result2['totalRatings'].fillna(dfFact['totalRatingsCount'])

    # ADD TOTAL WATCH TIME
    result1 = pd.merge(dfFact, dfSerieTotalWatchTime, on='serieID', how='left', suffixes=('', '_new'))
    result2 = pd.merge(dfFact, dfFilmTotalWatchTime, on='filmID', how='left', suffixes=('', '_new'))
    # Update genreListID in dfFact with the new values from the merge where applicable
    dfFact['totalWatchTime'] = result1['totalWatchTimeMinutes'].fillna(dfFact['totalWatchTime'])
    dfFact['totalWatchTime'] = result2['totalWatchTimeMinutes'].fillna(dfFact['totalWatchTime'])

    # ADD PEACK VIEW DATE
    result1 = pd.merge(dfFact, dfSeriePeakWatchDate, on='serieID', how='left', suffixes=('', '_new'))
    result2 = pd.merge(dfFact, dfFilmPeakWatchDate, on='filmID', how='left', suffixes=('', '_new'))
    # Update genreListID in dfFact with the new values from the merge where applicable
    dfFact['peakViewDate'] = result1['peakViewDate_new'].fillna(dfFact['peakViewDate'])
    dfFact['peakViewDate'] = result2['peakViewDate_new'].fillna(dfFact['peakViewDate'])

    # FINAL PUSH
    print("push content fact ....\n")
    sql_insert = """
        INSERT INTO contentfact (filmID,serieID,dateID,countryID,genreListID,countryListID,totalViews,averageRating,totalRatingsCount,totalWatchTime,peakViewDate)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    for index, row in dfFact.iterrows():
        cursor.execute(sql_insert, (
            row['filmID'],
            row['serieID'],
            row['dateID'],
            row['countryID'],
            row['genreListID'],
            row['countryListID'],
            row['totalViews'],
            row['averageRating'],
            row['totalRatingsCount'],
            row['totalWatchTime'],
            row['peakViewDate']
        ))
    mydb.commit()
    print("done\n")
    print("DONE PUSHING IN START SCHEMA\n")

    
if __name__ == "__main__":
    loadStarSchema()
    # Close the cursor and connection
    cursor.close()
    mydb.close()