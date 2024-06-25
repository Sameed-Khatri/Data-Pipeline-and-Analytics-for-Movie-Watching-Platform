import pandas as pd
import mysql.connector

# FIRST PART OF THE ETL PIPELINE WHICH EXTRACTS DATA FROM THE SPAGHETTI, PREPROCESS THE DATA AND PUSHES IT INTO THE STAGING AREA (respective csv files)

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="dwh"
)
cursor = mydb.cursor()

def processWatchingHistory():
    print("processing watching history...")
    sql_query = "SELECT * FROM watchinghistory where year(startTime) between (year(startTime)-5) and year(now())"
    dfWatchingHistory = pd.read_sql(sql_query, con=mydb)
    # Calculate the average rating per film (only where filmID is not null)
    film_avg_ratings = dfWatchingHistory[dfWatchingHistory['filmID'].notnull()].groupby('filmID')['ratings'].mean()
    # Calculate the average rating per episode (only where episodeID is not null)
    episode_avg_ratings = dfWatchingHistory[dfWatchingHistory['episodeID'].notnull()].groupby('episodeID')['ratings'].mean()
    # Define a function that fills missing ratings based on film or episode averages
    def fill_missing_ratings(row):
        if pd.isna(row['ratings']):
            if pd.notna(row['filmID']) and row['filmID'] in film_avg_ratings:
                return film_avg_ratings[row['filmID']]
            elif pd.notna(row['episodeID']) and row['episodeID'] in episode_avg_ratings:
                return episode_avg_ratings[row['episodeID']]
        return row['ratings']
    # Apply the function to fill missing ratings in the DataFrame
    dfWatchingHistory['ratings'] = dfWatchingHistory.apply(fill_missing_ratings, axis=1)
    dfWatchingHistory=dfWatchingHistory.dropna(subset=['ratings', 'startTime'], how='any')
    dfWatchingHistory.to_csv('WatchingHistory.csv',index=False)
    print("done processing watching history\n")


def processFilm():
    print("processing film...")
    sql_query = "SELECT f.*,a.name as ageCategory FROM film f join agecategory a on f.ageCategoryID=a.ageCategoryID where year(addDate) between (year(now())-5) and year(now())"
    dfFilm = pd.read_sql(sql_query, con=mydb)
    columns_to_drop = ['ageCategoryID', 'filmPhoto']
    dfFilm = dfFilm.drop(columns=columns_to_drop, axis=1)
    # add UNK-Unknown in missing values of countryID and ageCategory to make anothe category
    dfFilm['countryID'] = dfFilm['countryID'].fillna('UNK')
    dfFilm['ageCategory'] = dfFilm['ageCategory'].fillna('UNK')
    # Drop rows with missing values in 'title', 'duration', and 'addDate'
    dfFilm = dfFilm.dropna(subset=['title', 'duration', 'addDate'], how='any')
    dfFilm.to_csv('Film.csv',index=False)
    print("done processing film\n")


def processSerie():
    print("processing serie...")
    sql_query = "SELECT f.*,a.name as ageCategory FROM serie f join agecategory a on f.ageCategoryID=a.ageCategoryID where year(addDate) between (year(now())-5) and year(now())"
    dfSerie = pd.read_sql(sql_query, con=mydb)
    sql_query2 = "select avg(episodeDuration)as avgEpisodeDuration,serieID from episode group by serieID"
    dftemp = pd.read_sql(sql_query2, con=mydb)
    columns_to_drop = ['ageCategoryID', 'seriePhoto']
    dfSerie = dfSerie.drop(columns=columns_to_drop, axis=1)
    # add UNK-Unknown in missing values of countryID and ageCategory to make another category
    dfSerie['countryID'] = dfSerie['countryID'].fillna('UNK')
    dfSerie['ageCategory'] = dfSerie['ageCategory'].fillna('UNK')
    # Drop rows with missing values in 'title', 'duration', and 'addDate'
    dfSerie = dfSerie.dropna(subset=['title','addDate'], how='any')
    dfSerie = dfSerie.merge(dftemp, on='serieID', how='left')
    dfSerie.to_csv('Serie.csv',index=False)
    print("done processing serie\n")

#
def processCountry():
    print("processing country...")
    sql_query = "select * from country"
    dfCountry = pd.read_sql(sql_query, con=mydb)
    dfCountry.to_csv('Country.csv',index=False)
    print("done processing country\n")


def processDate():
    print("processing date...")
    sql_query = "select * from date where year between (year(now())-5) and year(now())"
    dfDate = pd.read_sql(sql_query, con=mydb)
    dfDate.to_csv('Date.csv',index=False)
    print("done processing date\n")

def processEpisode():
    print("processing episode...")
    sql_query = "select e.* from episode e join serie s on e.serieID=s.serieID where year(s.addDate) between (year(now())-5) and year(now())"
    dfEpisode = pd.read_sql(sql_query, con=mydb)
    # Drop the columns
    columns_to_drop = ['episodePhoto']
    dfEpisode = dfEpisode.drop(columns=columns_to_drop, axis=1)
    # Drop rows with missing values in 'title', 'duration', and 'addDate'
    dfEpisode = dfEpisode.dropna(subset=['episodeTitle','episodeDuration','serieID','seasonNumber'], how='any')
    dfEpisode.to_csv('Episode.csv',index=False)
    print("done processing epsiode\n")

# CREATING DATA FOR GENRE LIST DIMENSION
def makeGenreList():
    print("making genre list...")
    # FOR FILM
    # Query to get film-genre pairs
    sql_query = "SELECT f.filmID, g.genreName FROM filmgenre f JOIN genre g ON f.genreID = g.genreID join film m on f.filmID=m.filmID where year(addDate) between (year(now())-5) and year(now())"
    dfFilmGenre = pd.read_sql(sql_query, con=mydb)
    # Group by filmID and aggregate genres into a list
    dfFilmGenreList = dfFilmGenre.groupby('filmID')['genreName'].agg(list).reset_index()
    dfFilmGenreList['genreListID'] = range(1, len(dfFilmGenreList) + 1)
    dfFilmGenreList.rename(columns={'genreName': 'genreList'}, inplace=True)

    # FOR SERIE
    # Query to get film-genre pairs
    sql_query = "SELECT s.serieID, g.genreName FROM seriegenre s JOIN genre g ON s.genreID = g.genreID join serie r on s.serieID = r.serieID where year(addDate) between (year(now())-5) and year(now())"
    dfSerieGenre = pd.read_sql(sql_query, con=mydb)
    # Group by filmID and aggregate genres into a list
    dfSerieGenreList = dfSerieGenre.groupby('serieID')['genreName'].agg(list).reset_index()
    dfSerieGenreList['genreListID'] = range(1, len(dfSerieGenreList) + 1)
    dfSerieGenreList.rename(columns={'genreName': 'genreList'}, inplace=True)

    # CREATE EMPTY DATAFRAME
    columns = ['genreListID', 'serieID', 'filmID', 'genreList']
    # Create an empty DataFrame with the specified columns
    dfEmpty = pd.DataFrame(columns=columns)

    # CREATE A COMBINED GENRELIST DATAFRAME FOR BOTH FILM AND SERIE
    start_id = 1
    new_rows = []
    # Prepare data with filmID and genreList, and add incrementing genreListID
    for index, row in dfFilmGenreList.iterrows():
        new_row = {'genreListID': start_id, 'serieID': None, 'filmID': row['filmID'], 'genreList': row['genreList']}
        new_rows.append(new_row)
        start_id += 1
    # Prepare data with serieID and genreList, and add incrementing genreListID
    for index, row in dfSerieGenreList.iterrows():
        new_row = {'genreListID': start_id, 'serieID': row['serieID'], 'filmID': None, 'genreList': row['genreList']}
        new_rows.append(new_row)
        start_id += 1
    dfNewRows = pd.DataFrame(new_rows)
    # Concatenate the new rows with the original empty DataFrame
    dfGenreList = pd.concat([dfEmpty, dfNewRows], ignore_index=True)

    # ADJUST DATATYPES
    dfGenreList['serieID'] = dfGenreList['serieID'].astype('Int64')
    dfGenreList['filmID'] = dfGenreList['filmID'].astype('Int64')
    dfGenreList['serieID'] = dfGenreList['serieID'].replace({pd.NA: None})
    dfGenreList['filmID'] = dfGenreList['filmID'].replace({pd.NA: None})
    dfGenreList.to_csv('GenreList.csv',index =False)
    print("done making genre list\n")

# CREATING DATA FRO COUTRY LIST DIMENSION
def makeCountryList():
    print("making country list...")
    # FOR FILM
    # Query to get film-genre pairs
    sql_query = "select f.filmID,c.name as countryName from filmincountry f join country c on f.countryID=c.countryID join film m on f.filmID=m.filmID where year(addDate) between (year(now())-5) and year(now())"
    dfFilmCountry = pd.read_sql(sql_query, con=mydb)
    # Group by filmID and aggregate genres into a list
    dfFilmCountryList = dfFilmCountry.groupby('filmID')['countryName'].agg(list).reset_index()
    dfFilmCountryList.rename(columns={'countryName': 'countryList'}, inplace=True)

    # FOR SERIE
    # Query to get film-genre pairs
    sql_query = "select s.serieID,c.name as countryName from serieincountry s join country c on s.countryID=c.countryID join serie r on s.serieID = r.serieID where year(addDate) between (year(now())-5) and year(now())"
    dfSerieCountry = pd.read_sql(sql_query, con=mydb)
    # Group by filmID and aggregate genres into a list
    dfSerieCountryList = dfSerieCountry.groupby('serieID')['countryName'].agg(list).reset_index()
    dfSerieCountryList.rename(columns={'countryName': 'countryList'}, inplace=True)

    # CREATE EMPTY DATAFRAME
    columns = ['countryListID', 'serieID', 'filmID', 'countryList']
    # Create an empty DataFrame with the specified columns
    dfEmpty2 = pd.DataFrame(columns=columns)

    # CREATE A COMBINED GENRELIST DATAFRAME FOR BOTH FILM AND SERIE
    start_id = 1
    new_rows = []
    # Prepare data with filmID and genreList, and add incrementing genreListID
    for index, row in dfFilmCountryList.iterrows():
        new_row = {'countryListID': start_id, 'serieID': None, 'filmID': row['filmID'], 'countryList': row['countryList']}
        new_rows.append(new_row)
        start_id += 1
    # Prepare data with serieID and genreList, and add incrementing genreListID
    for index, row in dfSerieCountryList.iterrows():
        new_row = {'countryListID': start_id, 'serieID': row['serieID'], 'filmID': None, 'countryList': row['countryList']}
        new_rows.append(new_row)
        start_id += 1
    # Create a DataFrame from the collected new rows
    dfNewRows = pd.DataFrame(new_rows)
    # Concatenate the new rows with the original empty DataFrame
    dfCountryList = pd.concat([dfEmpty2, dfNewRows], ignore_index=True)

    # ADJUST DATATYPES
    dfCountryList['serieID'] = dfCountryList['serieID'].astype('Int64')
    dfCountryList['filmID'] = dfCountryList['filmID'].astype('Int64')
    dfCountryList['serieID'] = dfCountryList['serieID'].replace({pd.NA: None})
    dfCountryList['filmID'] = dfCountryList['filmID'].replace({pd.NA: None})
    dfCountryList.to_csv('CountryList.csv',index=False)
    print("done making country list\n")


def processFacts():
    print("calculating facts...")
    # total views
    sql_query = "select filmID,count(*) as totalViews from watchinghistory where filmID is not null group by filmID"
    dfFilmViewsTotal = pd.read_sql(sql_query, con=mydb)
    dfFilmViewsTotal.to_csv('FilmViewsTotal.csv',index=False)
    sql_query = "select e.serieID,count(*) as totalViews from watchinghistory w join episode e on w.episodeID=e.episodeID where w.episodeID is not null group by serieID"
    dfSerieViewsTotal = pd.read_sql(sql_query, con=mydb)
    dfSerieViewsTotal.to_csv('SerieViewsTotal.csv',index=False)
    # avg ratings
    sql_query = "select filmID,avg(ratings) as avgRating from watchinghistory where filmID is not null group by filmID"
    dfFilmAvgRatings = pd.read_sql(sql_query, con=mydb)
    dfFilmAvgRatings.to_csv('FilmAvgRatings.csv',index=False)
    sql_query = "select e.serieID,avg(ratings) as avgRating from watchinghistory w join episode e on w.episodeID=e.episodeID where w.episodeID is not null group by serieID"
    dfSerieAvgRatings = pd.read_sql(sql_query, con=mydb)
    dfSerieAvgRatings.to_csv('SerieAvgRatings.csv',index=False)
    # total ratings
    sql_query = "select filmID,count(ratings) as totalRatings from watchinghistory where filmID is not null group by filmID"
    dfFilmTotalRatings = pd.read_sql(sql_query, con=mydb)
    dfFilmTotalRatings.to_csv('FilmTotalRatings.csv',index=False)
    sql_query = "select e.serieID,count(ratings) as totalRatings from watchinghistory w join episode e on w.episodeID=e.episodeID where w.episodeID is not null group by serieID"
    dfSerieTotalRatings = pd.read_sql(sql_query, con=mydb)
    dfSerieTotalRatings.to_csv('SerieTotalRatings.csv',index=False)
    # total watch time
    sql_query = "select filmID,SUM(TIMESTAMPDIFF(MINUTE, startTime, endTime)) AS totalWatchTimeMinutes from watchinghistory where filmID is not null group by filmID"
    dfFilmTotalWatchTime = pd.read_sql(sql_query, con=mydb)
    dfFilmTotalWatchTime.to_csv('FilmTotalWatchTime.csv',index=False)
    sql_query = "select e.serieID,SUM(TIMESTAMPDIFF(MINUTE, startTime, endTime)) AS totalWatchTimeMinutes from watchinghistory w join episode e on w.episodeID=e.episodeID where w.episodeID is not null group by serieID"
    dfSerieTotalWatchTime = pd.read_sql(sql_query, con=mydb)
    dfSerieTotalWatchTime.to_csv('SerieTotalWatchTime.csv',index=False)
    # peak view date
    sql_query = "WITH FilmViewing AS (SELECT filmID, DATE(startTime) AS viewDate, COUNT(*) AS viewCount, ROW_NUMBER() OVER (PARTITION BY filmID ORDER BY COUNT(*) DESC, DATE(startTime)) AS row_num FROM watchinghistory WHERE filmID IS NOT NULL GROUP BY filmID, DATE(startTime)) SELECT filmID,viewDate AS peakViewDate FROM FilmViewing WHERE row_num = 1 ORDER BY filmID"
    dfFilmPeakWatchDate = pd.read_sql(sql_query, con=mydb)
    dfFilmPeakWatchDate.to_csv('FilmPeakWatchDate.csv',index=False)
    sql_query = "WITH FilmViewing AS (SELECT e.serieID,DATE(startTime) AS viewDate,COUNT(*) AS viewCount,ROW_NUMBER() OVER (PARTITION BY e.serieID ORDER BY COUNT(*) DESC, DATE(startTime)) AS row_num FROM watchinghistory w join episode e on w.episodeID=e.episodeID WHERE w.episodeID IS NOT NULL GROUP BY serieID, DATE(startTime)) SELECT serieID,viewDate AS peakViewDate FROM FilmViewing WHERE row_num = 1 ORDER BY serieID;"
    dfSeriePeakWatchDate = pd.read_sql(sql_query, con=mydb)
    dfSeriePeakWatchDate.to_csv('SeriePeakWatchDate.csv',index=False)
    print("done calculating facts\n")



if __name__ == "__main__":
    processWatchingHistory()
    processFilm()
    processSerie()
    processCountry()
    processDate()
    processEpisode()
    makeGenreList()
    makeCountryList()
    processFacts()

    cursor.close()
    mydb.close()