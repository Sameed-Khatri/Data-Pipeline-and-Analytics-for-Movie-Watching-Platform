import pandas as pd
import mysql.connector

# connecting to live datamart
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="contentfactmartlive"
)
cursor = mydb.cursor()

def generateFactTableSnapshot():
    query = """select cf.*,c.name as countryName,c.continent,c.region,d.date,d.year,d.month,d.day,d.qtr,cl.countryList,gl.genreList,
                s.title as SerieTitle,s.avgEpisodeDuration as SerieAvgEpisodeDuration,s.productionYear as SerieProductionYear,s.ageCategoryName as SerieAgeCategoryName,s.serieDescription,s.seasons,
                f.title as FilmTitle,f.duration as FilmDuration,f.productionYear as FilmProductionYear,f.ageCategoryName as FilmAgeCategoryName,f.filmDescription from contentfact cf
                join country c on cf.countryID=c.countryID
                join date d on cf.dateID=d.dateID
                join countrylist cl on cf.countryListID=cl.countryListID
                join genrelist gl on cf.genreListID=gl.genreListID
                left join serie s on cf.serieID=s.serieID
                left join film f on cf.filmID=f.filmID;"""
    dfFactTableSnapshot = pd.read_sql(query,con=mydb)
    dfFactTableSnapshot.fillna('-', inplace=True)
    dfFactTableSnapshot.to_csv('FactTableSnapshot.csv',index=False)

if __name__ == "__main__":
    generateFactTableSnapshot()
    cursor.close()
    mydb.close()