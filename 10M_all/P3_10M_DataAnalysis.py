
!/usr/bin/env python3
# coding: utf-8

# # Project3_10M_MovieLens Data Analysis
### Author: Farhana Alam

# arg1:movies.dat, arg2:ratings.dat, arg3:tags.dat, arg4:output_10M

# Running on cscluster hdfs with command: spark-submit --master spark://cscluster00.boisestate.edu:7077 P3_10M_DataAnalysis.py hdfs://cscluster00:9000/user/farhanaalam/10M_all/movies.dat hdfs://cscluster00:9000/user/farhanaalam/10M_all/ratings.dat hdfs://cscluster00:9000/user/farhanaalam/10M_all/tags.dat hdfs://cscluster00:9000/user/farhanaalam/output_10M_hdfs

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as func
import sys
k = 10


# Creating a Spark session
spark = (SparkSession
        .builder
        .appName("Spark Project3 10M Movie Data Analysis")
        .getOrCreate())

# Loading the 10M MovieLens DataFiles
#movies_file = "Documents/CS535/movie_data/ml-10m/movies.dat"  # sys.argv[1]
#ratings_file = "Documents/CS535/movie_data/ml-10m/ratings.dat" # sys.argv[2]
#tags_file = "Documents/CS535/movie_data/ml-10m/tags.dat"  # sys.argv[3]

# Output CSV file path
#output_csv_path = "Documents/CS535/movie_data/ml-10m-output"  # sys.argv[4]

# Loading DataFiles with argv 
movies_file = sys.argv[1]
ratings_file = sys.argv[2]
tags_file = sys.argv[3]

# Output CSV file path
output_csv_path = sys.argv[4]

#ratings datafiles to dataframes
ratings_df = (spark.read.format("csv")
    .option("sep", "::")
    .option("inferschema", "true")
    .option("samplingRatio", 0.1)  # Adjust the sampling ratio
    .load(ratings_file)
    .toDF("UserID", "MovieID", "Rating", "Rating_Timestamp"))

ratings_df.show(k)


#tags datafiles to dataframes
tags_df = (spark.read.format("csv")
    .option("sep", "::")
    .option("inferschema", "true")
    .option("samplingRatio", 0.1)  # Adjust the sampling ratio
    .load(tags_file)
    .toDF("UserID", "MovieID", "Tag", "Tag_Timestamp"))

tags_df.show(k)


#movies datafiles to dataframes
movies_df = (spark.read.format("csv")
      .option("sep", "::")
      .option("inferschema", "true")
      .option("samplingRatio", 0.1)  # Adjust the sampling ratio 
      .load(movies_file)
      .toDF("MovieID","Titles","Genres"))
movies_df.show(k)


# # Movies and Ratings

# ## k-Most popular movies of all time
# ### Finding k most popular movies of all times assuming k=10
# #### Considering rating timestamp, not the movie realease time


#INNER JOIN movies_df & ratings_df
moviesNratings = movies_df.join(ratings_df,movies_df.MovieID == ratings_df.MovieID, 'inner').select(
        movies_df.MovieID,movies_df.Titles,ratings_df.UserID,ratings_df.Rating, ratings_df.Rating_Timestamp)

moviesNratings.sort(col("UserID")).show(3)

most_pop_movies = moviesNratings.groupBy("MovieID","Titles").avg("Rating").orderBy("avg(Rating)", ascending=False)

#writing to file
most_pop_movies.limit(k).write.csv(output_csv_path, header=True, mode="append")
most_pop_movies.show(k)


# ## k-Most popular movies for a particular year
# ### Finding k most popular movies of all times assuming k=10, year = 2000
# #### Considering rating timestamp, not the movie realease time


moviesNratings_withYear = moviesNratings.withColumn("Year",year(from_unixtime("Rating_Timestamp"))).select("MovieID","Titles","UserID","Rating","Year").where(col("Year") == 2000)
moviesNratings_withYear.show(3)


top_10_pop_movies_of_a_year = moviesNratings_withYear.select("MovieID","Titles","Rating","Year").groupBy("MovieID","Titles").avg("Rating").orderBy("avg(Rating)",ascending=False)
#writing to file
top_10_pop_movies_of_a_year.limit(k).write.csv(output_csv_path, header=True, mode="append")
top_10_pop_movies_of_a_year.show(k)


# ## k-Most popular movies for a particular season
# ### Defining the season as (1: Winter, 2: Spring, 3: Summer, 4: Fall) 
# ### Assuming k=10, target_season = 3 (summer: month 7,8,9)
# #### Considering rating timestamp, not the movie realease time

target_season = 3
moviesNratings_withMonth = moviesNratings.withColumn("Month", month(from_unixtime("Rating_Timestamp"))).select("MovieID","Titles","UserID","Rating","Month").where((col("Month") >=(target_season * 3 - 2))&(col("Month")<=(target_season * 3)))

moviesNratings_withMonth.show(3)


top_10_pop_movies_of_summer = moviesNratings_withMonth.select("MovieID","Titles","Rating","Month").groupBy("MovieID","Titles").avg("Rating").orderBy("avg(Rating)",ascending=False)
#writing to file
top_10_pop_movies_of_summer.limit(k).write.csv(output_csv_path, header=True, mode="append")
top_10_pop_movies_of_summer.show(k)


# ## Top k movies with the most ratings (presumably most popular) that have the lowest ratings
# #### Most rating counts, but less popular/lowest rating avg

# count of the ratings
moviesNratings_with_rating_counts = moviesNratings.groupBy("MovieID","Titles").count()                                                                 
moviesNratings_with_rating_counts.show(10)

# average of the ratings
moviesNratings_with_avg_rating = moviesNratings.groupBy("MovieID","Titles").avg("Rating")
moviesNratings_with_avg_rating = moviesNratings_with_avg_rating.select("MovieID","Titles",round("avg(Rating)",2).alias("avg_rating"))
moviesNratings_with_avg_rating.show(k)


# Joining movie rating-count and average-rating
moviesNratings_rating_counts_with_avgRatings = moviesNratings_with_rating_counts.join(moviesNratings_with_avg_rating, 
                    moviesNratings_with_rating_counts.MovieID == moviesNratings_with_avg_rating.MovieID)\
                     .orderBy("count", ascending=False)\
                     .select(moviesNratings_with_rating_counts.MovieID,
                             moviesNratings_with_rating_counts.Titles,"count","avg_rating")
moviesNratings_rating_counts_with_avgRatings.show(k)


#Calculating the result
movies_hcount_lrating = moviesNratings_rating_counts_with_avgRatings.orderBy(['count', 'avg_rating'], ascending=[False, True])

#writing to file
movies_hcount_lrating.limit(k).write.csv(output_csv_path, header=True, mode="append")
movies_hcount_lrating.show(k)


# # Movies and Tags

#INNER JOIN movies_df & tags_df
moviesNtags = movies_df.join(tags_df,movies_df.MovieID == tags_df.MovieID, 'inner').select(
        movies_df.MovieID,movies_df.Titles,movies_df.Genres,tags_df.UserID,tags_df.Tag, tags_df.Tag_Timestamp)\
           .orderBy("UserID", ascending=True)

moviesNtags.show(13)


# ## k-Most tagged movies of all time
# ### Finding k most tagged movies of all times assuming k=10
# #### Considering tagging timestamp, not the movie realease time

most_tagged_movies = moviesNtags.groupBy("MovieID","Titles").count().orderBy("count", ascending=False)

#writing to file
most_tagged_movies.limit(k).write.csv(output_csv_path, header=True, mode="append")
most_tagged_movies.show(k)


# ## k-Most commonly used tags for movies of all time
# ### Finding k most commomly used tags for movies of all times, assuming k=10

most_used_tags = moviesNtags.groupBy("Tag").count().orderBy("count", ascending=False)

#writing to file
most_used_tags.limit(k).write.csv(output_csv_path, header=True, mode="append")
most_used_tags.show(k)


# ## k-Most commonly used tags for the most common genre of the dataset
# ### Finding the most common genre of the dataset,then finding the k-most common tags for that genre, assuming k=10

#Exploding Genre
movie_tagsNgenre = moviesNtags.withColumn("Genre", explode(split(trim(col("Genres")), "\\|"))).drop('Genres')
movie_tagsNgenre.show(5)

#Finding the most common genre which is our targer genre
genre_counts = movie_tagsNgenre.groupBy("Genre").count().orderBy("count", ascending=False)
target_genre = genre_counts.collect()[0][0]
genre_counts.show(25)

# Calculating the count of each tags for each genre
r_movie_tagsNgenre = movie_tagsNgenre.groupBy("Genre","Tag").count().orderBy(['count','Genre'], ascending=[False, False])
r_movie_tagsNgenre.show(5)

tags_for_target_genre = movie_tagsNgenre.where(col("Genre")== target_genre)

#writing to file
tags_for_target_genre.limit(k).write.csv(output_csv_path, header=True, mode="append")
tags_for_target_genre.show(k)


# ## Finding the month of the year where movies get most tags based on tagging timestamp

moviesNtags_withMonth = moviesNtags.withColumn("Month", month(from_unixtime("Tag_Timestamp"))).select("MovieID","Titles","Tag","Month")
moviesNtags_withMonth.show(15)

moviesNtags_withMonth = moviesNtags_withMonth.groupBy("Month").count().orderBy('count',ascending = False)

#writing to file
moviesNtags_withMonth.limit(12).write.csv(output_csv_path, header=True, mode="append")
moviesNtags_withMonth.show(12)


# ## For a particular genre which month is getting most tags
# ### selecting genre = Thriller

moviesNtags_Mn_Gn = moviesNtags.withColumn("Month", month(from_unixtime("Tag_Timestamp"))).select("MovieID","Titles","Genres","Tag","Month").where(col('Genres') == 'Thriller')
moviesNtags_Mn_Gn.show(k)

moviesNtags_Mn_Gn = moviesNtags_Mn_Gn.groupBy("Month").count().orderBy('count',ascending = False)

#writing to file
moviesNtags_Mn_Gn.limit(12).write.csv(output_csv_path, header=True, mode="append")
moviesNtags_Mn_Gn.show(12)


#stopping spark
spark.stop()

