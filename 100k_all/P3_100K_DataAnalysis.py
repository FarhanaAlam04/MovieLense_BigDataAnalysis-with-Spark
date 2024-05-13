#!/usr/bin/env python3
# coding: utf-8

# # Project3_100K_MovieLens Data Analysis
### Author: Farhana Alam

# arg1:u.item, arg2:u.data, arg3:u.user arg4:output_100k 

# Running on cscluster-hdfs with command : spark-submit --master spark://cscluster00.boisestate.edu:7077 P3_100k_DataAnalysis.py hdfs://cscluster00:9000/user/farhanaalam/100k_all/u.item hdfs://cscluster00:9000/user/farhanaalam/100k_all/u.data hdfs://cscluster00:9000/user/farhanaalam/100k_all/u.user hdfs://cscluster00:9000/user/farhanaalam/output_100k_hdfs


from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as func
import sys
k = 10


spark = (SparkSession
       .builder
       .appName("Spark Project3 100K Movie Data Analysis")
       .getOrCreate())


# Loading the 10M MovieLens DataFiles
#movies_file = "Documents/CS535/movie_data/ml-100k/u.item" # sys.argv[1]
#ratings_file = "Documents/CS535/movie_data/ml-100k/u.data" # sys.argv[2]
#users_file = "Documents/CS535/movie_data/ml-100k/u.user" # sys.argv[3]

# Output CSV file path
#output_csv_path = "Documents/CS535/movie_data/ml-100k-output" # sys.argv[4]

## Trying with argv
movies_file = sys.argv[1]
ratings_file = sys.argv[2]
users_file = sys.argv[3]

# Output CSV file path
output_csv_path = sys.argv[4]


#movies datafiles to dataframes
movies_df = (spark.read.format("csv")
      .option("sep", "|")
      .option("inferschema", "true")
      .option("samplingRatio", 0.1)  # Adjust the sampling ratio 
      .load(movies_file)
      .toDF("MovieID","Titles","release_date","video_release_date","IMDb_URL","unknown","Action","Adventure","Animation","Children's","Comedy","Crime","Documentary","Drama",
           "Fantasy","Film-Noir","Horror","Musical","Mystery","Romance","Sci-Fi","Thriller","War","Western"))
movies_df=movies_df.select("MovieID","Titles","release_date","Action","Drama","Fantasy")
movies_df.show(8)


# define schema for our data (u.user)
schema_user = StructType([
   StructField("UserID", IntegerType(), False),
   StructField("age", IntegerType(), False),
   StructField("gender", StringType(), False),
   StructField("occupation", StringType(), False),
   StructField("zip_code", IntegerType(), False)])

users_df = spark.read.option("sep", "|").option("multiLine", "true").option("ignoreTrailingWhiteSpace", "true").csv(users_file, schema=schema_user)
users_df.show(5)



#ratings datafiles to dataframes
ratings_df = (spark.read.format("csv")
    .option("sep", "\t")
    .option("inferschema", "true")
    .option("samplingRatio", 0.1)  # Adjust the sampling ratio
    .load(ratings_file)
    .toDF("UserID", "MovieID", "Rating", "Rating_Timestamp"))

ratings_df.show(k)


# # Movies and Ratings

#INNER JOIN movies_df & ratings_df
moviesNratings = movies_df.join(ratings_df,movies_df.MovieID == ratings_df.MovieID, 'inner').select(
        movies_df.MovieID,movies_df.Titles,ratings_df.UserID,ratings_df.Rating, ratings_df.Rating_Timestamp,movies_df.Action,movies_df.Drama,movies_df.Fantasy)

moviesNratings.sort(col("UserID")).show(3)


# ## k-Most popular movies of all time
# ### Finding k most popular movies of all times assuming k=10

most_pop_movies = moviesNratings.groupBy("MovieID","Titles").avg("Rating").orderBy("avg(Rating)", ascending=False)

#writing to file
most_pop_movies.limit(k).write.csv(output_csv_path, header=True, mode="append")
most_pop_movies.show(k)



# ## k-Most popular movies for a particular year
# ### Finding k most popular movies of all times assuming k=10, year = 1998
# #### Considering rating timestamp, not the movie realease time



#INNER JOIN movies_df & ratings_df
#moviesNratings = movies_df.join(ratings_df,movies_df.MovieID == ratings_df.MovieID, 'inner').select(
#        movies_df.MovieID,movies_df.Titles,ratings_df.UserID,ratings_df.Rating, ratings_df.Rating_Timestamp)
#
#moviesNratings.sort(col("UserID")).show(3)




moviesNratings_withYear = moviesNratings.withColumn("Year", year(from_unixtime("Rating_Timestamp"))).select("MovieID","Titles","UserID","Rating","Year").where(col("Year") == 1998)
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
movies_hcount_lrating = moviesNratings_rating_counts_with_avgRatings.orderBy(['count','avg_rating'], ascending=[False, True])

#writing to file
movies_hcount_lrating.limit(k).write.csv(output_csv_path, header=True, mode="append")
movies_hcount_lrating.show(k)


# # Movies, Ratings, Users

#INNER JOIN movies_df & ratings_df & users_df

moviesNratingsNusers = moviesNratings.join(users_df,moviesNratings.UserID == users_df.UserID,'inner').select(
        moviesNratings.MovieID,moviesNratings.Titles,moviesNratings.Rating, 
        moviesNratings.Rating_Timestamp,moviesNratings.UserID,users_df.age,users_df.gender,users_df.occupation)
moviesNratingsNusers.sort(col("UserID")).show(3)



# ## k most popular movies for a particular age
# ### Finding most rated 10 movies by a particular age 25 yrs

target_age = 25
top_10_pop_movies_by_age = moviesNratingsNusers.select("MovieID","Titles","Rating","UserID","age").where(col("age") == target_age)
top_10_pop_movies_by_age.show(3)


top_10_pop_movies_by_age = top_10_pop_movies_by_age.select("MovieID","Titles","Rating","age").groupBy("MovieID","Titles").avg("Rating").orderBy("avg(Rating)",ascending=False)

#writing to file
movies_hcount_lrating.limit(k).write.csv(output_csv_path, header=True, mode="append")
top_10_pop_movies_by_age.show(k)



# ## For a particular genre finding k-most rated movies
# ### selecting genre = Drama, k = 10

target_genre = 'Drama'
top_10_pop_movies_by_genre = moviesNratings.select("MovieID","Titles","Rating","Drama").where(col(target_genre) == 1)
top_10_pop_movies_by_genre.show(3)

top_10_pop_movies_by_genre = top_10_pop_movies_by_genre.select("MovieID","Titles","Rating").groupBy("MovieID","Titles").avg("Rating").orderBy("avg(Rating)",ascending=False)

#writing to file
top_10_pop_movies_by_genre.limit(k).write.csv(output_csv_path, header=True, mode="append")
top_10_pop_movies_by_genre.show(k)



# ## Finding the average rating by age group

#moviesNratingsNusers
avg_rating_by_age_group = moviesNratingsNusers.select("MovieID","Titles","Rating","UserID","age").groupBy("age").avg("Rating").orderBy("avg(Rating)",ascending=False)
avg_rating_by_age_group = avg_rating_by_age_group.select("age",round("avg(Rating)",2).alias("avg_rating"))

#writing to file
avg_rating_by_age_group.limit(k).write.csv(output_csv_path, header=True, mode="append")
avg_rating_by_age_group.show(k)



#stopping spark
spark.stop()

