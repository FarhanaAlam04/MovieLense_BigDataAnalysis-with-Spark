# MovieLense_BigDataAnalysis - with Spark

* Author: Farhana Alam
* Semester: Fall 2023

## Overview

The project focusses on movie data mining with a primary objective of deriving interesting
trends from a collection of movie ratings by users. The analysis aims to uncover at least four
noteworthy patterns within the data. Few of the examples are exploring the k most popular movies
for a particular age group, identifying the top k movies with the most ratings, which could be
considered the most popular, but interestingly have the lowest ratings and so on.

The datasets utilized for this project are sourced from GroupLens Research, which has gathered
and provided rating datasets from the MovieLens website. Initially the work was 
started on a smaller dataset of 100,000 ratings (1-5) from 943 users on 1682 movies.
and also utilized 10M dataset. This data set contains 10000054 ratings and 95580 tags applied
to 10681 movies by 71567 users.  A few trends were also explored on the 25M dataset to check
scalability on large datasets.

For this work, Spark was run locally on our VM as well as on the cluster. Spark sessions are
appropriately configured and python scripts are run for implementing our program using PySpark
SQL and Dataframe operations.

## Reflection

This project of  movie data mining using Spark was a valuable learning experience. The
challenges encountered helped us to get a better understanding of the intricacies involved
in large-scale data analysis. What worked well was the easy data availability and a clear
problem statement. Accessing the MovieLens dataset from GroupLens Research was straightforward,
providing a rich source for movie ratings. The project's problem statement was well-defined,
outlining the objective of deriving interesting trends from the movie rating dataset. This
clarity facilitated a focused approach.

Spark SQL was particularly useful for this project due to its capabilities in processing
and analyzing large-scale structured data using Spark. It allowed us to express complex data
manipulations and analyses using SQL-like queries. Because of our familiarity with SQL syntax it
became easier to formulate queries to extract specific information from the dataset. But we were
also interested learning Dataframe operations on Spark. So, we utilized dataframe operations too.

There was an initial learning curve on understanding and adapting to the structure of the
dataset. However, it also contributed to a deeper understanding of the data. Some challenges
were faced in the process of rebasing in Git during the project's development. Addressing
conflicts and ensuring a smooth rebase had an initial learning curve as well. During the Git
rebase process, resolving conflicts in the version history took some time. This highlighted
the importance of clear communication within the development team.

In conclusion, Spark coding streamlined the data analysis process. The project presented a
range of interesting analysis opportunities, from identifying popular movies across different
dimensions to exploring trends related to ratings and seasons. It provided valuable insights
into effective data analysis and collaborative development.



## Compiling and Using

- Setting up link to movies:
  - cd ~
  - ln -s /home/amit/CS535-data/movies-data movies

- To run:
  -  To run locally :

      - spark-submit --master local[*] PythonDataAnalysisFile.py InputDataFile.csv
        SmallDataInput/ratings.csv SmallDataInput/users.csv SmallDataOutput 2> /dev/null

        where local[*] says to use as many threads as the number of logical threads on our local system.

    
  - To run 10M data on a Spark cluster that is up and running along with a Hadoop cluster, use:

    - hdfs  dfs -put 10M_all
      
    - spark-submit --master spark://cscluster00.boisestate.edu:7077 P3_10M_DataAnalysis.py hdfs://cscluster00:9000/...path/to/10M_all/sample_with_tag/movie.csv hdfs://cscluster00:9000/...path..to/10M_all/sample_with_tag/ratings.csv hdfs://cscluster00:9000/...path..to/10M_all/sample_with_tag/tags.csv hdfs://cscluster00:9000/...path..to/output_10M 

    - hdfs dfs -get output_10M
   
  - To run 100K data on a Spark cluster that is up and running along with a Hadoop cluster, use:

    - hdfs  dfs -put 100k_all
      
    - spark-submit --master spark://cscluster00.boisestate.edu:7077 P3_100K_DataAnalysis.py hdfs://cscluster00:9000/...path/to/100k_all/input_100k/u.item hdfs://cscluster00:9000/...path..to/100k_all/input_100k/u.data hdfs://cscluster00:9000/...path..to/100k_all/input_100k/u.user hdfs://cscluster00:9000/...path..to/output_100K 

    - hdfs dfs -get output_100K
    
Note: Please do not try to run the .ipynb files; all the notebooks has hardcoded file-path of input and output.  

## Results 

**Proof of correctness**

  The began by confirming the correctness of our methodology. At first wrote queries and performed data analysis on a small subset of the 
  1M dataset comprising of 32373 ratings, from 5000 users on 100 movies in Excel using Power Query and Pivot Tables.

  After that the same analyses were done using Spark SQL on the above smaller dataset both on
  our VM and cscluster and the results matched.

 Then we explored more larger datasets and found the following trends.
  
**Interesting Trends**
  
The Movie Data Mining uncovered the following interesting trends. For all three datasets we worked on first four questions:
  
   - What are the k most popular movies of all time?
      
      This global popularity analysis provides insights into the overall popularity of movies
      across all genres and years. It identifies movies that have received the highest number
      of ratings and highest average ratings, indicating a broad appeal among users.

      Platforms can use this information to understand which movies have resonated the most
      with their user base. This information about the most popular movies of all time can be
      used for catalog promotion.

      Highlighting these movies can attract new users and retain existing ones by offering a
      selection of widely appreciated content.

   - What are the k most popular movies for a particular year?
 
      The query allows for an annual analysis of the most popular movies, helping content
      platforms understand the trends and preferences of users during a specific year. We selected
      year 1998 for 100K data, year 2002 for 1M data and year 2000 for 10M and Sample data.

      Yearly user engagement metrics can help the platforms assess success of their content
      strategy and make informed decisions for the future. Since, all the dataset does not have
      releasing timestamp, we have used rating timestamps for some cases.

      Users may find value in discovering or revisiting movies that were celebrated during a
      specific time frame.

      Highlighting the top movies of a given year can enhance user satisfaction and keep the
      catalog diverse and engaging.


   - What are the k movies with most ratings (presumably popular) with lowest Rating?
      
      The combination of high rating counts and lower average ratings may indicate that certain
      movies are widely watched but not universally well-received.

      Understanding user engagement with such movies can be crucial for content platforms to
      refine their recommendation algorithms and content curation strategies.

      It present an opportunity for content platforms to analyze user feedback and identify
      areas for improvement.

      It may also be useful in balancing content recommrndations. While some users may enjoy
      popular blockbusters, others may prefer movies with niche appeal or unconventional
      storytelling.

   - What are the top k movies for a specific season (1: Winter, 2: Spring, 3: Summer, 4: Fall)?

      This query finds the top k movies for a specified season. We determined 3 months of a year
      as a season. We selected Summer season for the query.  Understanding seasonal viewing
      patterns can be used for seasonal content recommendation. Again, since, all the dataset
      does not have releasing timestamp, we have used rating timestamps for some cases.\
     
### Some other trends we found on 100K dataset:

  - For a particular genre what are the k-most rated movies?
     
       Finding this trend can help users to get the genre-based most popular movies.\
       We selected the genre 'Drama' and the most popular movie is **'Aiqing wansui'**.\
     
 -  What are the average rating given by a age group?
     
      Finding this trend is helpful to get the average rating given by an age group to
      understand the movie preferences by age.\

### Some other trends we worked on 10M dataset:

  - What are the k-most tagged movies of all time?
    
      This results identifies movies that have received the highest number of taggings across all the year and genre.\
      Most tagged movies are **'Pulp Fiction (1994)'**(10M) and **'Toy Story (1995)'**(sample).
     
  - What are the k-most commonly used tags for movies of all time?
    
       This result finds out the tagging words which have been used the most.\
       Most used tags are **'Tumey's DVDs'**(10M) and **'Bad'**(sample).
     
  - What are the k-most commonly used tags for the most common genre of the dataset?
    
       This finding helps out to understand the trend of tagging for a particular genre. In our case, we select the 'Drama'genre which has the most number of movies in the dataet.\
       Most used tags for the Drama genre are **'bloody'**(10M) and **'Horror'**(sample).
     
  - Find the month of the year where movies get most tags based on tagging timestamp.
     
       Based on tagging timestamp this finding helps to understand the trend when (month) viewers love to watch and tag
     movies the most. Most tags were given on the month of **February**(10M) and **December**(sample).
      
  - For a particular genre which month is getting the most tags?
     
       This findings can give an insight on what genre is popular on which time (month) by viewers for getting movie
     watched and tagged. We selected the genre 'Thriller'. For this genre most tags were given on the month of **April**(10M) and **December**(sample).


**Timing and Performance Analysis**

In summary, Spark is well-suited for big data processing where parallelization and distributed computing can significantly improve execution times.
