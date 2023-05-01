print("lets start!!")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import split, explode,when,countDistinct
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import udf
from pyspark.sql.functions import posexplode
import pyspark.sql.functions as F

def my_function(x):
    return dict_category[x]


if __name__ == "__main__":

    spark = SparkSession.builder.appName("Yelp Data Preprocess").getOrCreate()

    # load primary datasets
    df_review = spark.read.load("s3://yelp/yelp_academic_dataset_review.json", format='json')
    df_user = spark.read.load("s3://yelp/yelp_academic_dataset_user.json", format='json')
    df_business = spark.read.load("s3://yelp/yelp_academic_dataset_business.json", format='json')

    # check if duplicate ids
    print(df_business.select(countDistinct("business_id")).collect()[0][0] == df_business.count())
    distinct_cat = df_business.select(explode(split(col("categories"), ",")).alias("values")).distinct()
    distinct_cat_list = [row.values for row in distinct_cat.collect()]

    # find unique categories
    dict_category = {}
    i=1
    for cat in distinct_cat_list:
        dict_category[cat] = i
        i+=1
    # split list of cats.
    df_biz_exploded = df_business.withColumn("categories_exploded", explode(split(df_business.categories, ",")))

    # find id from dict. for cat.
    my_udf = udf(my_function)
    df_biz_exploded_mapped = df_biz_exploded.withColumn("categories_exploded_mapped",my_udf(df_biz_exploded.categories_exploded))
    df_biz_exploded_mapped_bid_cat = df_biz_exploded_mapped.select(['business_id','categories_exploded_mapped'])
    df_biz_exploded_mapped_bid_cat.count()

    # remove duplicate categories
    df_biz_exploded_mapped_bid_cat_unique = df_biz_exploded_mapped_bid_cat.groupBy('business_id','categories_exploded_mapped').count()

    df_biz_exploded_mapped_bid_cat_unique = df_biz_exploded_mapped_bid_cat_unique.drop('count')


    df_biz_exploded_mapped_bid_cat_unique.write.csv('s3://yelp/yelp_business_categories.csv')

    rows = list(zip(list(dict_category.keys()),list(dict_category.values())))
    df_categories = spark.createDataFrame(rows, ['id','categories'])

    df_categories.write.csv('s3://yelp/yelp_categories.csv')

    # filter review_ids to adhere to foreign keys being present
    df_review_filter = df_review.join(df_business.select('business_id'),['business_id'],"leftsemi")

    df_review_filter = df_review_filter.join(df_user.select('user_id'),['user_id'],"leftsemi")

    # to load data correctly, text have large spacing making loading failures to db
    df_review_arrange = df_review_filter.withColumn("text", regexp_replace("text", "\\|", ""))
    df_review_arrange = df_review_arrange.withColumn("text", regexp_replace("text", "\s+", " "))

    df_review_arrange = df_review_arrange.select('review_id','business_id','user_id','date','stars','useful','cool','funny','text')

    df_review_arrange.write.format('csv').option('sep','|').save('s3://yelp/yelp_review.csv',header='True')

    # split biz hrs to different cols from nested object
    df_biz_hrs = df_business.select("business_id","hours.Monday", "hours.Tuesday", "hours.Wednesday", "hours.Thursday", "hours.Friday", "hours.Saturday", "hours.Sunday")

    df_biz_hrs.write.csv('s3://yelp/yelp_business_hours.csv')

    df_business_final = df_business.drop('attributes','categories','hours')

    df_business_final = df_business_final.withColumn("is_open", when(df_business_final.is_open== 1, 'TRUE').otherwise('FALSE'))

    df_business_final.write.csv('s3://yelp/yelp_business.csv',header='True')

    biz_id_list = df_business.select(collect_list('business_id')).first()[0]

    # filter TIPS  to adhere to foreign keys being present

    df_tip = spark.read.load("s3://yelp/yelp_academic_dataset_tip.json", format='json')


    df_tip_user_filter = df_tip.select(df_tip.columns).join(df_user,'user_id','leftsemi')


    df_tip_user_biz_filter = df_tip_user_filter.select(df_tip_user_filter.columns).join(df_business,'business_id','leftsemi')


    df_tip_user_biz_filter.write.csv("s3://yelp/yelp_tips.csv",header='True')



    df_checkin =  spark.read.load("s3://yelp/yelp_academic_dataset_checkin.json", format='json')

    df_business.count()

    # filter CHECKIN  to adhere to foreign keys being present

    df_checkin_filter = df_checkin.select(df_checkin.columns).join(df_business,'business_id','leftsemi')


    df_checkin_exp = df_checkin.withColumn("date",explode(split(df_checkin.date,',')))

    df_checkin_exp = df_checkin_exp.withColumn("id", F.monotonically_increasing_id() + 1)

    df_checkin_exp.write.csv("s3://yelp/yelp_checkins.csv",header='True')

    # split friend ids present in list and make seperate table
    df_user_exploded = df_user.withColumn("friends_exploded", explode(split(df_user.friends, ",")))


    df_friend = df_user_exploded.select("user_id","friends_exploded")

    df_friend.write.csv('s3://yelp/yelp_user_friend.csv')

    # split elite years from list and make seperate table
    df_user_exploded_elite = df_user.withColumn("elite_exploded", explode(split(df_user.elite, ",")))

    df_elite_years = df_user_exploded_elite.select("user_id","elite_exploded")
    df_elite_years.write.csv('s3://yelp/yelp_elite_years.csv')
    # arrange columns
    df_user_final = df_user.select('user_id','name','yelping_since','review_count','compliment_list','compliment_photos','compliment_profile','fans','funny','useful')
    df_user_final.show()

    df_user_final.write.csv('s3://yelp/yelp_users.csv',header='True')




