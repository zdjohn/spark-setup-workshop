"""[summary]
    """

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from typing import Tuple


def to_refined_reviews(df_reviews: DataFrame) -> DataFrame:
    """
    `products_distinct` is more than 3
    and, `purchase_rate` is more than 0.5

    Args:
        df_reviews (DataFrame): [description]
        root
        |-- customer_id: string (nullable = true)
        |-- product_id: string (nullable = true)
        |-- marketplace: string (nullable = true)
        |-- review_id: string (nullable = true)
        |-- product_parent: string (nullable = true)
        |-- product_title: string (nullable = true)
        |-- star_rating: integer (nullable = true)
        |-- helpful_votes: integer (nullable = true)
        |-- total_votes: integer (nullable = true)
        |-- vine: string (nullable = true)
        |-- verified_purchase: string (nullable = true)
        |-- review_headline: string (nullable = true)
        |-- review_body: string (nullable = true)
        |-- review_date: date (nullable = true)
        |-- year: integer (nullable = true)
        |-- products_count: long (nullable = true)
        |-- products_distinct: long (nullable = true)
        |-- pp_counts: long (nullable = true)
        |-- reviews_count: long (nullable = true)
        |-- verified_purchase_count: long (nullable = true)
        |-- purchase_rate: float (nullable = true)

        e.g.:
+-----------+----------+-----------+--------------+--------------+--------------------+-----------+-------------+-----------+----+-----------------+--------------------+--------------------+-----------+----+--------------+-----------------+---------+-------------+-----------------------+-------------+
|customer_id|product_id|marketplace|     review_id|product_parent|       product_title|star_rating|helpful_votes|total_votes|vine|verified_purchase|     review_headline|         review_body|review_date|year|products_count|products_distinct|pp_counts|reviews_count|verified_purchase_count|purchase_rate|
+-----------+----------+-----------+--------------+--------------+--------------------+-----------+-------------+-----------+----+-----------------+--------------------+--------------------+-----------+----+--------------+-----------------+---------+-------------+-----------------------+-------------+
|   10000133|B0018CCOA8|         US|R16C1VOJDDK9BT|       4675149|               Ghost|          5|            0|          0|   N|                N|          Wonderful.|When I first boug...| 2012-09-20|2012|             1|                1|        1|            1|                      0|          0.0|
|   10000179|B00A7ZX7BW|         US|R1Y3TPQPTXODJ6|     839345023|Pour It Up (Expli...|          1|            0|          0|   N|                Y|       not satisfied|Couldn't even dow...| 2013-01-08|2013|          null|             null|     null|         null|                   null|         null|
|   10000362|B0083EYC4A|         US|R2L49J8OBHCS5M|     485000348|          Next to Me|          5|            0|          0|   N|                Y|       Great song!!!|Like the title sa...| 2013-09-02|2013|             2|                2|        2|            2|                      2|          1.0|
|   10000393|B00DGHX1U0|         US|R31Z9DVGG6YX3U|     663209256|Don't Think They ...|          4|            0|          0|   N|                Y|         chris brown|its one of the fe...| 2013-09-19|2013|             6|                6|        6|            6|                      6|          1.0|
|   10000393|B00DJ9MIG8|         US|R1VVM3A7PJXN5N|     307133810|Get Like Me [feat...|          3|            1|          1|   N|                N|       okay not good|nelly time may ha...| 2013-09-19|2013|             6|                6|        6|            6|                      6|          1.0|
|   10000393|B000QO9H0O|         US|R31HGI7JW33CUC|     752845439|                Home|          5|            0|          0|   N|                Y|              Genius|One of the best i...| 2013-06-06|2013|             6|                6|        6|            6|                      6|          1.0|
|   10000393|B008419GBQ|         US|R31RUDWKHDIYGX|     795557121|What You Won't Do...|          5|            0|          0|   N|                Y|Great Old School ...|I remember this s...| 2013-12-30|2013|             6|                6|        6|            6|                      6|          1.0|
|   10000393|B009E2V6LW|         US|R22DBME5285IU4|     450328699|How Many Drinks? ...|          5|            0|          0|   N|                Y|              miguel|love this song es...| 2013-09-19|2013|             6|                6|        6|            6|                      6|          1.0|
|   10000393|B00AR7L6ZO|         US|R3GQH4H61SC0QX|     395335753|         Lose to Win|          5|            1|          1|   N|                Y|Best song in a lo...|Fantasia has a hi...| 2013-01-28|2013|             6|                6|        6|            6|                      6|          1.0|
|   10000393|B00EFWAOBY|         US|R20URYKT9MYGHO|     568296409|In The Middle Of ...|          5|            0|          0|   N|                Y|          Great Song|Great song, I lov...| 2013-08-26|2013|             6|                6|        6|            6|                      6|          1.0|
+-----------+----------+-----------+--------------+--------------+--------------------+-----------+-------------+-----------+----+-----------------+--------------------+--------------------+-----------+----+--------------+-----------------+---------+-------------+-----------------------+-------------+


    Returns:
        DataFrame:
+-----------+----------+-----------+--------------+--------------+--------------------+-----------+-------------+-----------+----+-----------------+--------------------+--------------------+-----------+----+--------------+-----------------+---------+-------------+-----------------------+-------------+
|customer_id|product_id|marketplace|     review_id|product_parent|       product_title|star_rating|helpful_votes|total_votes|vine|verified_purchase|     review_headline|         review_body|review_date|year|products_count|products_distinct|pp_counts|reviews_count|verified_purchase_count|purchase_rate|
+-----------+----------+-----------+--------------+--------------+--------------------+-----------+-------------+-----------+----+-----------------+--------------------+--------------------+-----------+----+--------------+-----------------+---------+-------------+-----------------------+-------------+
|   10000393|B00DGHX1U0|         US|R31Z9DVGG6YX3U|     663209256|Don't Think They ...|          4|            0|          0|   N|                Y|         chris brown|its one of the fe...| 2013-09-19|2013|             6|                6|        6|            6|                      6|          1.0|
|   10000393|B000QO9H0O|         US|R31HGI7JW33CUC|     752845439|                Home|          5|            0|          0|   N|                Y|              Genius|One of the best i...| 2013-06-06|2013|             6|                6|        6|            6|                      6|          1.0|
|   10000393|B008419GBQ|         US|R31RUDWKHDIYGX|     795557121|What You Won't Do...|          5|            0|          0|   N|                Y|Great Old School ...|I remember this s...| 2013-12-30|2013|             6|                6|        6|            6|                      6|          1.0|
|   10000393|B009E2V6LW|         US|R22DBME5285IU4|     450328699|How Many Drinks? ...|          5|            0|          0|   N|                Y|              miguel|love this song es...| 2013-09-19|2013|             6|                6|        6|            6|                      6|          1.0|
|   10000393|B00AR7L6ZO|         US|R3GQH4H61SC0QX|     395335753|         Lose to Win|          5|            1|          1|   N|                Y|Best song in a lo...|Fantasia has a hi...| 2013-01-28|2013|             6|                6|        6|            6|                      6|          1.0|
|   10000393|B00EFWAOBY|         US|R20URYKT9MYGHO|     568296409|In The Middle Of ...|          5|            0|          0|   N|                Y|          Great Song|Great song, I lov...| 2013-08-26|2013|             6|                6|        6|            6|                      6|          1.0|
|   10002805|B001NZICEW|         US|R3BW1TRVYHFZ82|     720080443|Between Me & You ...|          5|            0|          0|   N|                N|Every Little Thin...|This is such a gr...| 2013-09-14|2013|            10|               10|       10|           10|                      7|          0.7|
|   10002805|B005636A6E|         US|R1N65KUL2TKREG|     710435019|Sorry For Party R...|          5|            0|          0|   N|                Y|No Hard Feelings,...|This song gets me...| 2013-05-16|2013|            10|               10|       10|           10|                      7|          0.7|
|   10002805|B000VZV9KY|         US|R2EEDNMAR6VZ3L|     112255675|Escape (The Pina ...|          5|            0|          0|   N|                Y|               Dream|This song is like...| 2013-05-16|2013|            10|               10|       10|           10|                      7|          0.7|
|   10002805|B0012FCFY6|         US|R3HX16L2SCJE8V|     973779319|        Boogie Shoes|          5|            0|          0|   N|                N|Just to Boogie wi...|This song just pu...| 2013-08-23|2013|            10|               10|       10|           10|                      7|          0.7|
+-----------+----------+-----------+--------------+--------------+--------------------+-----------+-------------+-----------+----+-----------------+--------------------+--------------------+-----------+----+--------------+-----------------+---------+-------------+-----------------------+-------------+
    """

    return df_reviews.where((df_reviews.products_distinct.isNotNull()) & (df_reviews.products_distinct >= 3) & (df_reviews.purchase_rate > 0.5) & (df_reviews.star_rating > 3))


def to_overlapping_customers(source: DataFrame, target: DataFrame) -> DataFrame:
    """
    get overlapping users between 2 domains

    Args:
        source (DataFrame): densed review
        target (DataFrame): densed review

    +-----------+----------+-----------+--------------+--------------+--------------------+-----------+-------------+-----------+----+-----------------+--------------------+--------------------+-----------+----+--------------+-----------------+---------+-------------+-----------------------+-------------+
    |customer_id|product_id|marketplace|     review_id|product_parent|       product_title|star_rating|helpful_votes|total_votes|vine|verified_purchase|     review_headline|         review_body|review_date|year|products_count|products_distinct|pp_counts|reviews_count|verified_purchase_count|purchase_rate|
    +-----------+----------+-----------+--------------+--------------+--------------------+-----------+-------------+-----------+----+-----------------+--------------------+--------------------+-----------+----+--------------+-----------------+---------+-------------+-----------------------+-------------+
    |   10000133|B0018CCOA8|         US|R16C1VOJDDK9BT|       4675149|               Ghost|          5|            0|          0|   N|                N|          Wonderful.|When I first boug...| 2012-09-20|2012|             1|                1|        1|            1|                      0|          0.0|
    |   10000179|B00A7ZX7BW|         US|R1Y3TPQPTXODJ6|     839345023|Pour It Up (Expli...|          1|            0|          0|   N|                Y|       not satisfied|Couldn't even dow...| 2013-01-08|2013|          null|             null|     null|         null|                   null|         null|
    |   10000362|B0083EYC4A|         US|R2L49J8OBHCS5M|     485000348|          Next to Me|          5|            0|          0|   N|                Y|       Great song!!!|Like the title sa...| 2013-09-02|2013|             2|                2|        2|            2|                      2|          1.0|
    |   10000393|B00DGHX1U0|         US|R31Z9DVGG6YX3U|     663209256|Don't Think They ...|          4|            0|          0|   N|                Y|         chris brown|its one of the fe...| 2013-09-19|2013|             6|                6|        6|            6|                      6|          1.0|
    |   10000393|B00DJ9MIG8|         US|R1VVM3A7PJXN5N|     307133810|Get Like Me [feat...|          3|            1|          1|   N|                N|       okay not good|nelly time may ha...| 2013-09-19|2013|             6|                6|        6|            6|                      6|          1.0|
    |   10000393|B000QO9H0O|         US|R31HGI7JW33CUC|     752845439|                Home|          5|            0|          0|   N|                Y|              Genius|One of the best i...| 2013-06-06|2013|             6|                6|        6|            6|                      6|          1.0|
    |   10000393|B008419GBQ|         US|R31RUDWKHDIYGX|     795557121|What You Won't Do...|          5|            0|          0|   N|                Y|Great Old School ...|I remember this s...| 2013-12-30|2013|             6|                6|        6|            6|                      6|          1.0|
    |   10000393|B009E2V6LW|         US|R22DBME5285IU4|     450328699|How Many Drinks? ...|          5|            0|          0|   N|                Y|              miguel|love this song es...| 2013-09-19|2013|             6|                6|        6|            6|                      6|          1.0|
    |   10000393|B00AR7L6ZO|         US|R3GQH4H61SC0QX|     395335753|         Lose to Win|          5|            1|          1|   N|                Y|Best song in a lo...|Fantasia has a hi...| 2013-01-28|2013|             6|                6|        6|            6|                      6|          1.0|
    |   10000393|B00EFWAOBY|         US|R20URYKT9MYGHO|     568296409|In The Middle Of ...|          5|            0|          0|   N|                Y|          Great Song|Great song, I lov...| 2013-08-26|2013|             6|                6|        6|            6|                      6|          1.0|
    +-----------+----------+-----------+--------------+--------------+--------------------+-----------+-------------+-----------+----+-----------------+--------------------+--------------------+-----------+----+--------------+-----------------+---------+-------------+-----------------------+-------------+


    Returns:
        DataFrame: [description]
    """

    customer_ids = source.join(
        target,
        ['customer_id']
    ).select(F.col('customer_id')).distinct()
    return customer_ids


def to_indexed_ids(df: DataFrame, col_name: str) -> DataFrame:
    """[summary]
    Args:
        df (DataFrame): [description]
        col_name (str): [description]
    Returns:
        DataFrame: [description]
    # +-----------------+-----------+
    # |customer_id_index|customer_id|
    # +-----------------+-----------+
    # |                1|   10001105|
    # |                2|   10007421|
    # |                3|   10008274|
    # |                4|   10010722|
    # |                5|   10012171|
    # +-----------------+-----------+
    """
    # ref: https://towardsdatascience.com/adding-sequential-ids-to-a-spark-dataframe-fa0df5566ff6
    index_window = Window.orderBy(F.col(col_name))
    index_df = df.withColumn(
        f'{col_name}_index', F.row_number().over(index_window)-1)
    return index_df.select(F.col(f'{col_name}_index'), F.col(col_name))


def to_overlapping_reviews(review_df: DataFrame, customer_ids: DataFrame) -> DataFrame:
    """
    Args:
        review_df ([type]): [description]
        user_ids (DataFrame): [description]

    Returns:
        DataFrame: [description]
    """
    return review_df.join(customer_ids, ["customer_id"]).select('customer_id', 'product_id')

    # video

    # software


def train_test_split(review_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """[summary]

    Args:
        review_df (DataFrame): [description]
        user_ids (DataFrame): [description]
        split (float): [description]

    Returns:
        DataFrame: [description]
    """
    test_user_df = review_df.drop_duplicates('user_id')
    train_user_df = review_df.join(test_user_df, 'left_anti')
    return test_user_df, train_user_df
