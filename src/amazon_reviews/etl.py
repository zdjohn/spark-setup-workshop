"""[summary]

    Returns:
        [type]: [description]
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def to_stats_aggregation(dataframe: DataFrame) -> DataFrame:
    """Transform original dataset.

    :param df: Input DataFrame.

    :return: aggregated dataframes based on users purchase, category, market, etc..
    """

    df_stats_agg = dataframe.where(
        dataframe.star_rating >= 4).groupby('customer_id').agg(
            F.count('product_id').alias('products_count'),
            F.countDistinct('product_id').alias('products_distinct'),
            F.countDistinct('product_parent').alias('pp_counts'),
            F.count('review_id').alias('reviews_count'),
            F.count(F.when(F.col('verified_purchase') == 'Y', True)
                    ).alias('verified_purchase_count')
    )

    df_stats_agg = df_stats_agg.withColumn(
        'purchase_rate',
        (df_stats_agg.verified_purchase_count/df_stats_agg.reviews_count).cast("float"))

    return df_stats_agg


def to_dense_product_user_ids(dataframe: DataFrame, dataframe_stats_agg: DataFrame, floor: int, ceiling: int) -> DataFrame:
    """filter costomer based on user-item interaction

    Args:
        dataframe (DataFrame): source dataframe with user aggregated metrics
        floor (int): minimal review counts
        ceiling (int): maximun review counts

    Returns:
        DataFrame: users with right density
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
    """

    # consider verified purchase rate
    filter_sparse = dataframe_stats_agg.where(
        (dataframe_stats_agg.products_distinct >= floor)
        & (dataframe_stats_agg.products_distinct <= ceiling))

    filtered_product_id = filter_sparse.join(
        dataframe, ['customer_id']).select(F.col('product_id')).distinct()

    filtered_users_reviews = dataframe.where(dataframe.star_rating >= 4).join(
        filtered_product_id, on='product_id'
    ).join(
        filter_sparse, on='customer_id', how='left_outer'
    )

    return filtered_users_reviews


def to_dense_reviews(dataframe: DataFrame, dense_product_user: DataFrame) -> DataFrame:
    """filter review df by dense_product_user

    Args:
        dataframe (DataFrame): original review df
        dense_product_user (DataFrame): dataframe with previous filtered dense_product_user only

    Returns:
        DataFrame: dense positive user review
    """

    return dataframe.where(dataframe.star_rating >= 4).join(
        F.broadcast(dense_product_user.select('product_id', 'customer_id')),
        ['product_id', 'customer_id']
    )
