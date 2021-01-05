# pyspark sample project on amazon review datasets

In this project, I am trying to illustrate some common (key) etl tasks using spark (pyspark).

I hope this project can provide a in-depth spark example for developers who want to learn spark.

## About Amazon Customer Reviews Dataset

Amazon Customer Reviews (a.k.a. Product Reviews) is one of Amazon’s iconic products. In a period of over two decades since the first review in 1995, millions of Amazon customers have contributed over a hundred million reviews to express opinions and describe their experiences regarding products on the Amazon.com website. This makes Amazon Customer Reviews a rich source of information for academic researchers in the fields of Natural Language Processing (NLP), Information Retrieval (IR), and Machine Learning (ML), amongst others. Accordingly, we are releasing this data to further research in multiple disciplines related to understanding customer product experiences. Specifically, this dataset was constructed to represent a sample of customer evaluations and opinions, variation in the perception of a product across geographical regions, and promotional intent or bias in reviews.

ref: https://s3.amazonaws.com/amazon-reviews-pds/readme.html

## The Goal (back story)

The data prepared here is intended for training a graph-based recommender system. The aim of developing such model is trying to answer questions as following.

> "What are considered as classic as "Abbey Road (Beatles)" when it comes to movie?"
> "If someone like Jazz, what types of movies would the same person like?"

We will not going to dive into the ML part. (if you are interested, https://github.com/zdjohn/TRM-research-proposal here are more details)

### In this project, we are going to focusing on 3 key data ETL tasks

1. We are going to **explore and analyze** the interaction between customer and products in different categories for the recommendation problem. i.e. Removing sparse data points and bot generated data.

2. We want to **extract the overlapping user's reviews** across two different product domains for transfer learning task. (i.e. Music and Video in this example)

3. We want to **form product and customer graph** based on the user-item interactions.

Last but not least, the size of the amazon review data is enormous, so we are going to perform ETL directly against AWS public S3 dataset. This means you will need an aws account to run the example.

## ETL tasks

### 1. data exploration and analysis

- The number of users
- The number of products
- What are user's review metrics (mean, min, max)
- Extract dense user reviews based on user-item interactions by category

see [notebook aws-review-DEA](./notebook/aws-review-DEA.ipynb), source code for the job [here](./src/amazon_reviews)

### 2. cross domain data ETL

- Who are the common customers across two different product categories
- What products dose common customer interact in each category
- Add user and product index for training

see [notebook cross-domain-reviews](./notebook/cross-domain-reviews.ipynb),
source code for the job [here](./src/cross_domain_reviews)

### 3. Forming customer and product graphs based on user-item interaction

- use customer/product index as graph nodes
- get all customer/product edges based on customer-product interaction

## Testing ETL (TODO)

## Running on Prod (TODO)
