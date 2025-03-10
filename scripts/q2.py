import psycopg2
import os
import time
import pandas as pd
from pymongo import MongoClient


BENCHMARK_COUNT = 5
bench_results = {
    "postgres": [],
    "mongodb": [],
    "neo4j": [],
    "orioledb": []
}

path = os.getcwd()

def run_postgres_q2(with_benchmak:bool = False):
    conn = psycopg2.connect(
        dbname="ecommerce",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    sql_file = os.path.join(path, "scripts", "q2.sql")
    
    with open(sql_file, "r") as f:
        query = f.read()

    if (with_benchmak):
        for i in range(BENCHMARK_COUNT):
            start = time.perf_counter()
            cur.execute(query)
            end = time.perf_counter()
            bench_results["postgres"].append(end - start)
            print(f'Runtime {i+1} postgres {end - start}')
    else:
        cur.execute(query)
    cur.execute(query)
    results = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    df = pd.DataFrame(results, columns=colnames)
    
    cur.close()
    conn.close()
    
    return df


def run_oriole_q2(with_benchmak:bool = False):
    conn = psycopg2.connect(
        dbname="ecommerce",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5433"
    )
    cur = conn.cursor()
    sql_file = os.path.join(path, "scripts", "q2.sql")
    
    with open(sql_file, "r") as f:
        query = f.read()

    if (with_benchmak):
        for i in range(BENCHMARK_COUNT):
            start = time.perf_counter()
            cur.execute(query)
            end = time.perf_counter()
            bench_results["orioledb"].append(end - start)
            print(f'Runtime {i+1} orioledb {end - start}')
    else:
        cur.execute(query)
    cur.execute(query)
    results = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    df = pd.DataFrame(results, columns=colnames)
    
    cur.close()
    conn.close()
    
    return df


# def run_mongo_q2():
#     client = MongoClient("mongodb://localhost:27017/")
#     db = client["ecommerce"]
#     events_collection = db["events"]
    
#     pipeline = [
#         { "$match": { "event_type": { "$in": [ "view", "purchase" ] } } },
#         { "$group": {
#             "_id": { "user_id": "$user_id", "product_id": "$product_id" },
#             "event_count": { "$sum": 1 }
#         }},
#         { "$lookup": {
#             "from": "products",
#             "localField": "_id.product_id",
#             "foreignField": "product_id",
#             "as": "product"
#         }},
#         { "$unwind": "$product" },
#         { "$lookup": {
#             "from": "users",
#             "localField": "_id.user_id",
#             "foreignField": "user_id",
#             "as": "user"
#         }},
#         { "$unwind": "$user" },
#         { "$unwind": "$user.devices" },
#         { "$project": {
#             "client_id": "$user.devices.client_id",
#             "product_id": "$_id.product_id",
#             "event_count": 1,
#             "category_id": "$product.category_id",
#             "category_code": "$product.category_code"
#         }},
#         { "$group": {
#             "_id": "$client_id",
#             "top_recommendations": {
#                 "$topN": {
#                     "output": {
#                         "product_id": "$product_id",
#                         "category_id": "$category_id",
#                         "category_code": "$category_code",
#                         "event_count": "$event_count"
#                     },
#                     "sortBy": { "event_count": -1 },
#                     "n": 3
#                 }
#             }
#         }},
#         { "$sort": { "_id": 1 } }
#     ]
    
#     results = list(events_collection.aggregate(pipeline))
#     df = pd.DataFrame(results)
#     client.close()
#     return df


def run_mongo_q2(with_benchmak:bool = False):
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["ecommerce"]
    events_collection = db["events"]
    
    pipeline = [
        { "$match": { "event_type": { "$in": [ "view", "purchase" ] } } },
        { "$group": {
            "_id": { "user_id": "$user_id", "product_id": "$product_id" },
            "event_count": { "$sum": 1 }
        }},
        { "$lookup": {
            "from": "products",
            "localField": "_id.product_id",
            "foreignField": "product_id",
            "as": "product"
        }},
        { "$unwind": "$product" },
        { "$lookup": {
            "from": "users",
            "localField": "_id.user_id",
            "foreignField": "user_id",
            "as": "user"
        }},
        { "$unwind": "$user" },
        { "$unwind": "$user.devices" },
        { "$project": {
            "client_id": "$user.devices.client_id",
            "product_id": "$_id.product_id",
            "event_count": 1,
            "category_id": "$product.category_id",
            "category_code": "$product.category_code"
        }},
        { "$group": {
            "_id": "$client_id",
            "top_recommendations": {
                "$topN": {
                    "output": {
                        "product_id": "$product_id",
                        "category_id": "$category_id",
                        "category_code": "$category_code",
                        "event_count": "$event_count"
                    },
                    "sortBy": { "event_count": -1 },
                    "n": 5
                }
            }
        }},
        { "$unwind": "$top_recommendations" },
        { "$project": {
            "client_id": "$_id",
            "product_id": "$top_recommendations.product_id",
            "category_id": "$top_recommendations.category_id",
            "category_code": "$top_recommendations.category_code",
            "event_count": "$top_recommendations.event_count"
        }},
        { "$sort": { "client_id": 1 } }
    ]
    
    # Run the aggregation pipeline and convert results to a pandas DataFrame
    if (with_benchmak):
        for i in range(BENCHMARK_COUNT):
            start = time.perf_counter()
            results = db.events_collection.aggregate(pipeline)
            end = time.perf_counter()
            bench_results["mongodb"].append(end - start)
            print(f'Runtime {i+1} mongodb {end - start}')
    else:
        results = db.events_collection.aggregate(pipeline)

    results = list(results)
    df = pd.DataFrame(results)
    client.close()
    return df



if __name__ == "__main__":
    df_oriole = run_oriole_q2(True)
    df_mongo = run_mongo_q2(True)
    df_postgres = run_postgres_q2(True)
    print(df_postgres.head())
    print('Total rows')
    print(len(df_postgres))

    # grouped_recommendations = recommendations_df.groupby('client_id').apply(
    #     lambda df: df[['product_id', 'category_id', 'category_code', 'event_count']].to_dict(orient='records')
    # ).reset_index(name='recommendations')

    # for index, row in grouped_recommendations.head(10).iterrows():
    #     print(f"Client {row['client_id']}: Recommendations: {row['recommendations']}")
    #     print()

    if(bench_results):
        results_dir = "results"
        if not os.path.exists(results_dir):
            os.makedirs(results_dir)
        df_bench = pd.DataFrame({k: v for k, v in bench_results.items() if v})   
        csv_path = os.path.join(results_dir, "q2.csv")
        df_bench.to_csv(csv_path, index=False)
