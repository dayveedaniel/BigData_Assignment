#!/usr/bin/env python3
import psycopg2
import os
import json
import matplotlib.pyplot as plt
import pandas as pd
from pymongo import MongoClient
from neo4j import GraphDatabase



path = os.getcwd()


def make_autopct(values):
    """
    Create a function for autopct that shows both percentage and count.
    """
    def my_autopct(pct):
        total = sum(values)
        count = int(round(pct*total/100.0))
        return '{p:.1f}%\n({v:d})'.format(p=pct, v=count)
    return my_autopct

def plot_purchase_ratio(df):
    """
    Pie chart showing the overall ratio of users who made purchases due to campaigns versus those who didn't.
    Aggregates over all campaigns.
    """
    total_received = df["users_received"].sum()
    total_purchased = df["users_purchased"].sum()
    not_purchased = total_received - total_purchased

    sizes = [total_purchased, not_purchased]
    labels = ["Purchased", "Not Purchased"]
    colors = ['#66b3ff', '#ff9999']

    plt.figure(figsize=(8, 8))
    plt.pie(sizes, labels=labels, autopct=make_autopct(sizes), colors=colors, startangle=140)
    plt.title("Overall Purchase Ratio")
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.show()

def plot_campaign_type_users(df):
    """
    Pie chart showing, for each campaign type, the total number of users reached.
    """
    campaign_group = df.groupby("campaign_type")["users_received"].sum()
    labels = campaign_group.index.tolist()
    sizes = campaign_group.values.tolist()
    # Define a list of colors (extend or change as needed)
    colors = ['#ff9999','#66b3ff','#99ff99','#ffcc99', '#c2c2f0', '#ffb3e6'][:len(labels)]

    plt.figure(figsize=(8, 8))
    plt.pie(sizes, labels=labels, autopct=make_autopct(sizes), colors=colors, startangle=140)
    plt.title("Users Reached by Campaign Type")
    plt.axis('equal')
    plt.show()

def plot_message_channel_usage(df):
    """
    Pie chart showing usage frequency of each message channel based on the count of campaigns.
    """
    channel_group = df.groupby("channel")["campaign_id"].count()
    labels = channel_group.index.tolist()
    sizes = channel_group.values.tolist()
    colors = ['#c2c2f0','#ffb3e6','#ff9999','#66b3ff', '#99ff99', '#ffcc99'][:len(labels)]

    plt.figure(figsize=(8, 8))
    plt.pie(sizes, labels=labels, autopct=make_autopct(sizes), colors=colors, startangle=140)
    plt.title("Message Channel Usage (by Campaign Count)")
    plt.axis('equal')
    plt.show()

def plot_message_channel_users(df):
    """
    Pie chart showing which message channel brought the most users.
    For each channel, the total 'users_received' is summed.
    """
    channel_group = df.groupby("channel")["users_received"].sum()
    labels = channel_group.index.tolist()
    sizes = channel_group.values.tolist()
    colors = ['#ffcc99','#99ff99','#66b3ff','#ff9999', '#c2c2f0', '#ffb3e6'][:len(labels)]

    plt.figure(figsize=(8, 8))
    plt.pie(sizes, labels=labels, autopct=make_autopct(sizes), colors=colors, startangle=140)
    plt.title("Users Reached by Message Channel")
    plt.axis('equal')
    plt.show()




def run_neo4j_analysis():
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "12345678" 
    
    cypher_file_path = os.path.join(path, "scripts", "q1.cypher")

    with open(cypher_file_path, "r") as file:
        query = file.read()

    driver = GraphDatabase.driver(uri, auth=(user, password))
    records = []
    
    # Execute the query and collect results.
    with driver.session() as session:
        result = session.run(query)
        for record in result:
            records.append({
                "campaign_id": record["campaign_id"],
                "campaign_type": record["campaign_type"],
                "channel": record["channel"],
                "users_received": record["users_received"],
                "users_purchased": record["users_purchased"],
                "purchase_percentage": record["purchase_percentage"]
            })
    
    driver.close()
    
    # Create a pandas DataFrame with the required column order.
    df = pd.DataFrame(records, columns=[
        "campaign_id", 
        "campaign_type", 
        "channel",
        "users_received", 
        "users_purchased", 
        "purchase_percentage", 
      
    ])

    print(df.head())
    
    return df



def run_postgres_analysis():
    conn = psycopg2.connect(
        dbname="ecommerce",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    sql_file = os.path.join(path, "scripts", "q1.sql")
    
    # Read the SQL query from the file
    with open(sql_file, "r") as f:
        query = f.read()
    
    cur.execute(query)
    results = cur.fetchall()
    # Get column names from the cursor description
    colnames = [desc[0] for desc in cur.description]
    df = pd.DataFrame(results, columns=colnames)
    
    cur.close()
    conn.close()
    
    return df


def run_mongo_analysis():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["ecommerce"]

    pipeline = [
        {
            "$lookup": {
                "from": "campaigns",
                "localField": "campaign_id",
                "foreignField": "_id",
                "as": "campaign_info"
            }
        },
        { "$unwind": "$campaign_info" },
        { "$group": {
            "_id": {
                "campaign_id": "$campaign_info.campaign_id",
                "campaign_type": "$campaign_info.campaign_type",
                "channel": "$campaign_info.channel"
            },
            "users_received": { "$addToSet": "$client_id" },
            "users_purchased": { 
                "$addToSet": {
                    "$cond": [
                        { "$ne": [ "$purchased_at", None ] },
                        "$client_id",
                        "$$REMOVE"
                    ]
                }
            },
        }},
        { "$project": {
            "campaign_id": "$_id.campaign_id",
            "campaign_type": "$_id.campaign_type",
            "channel": "$_id.channel",
            "users_received": { "$size": "$users_received" },
            "users_purchased": { "$size": "$users_purchased" },
            "purchase_percentage": {
                "$cond": [
                    { "$gt": [ { "$size": "$users_received" }, 0 ] },
                    { "$round": [ { "$multiply": [ { "$divide": [ { "$size": "$users_purchased" }, { "$size": "$users_received" } ] }, 100 ] }, 2 ] },
                    0
                ]
            },
            "_id": 0
        }},
        { "$sort": { "purchase_percentage": -1 } }
    ]
    
    # Run the pipeline on the messages collection.
    results = list(db.messages.aggregate(pipeline))
    
    # Create DataFrame with the specified columns.
    df = pd.DataFrame(results, columns=["campaign_id", "campaign_type", "channel", "users_received", "users_purchased", "purchase_percentage"])
    
    return df


if __name__ == "__main__":
    # df = run_mongo_analysis()
    # df = run_neo4j_analysis()
    df = run_postgres_analysis()
    
    total_campaigns = len(df)
    print(df.head())
    
    print(f"Total campaigns returned: {total_campaigns}")
    
    plot_purchase_ratio(df)
    plot_campaign_type_users(df)
    plot_message_channel_usage(df)
    plot_message_channel_users(df)