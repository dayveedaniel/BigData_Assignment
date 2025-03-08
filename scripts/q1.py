#!/usr/bin/env python3
import psycopg2
import os
import json
import matplotlib.pyplot as plt
import pandas as pd
from pymongo import MongoClient

path = os.getcwd()

def plot_campaigns(df):
    """
    Plots a stacked bar chart for campaigns where the purchase rate is greater than 50%.
    The chart shows the number of users who purchased vs. those who did not purchase.
    """
    # Calculate number of users who did not purchase
    df["users_not_purchased"] = df["users_received"] - df["users_purchased"]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(df["campaign_id"], df["users_purchased"], label="Users Purchased", color="green")
    ax.bar(df["campaign_id"], df["users_not_purchased"],
           bottom=df["users_purchased"], label="Users Not Purchased", color="red")

    # Annotate each bar with the purchase rate percentage
    for idx, row in df.iterrows():
        ax.text(row["campaign_id"], row["users_received"] + 1, f"{row['purchase_rate_percentage']:.1f}%",
                ha="center", va="bottom", fontsize=10)

    ax.set_xlabel("Campaign ID")
    ax.set_ylabel("Number of Users")
    ax.set_title("High-Performing Campaigns (Purchase Rate > 50%)")
    ax.legend()
    plt.tight_layout()
    plt.show()

    
def plot_campaign_type_aggregate(df):
    """
    Aggregates the data by campaign type and plots a bar chart showing the overall purchase rate
    for each campaign type.
    """
    grouped = df.groupby("campaign_type").agg({
        "users_received": "sum",
        "users_purchased": "sum"
    }).reset_index()
    grouped["purchase_rate"] = (grouped["users_purchased"] / grouped["users_received"]) * 100

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.bar(grouped["campaign_type"], grouped["purchase_rate"], color="blue")

    # Annotate each bar with the purchase rate percentage
    for idx, row in grouped.iterrows():
        ax.text(row["campaign_type"], row["purchase_rate"] + 1, f"{row['purchase_rate']:.1f}%",
                ha="center", va="bottom", fontsize=10)

    ax.set_xlabel("Campaign Type")
    ax.set_ylabel("Aggregate Purchase Rate (%)")
    ax.set_title("Aggregate Purchase Rate by Campaign Type")
    plt.tight_layout()
    plt.show()


def run_postgres_analysis():
    """
    Connects to the PostgreSQL database, executes the SQL query (stored in scripts/q1.sql),
    and returns the results as a pandas DataFrame.
    """
    conn = psycopg2.connect(
        dbname="ecommerce",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    
    sql_file = os.path.join(path, "scripts", "q1.sql")
    
    with open(sql_file, "r") as f:
        query = f.read()
    
    cur.execute(query)
    results = cur.fetchall()
    
    # Define column names corresponding to the SQL SELECT output:
    columns = ["campaign_id", "campaign_type", "users_received", "users_purchased", "purchase_rate_percentage", "message_details"]
    df = pd.DataFrame(results, columns=columns)
    
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
                "campaign_type": "$campaign_info.campaign_type"
            },
            # Collect distinct client_ids for users who received a message
            "users_received": { "$addToSet": "$client_id" },
            # Collect distinct client_ids where purchased_at is not null
            "users_purchased": { 
                "$addToSet": {
                    "$cond": [
                        { "$ne": [ "$purchased_at", None ] },
                        "$client_id",
                        "$$REMOVE"
                    ]
                }
            },
            # Aggregate message details
            "message_details": { "$push": {
                "message_id": "$message_id",
                "platform": "$platform",
                "stream": "$stream",
                "sent_at": "$sent_at"
            } }
        }},
        { "$project": {
            "campaign_id": "$_id.campaign_id",
            "campaign_type": "$_id.campaign_type",
            "users_received": { "$size": "$users_received" },
            "users_purchased": { "$size": "$users_purchased" },
            "purchase_rate_percentage": {
                "$cond": [
                    { "$gt": [ { "$size": "$users_received" }, 0 ] },
                    { "$round": [ { "$multiply": [ { "$divide": [ { "$size": "$users_purchased" }, { "$size": "$users_received" } ] }, 100 ] }, 2 ] },
                    0
                ]
            },
            "message_details": 1,
            "_id": 0
        }},
        { "$sort": { "purchase_rate_percentage": -1 } }
    ]
    
    # Run the pipeline on the messages collection.
    results = list(db.messages.aggregate(pipeline))
    total_campaigns = len(results)
    campaigns_with_purchase = 0

    print("Campaign Purchase Analysis (MongoDB):")
    for doc in results:
        print(f"Campaign ID: {doc.get('campaign_id')}, Campaign Type: {doc.get('campaign_type')}, Users Received: {doc.get('users_received')}, Users Purchased: {doc.get('users_purchased')}, Purchase Rate: {doc.get('purchase_rate_percentage'):.2f}%")
        print("Purchase Details:")
        print(json.dumps(doc.get("message_details", []), indent=2, default=str))
        print("-" * 50)
    
    
    # Return the results as a pandas DataFrame with the same format as the PostgreSQL query.
    df = pd.DataFrame(results, columns=["campaign_id", "campaign_type", "users_received", "users_purchased", "purchase_rate_percentage", "message_details"])
    return df

    

if __name__ == "__main__":
    df = run_mongo_analysis()
    # df = run_postgres_analysis()
    
    total_campaigns = len(df)
    
    print(f"Total campaigns returned: {total_campaigns}")
    
    # Plot the charts
    plot_campaigns(df)
    plot_campaign_type_aggregate(df)