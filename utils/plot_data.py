import matplotlib.pyplot as plt

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

