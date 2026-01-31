import pandas as pd
import sqlite3


# Loading orders data 
orders = pd.read_csv("orders.csv")
print(orders.head())


# Loading users data

users = pd.read_json("users.json")
print(users.head())


#Connecting database

connection = sqlite3.connect("restaurant.db")
cursor = connection.cursor()


with open("restaurants.sql", "r") as f:
    sql_script = f.read()
    cursor.executescript(sql_script)
    connection.commit()


# Loading restaurants table

restaurant = pd.read_sql("SELECT * FROM restaurants", connection)

#removing duplicates if any

restaurant = restaurant.drop_duplicates(subset="restaurant_id")

print(restaurant.head())



# Orders + Users (LEFT JOIN to retain all orders)
orders_users = pd.merge(
    orders,
    users,
    on="user_id",
    how="left"
)

# Orders_Users + Restaurants (LEFT JOIN)
final_data = pd.merge(
    orders_users,
    restaurant,
    on="restaurant_id",
    how="left"
)

#saving final data to csv

final_data.to_csv("final_food_delivery_dataset.csv", index=False)

#checking rows and columns of dataframes

print("Orders rows:", orders.shape[0])
print("Final rows:", final_data.shape[0])   
print("Restaurant columns:", restaurant.columns)

#Which city has the highest total revenue (total_amount) from Gold members?  

gold_members = final_data[final_data["membership"] == "Gold"]
city_revenue = gold_members.groupby("city")["total_amount"].sum()
highest_revenue_city = city_revenue.idxmax()

print(highest_revenue_city)

#Which cuisine has the highest average order value across all orders?

cuisine_avg = final_data.groupby("cuisine")["total_amount"].mean()
highest_avg_cuisine = cuisine_avg.idxmax()

print(highest_avg_cuisine)

#How many distinct users placed orders worth more than ₹1000 in total (sum of all their orders)?

user_total = final_data.groupby("user_id")["total_amount"].sum()
users_above_1000 = user_total[user_total > 1000]
count_users = users_above_1000.count()

print(count_users)

#Which restaurant rating range generated the highest total revenue?

final_data["rating_range"] = pd.cut(
    final_data["rating"],
    bins=[3.0, 3.5, 4.0, 4.5, 5.0],
    labels=["3.0–3.5", "3.6–4.0", "4.1–4.5", "4.6–5.0"],
    include_lowest=True
)
rating_revenue = final_data.groupby("rating_range")["total_amount"].sum()
highest_revenue_range = rating_revenue.idxmax()

print(highest_revenue_range)

#Among Gold members, which city has the highest average order value?

gold_data = final_data[final_data["membership"] == "Gold"]
city_avg = gold_data.groupby("city")["total_amount"].mean()
highest_avg_city = city_avg.idxmax()

print(highest_avg_city)

#Which cuisine has the lowest number of distinct restaurants but still contributes significant revenue?

restaurant_count = final_data.groupby("cuisine")["restaurant_id"].nunique()
total_revenue = final_data.groupby("cuisine")["total_amount"].sum()
cuisine_summary = pd.DataFrame({
    "restaurant_count": restaurant_count,
    "total_revenue": total_revenue
})

threshold = cuisine_summary["total_revenue"].quantile(0.75)
significant = cuisine_summary[cuisine_summary["total_revenue"] >= threshold]
answer = significant["restaurant_count"].idxmin()

print(answer)


#What percentage of total orders were placed by Gold members? (Rounded to nearest integer)

total_orders = final_data.shape[0]
gold_orders = final_data[final_data["membership"] == "Gold"].shape[0]
percentage = round((gold_orders / total_orders) * 100)

print(percentage)

#Which restaurant has the highest average order value but less than 20 total orders?

restaurant_stats = (
    final_data
    .groupby("restaurant_id")
    .agg(
        order_count=("order_id", "count"),
        avg_order_value=("total_amount", "mean")
    )
)

filtered = restaurant_stats[restaurant_stats["order_count"] < 20]
answer = filtered["avg_order_value"].idxmax()

print(answer)

#Which combination contributes the highest revenue?

combo_revenue = (
    final_data
    .groupby(["membership", "cuisine"])["total_amount"]
    .sum()
    .reset_index()
    .sort_values("total_amount", ascending=False)
)

print(combo_revenue)


#During which quarter of the year is the total revenue highest?

final_data["order_date"] = pd.to_datetime(final_data["order_date"])
final_data["quarter"] = final_data["order_date"].dt.quarter
quarter_revenue = final_data.groupby("quarter")["total_amount"].sum()

print(quarter_revenue)

#How many total orders were placed by users with Gold membership?

gold_orders_count = final_data[final_data["membership"] == "Gold"].shape[0]

print(gold_orders_count)

#What is the total revenue (rounded to nearest integer) generated from orders placed in Hyderabad city?

hyderabad_revenue = final_data[final_data["city"] == "Hyderabad"]["total_amount"].sum()

print(round(hyderabad_revenue))


#How many distinct users placed at least one order?

distinct_users = final_data["user_id"].nunique()

print(distinct_users)


#What is the average order value (rounded to 2 decimals) for Gold members?

gold_avg_order_value = final_data[final_data["membership"] == "Gold"]["total_amount"].mean()

print(round(gold_avg_order_value, 2))


#How many orders were placed for restaurants with rating ≥ 4.5?

orders_high_rating = final_data[final_data["rating"] >= 4.5].shape[0]

print(orders_high_rating)

#How many orders were placed in the top revenue city among Gold members only?

top_city = (
    final_data[final_data["membership"] == "Gold"]
    .groupby("city")["total_amount"]
    .sum()
    .idxmax()
)

orders_count = final_data[
    (final_data["membership"] == "Gold") &
    (final_data["city"] == top_city)
].shape[0]

print(orders_count)

