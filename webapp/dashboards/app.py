import streamlit as st
import pandas as pd
import os
import glob

# App Title
st.title("E-Commerce Orders Dashboard")

# Let user choose between 'by_user' and 'by_product'
aggregation_type = st.selectbox("Select Aggregation Type", ["by_user", "by_product"])

# Define folder path based on selection
folder_path = f"data/processed/aggregated_orders/{aggregation_type}/"
parquet_files = sorted(
    glob.glob(os.path.join(folder_path, "*.parquet")),
    reverse=True
)

# Load and display data if available
if parquet_files:
    df = pd.read_parquet(parquet_files[0])

    st.subheader("Sample of Aggregated Orders")
    st.dataframe(df.head())

    st.subheader("Metrics Overview")

    if aggregation_type == "by_user":
        st.metric("Total Users", len(df))

        if "orders_by_user" in df.columns:
            st.metric("Total Orders", df["orders_by_user"].sum())

        if "total_spent" in df.columns:
            avg_spent = df["total_spent"].mean()
            st.metric("Avg Spent per User", f"${avg_spent:.2f}")
            st.subheader("Distribution of Total Spent")
            st.bar_chart(df["total_spent"])

    elif aggregation_type == "by_product":
        st.metric("Total Products", len(df))

        if "price" in df.columns:
            avg_price = df["price"].mean()
            st.metric("Average Price", f"${avg_price:.2f}")

        if "category" in df.columns:
            st.subheader("Orders by Category")
            st.bar_chart(df["category"].value_counts())

        if "order_value_bucket" in df.columns:
            st.subheader("Orders by Value Bucket")
            st.bar_chart(df["order_value_bucket"].value_counts())

else:
    st.warning(f"No parquet files found in '{aggregation_type}' directory.")
