import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, stddev, min, max, count, to_date, lag,
    when, datediff, first as spark_first, last as spark_last,
    year, month, weekofyear
)
from pyspark.sql.window import Window
import yfinance as yf
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# =====================================================================
# INITIALIZE SPARK
# =====================================================================
spark = SparkSession.builder.appName("StockAnalysis").getOrCreate()

# =====================================================================
# DATA LOADING AND PREPROCESSING FUNCTIONS
# =====================================================================
def load_data(ticker, start_date, end_date):
    data = yf.download(ticker, start=start_date, end=end_date)
    data.reset_index(inplace=True)
    # Assurer que toutes les colonnes sont des chaînes simples
    data.columns = [c if isinstance(c, str) else c[0] for c in data.columns]
    # Ajouter la colonne "Ticker" pour pouvoir partitionner par stock
    data['Ticker'] = ticker
    sdf = spark.createDataFrame(data)
    sdf = sdf.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))
    numeric_cols = ["Open", "Close", "High", "Low", "Volume"]
    for nc in numeric_cols:
        sdf = sdf.withColumn(nc, col(nc).cast("double"))
    return sdf

def detect_periodicity(df):
    # Partitionner par Ticker pour des données multiples (même si ici il n'y a qu'un stock)
    w = Window.partitionBy("Ticker").orderBy("Date")
    df_temp = df.withColumn("Prev_Date", lag("Date").over(w))
    df_temp = df_temp.withColumn("Diff", datediff(col("Date"), col("Prev_Date")))
    avg_diff = df_temp.select(avg("Diff").alias("avgDiff")).first()["avgDiff"]
    if avg_diff is None:
        return "N/A"
    if 0.9 <= avg_diff <= 1.1:
        return "daily"
    elif 6 <= avg_diff <= 8:
        return "weekly"
    elif 28 <= avg_diff <= 31:
        return "monthly"
    else:
        return f"{avg_diff:.1f} days (non standard)"

def add_moving_average(df, column_name, window_size):
    # Calculer la moyenne mobile en partitionnant par Ticker
    window_spec = Window.partitionBy("Ticker").orderBy("Date").rowsBetween(-window_size, 0)
    new_column_name = f"Moving Average ({window_size} days)"
    return df.withColumn(new_column_name, avg(column_name).over(window_spec))

# =====================================================================
# OHLC RESAMPLING FUNCTIONS
# =====================================================================
def resample_ohlc_spark(df, timeframe):
    if timeframe == "daily":
        return df
    elif timeframe == "weekly":
        df_grouped = df.groupBy(year("Date").alias("Year"), weekofyear("Date").alias("Week")) \
            .agg(
                min("Date").alias("Date"),
                spark_first("Open").alias("Open"),
                max("High").alias("High"),
                min("Low").alias("Low"),
                spark_last("Close").alias("Close"),
                spark_last("Volume").alias("Volume")
            ) \
            .orderBy("Year", "Week")
        return df_grouped
    elif timeframe == "monthly":
        df_grouped = df.groupBy(year("Date").alias("Year"), month("Date").alias("Month")) \
            .agg(
                min("Date").alias("Date"),
                spark_first("Open").alias("Open"),
                max("High").alias("High"),
                min("Low").alias("Low"),
                spark_last("Close").alias("Close"),
                spark_last("Volume").alias("Volume")
            ) \
            .orderBy("Year", "Month")
        return df_grouped
    else:
        return df

# =====================================================================
# EXPLORATION AND ANALYSIS FUNCTIONS
# =====================================================================
def calculate_periodic_returns(df, period):
    if period == "weekly":
        df = df.withColumn("Week", col("Date").substr(1, 7))
        df = df.groupBy("Week").agg(((max("Close") - min("Open")) / min("Open") * 100).alias("Weekly Return"))
    elif period == "monthly":
        df = df.withColumn("Month", col("Date").substr(1, 7))
        df = df.groupBy("Month").agg(((max("Close") - min("Open")) / min("Open") * 100).alias("Monthly Return"))
    elif period == "yearly":
        df = df.withColumn("Year", col("Date").substr(1, 4))
        df = df.groupBy("Year").agg(((max("Close") - min("Open")) / min("Open") * 100).alias("Yearly Return"))
    return df

def best_stock_in_period(tickers, start_date, end_date, period, min_volume=0):
    best_stock = None
    best_return = float('-inf')
    return_col = {
        "weekly": "Weekly Return",
        "monthly": "Monthly Return",
        "yearly": "Yearly Return"
    }.get(period, None)

    for name, ticker in tickers.items():
        df_tmp = load_data(ticker, start_date, end_date)
        df_tmp = df_tmp.filter(col("Volume") >= min_volume)
        df_period = calculate_periodic_returns(df_tmp, period)
        if return_col is None:
            continue
        avg_r = df_period.select(avg(return_col)).collect()[0][0]
        if avg_r is not None and avg_r > best_return:
            best_return = avg_r
            best_stock = name

    return best_stock, best_return if best_return != float('-inf') else None

def avg_open_close_different_periods(df):
    results = {}
    df_weekly = df.withColumn("Week", col("Date").substr(1,7))
    df_weekly = df_weekly.groupBy("Week").agg(avg("Open").alias("Avg Open"), avg("Close").alias("Avg Close"))
    results["weekly"] = df_weekly

    df_monthly = df.withColumn("Month", col("Date").substr(1,7))
    df_monthly = df_monthly.groupBy("Month").agg(avg("Open").alias("Avg Open"), avg("Close").alias("Avg Close"))
    results["monthly"] = df_monthly

    df_yearly = df.withColumn("Year", col("Date").substr(1,4))
    df_yearly = df_yearly.groupBy("Year").agg(avg("Open").alias("Avg Open"), avg("Close").alias("Avg Close"))
    results["yearly"] = df_yearly

    return results

# =====================================================================
# VISUALIZATION FUNCTIONS
# =====================================================================
def plot_visualizations(df, title, x, y):
    pdf = df.select(x, y).toPandas()
    fig = px.line(pdf, x=x, y=y, title=title)
    st.plotly_chart(fig, use_container_width=True)

def plot_volatility(df):
    pdf = df.select("Date", "High", "Low").toPandas()
    pdf["Volatility"] = pdf["High"] - pdf["Low"]
    fig = px.line(pdf, x="Date", y="Volatility", title="Volatility (High - Low)")
    st.plotly_chart(fig, use_container_width=True)

def plot_candlestick(df, title="Candlestick Chart (OHLC)"):
    pdf = df.select("Date", "Open", "High", "Low", "Close").toPandas()
    fig = go.Figure(data=[go.Candlestick(
        x=pdf['Date'],
        open=pdf['Open'],
        high=pdf['High'],
        low=pdf['Low'],
        close=pdf['Close']
    )])
    fig.update_layout(title=title, xaxis_rangeslider_visible=False)
    st.plotly_chart(fig, use_container_width=True)

def plot_volume(df):
    pdf = df.select("Date", "Volume").toPandas()
    fig = px.bar(pdf, x="Date", y="Volume", title="Volume Evolution")
    st.plotly_chart(fig, use_container_width=True)

# =====================================================================
# STREAMLIT APPLICATION
# =====================================================================
def main():
    st.sidebar.title("Options")
    tickers = {"Apple": "AAPL", "Microsoft": "MSFT", "Amazon": "AMZN", "Tesla": "TSLA"}

    selected_stock = st.sidebar.selectbox("Select a stock:", list(tickers.keys()))
    start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime("2020-01-01"))
    end_date = st.sidebar.date_input("End Date", value=pd.to_datetime("2023-12-31"))
    min_volume_filter = st.sidebar.number_input("Minimum Volume:", min_value=0, value=0, step=1000)
    candlestick_timeframe = st.sidebar.selectbox("Candlestick Granularity:", ["daily", "weekly", "monthly"])
    period_for_returns = st.sidebar.selectbox("Return Period (Stats):", ["weekly", "monthly", "yearly"])
    window_size = st.sidebar.slider("Moving Average Window (days)", 5, 50, 20)

    if start_date >= end_date:
        st.sidebar.error("Start Date must be earlier than End Date.")
        st.stop()

    df_spark = load_data(tickers[selected_stock], start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))

    st.title("Stock Analysis Application")
    st.subheader(f"Selected stock: {selected_stock}")

    tab_overview, tab_time_series, tab_corr, tab_conclusions = st.tabs([
        "Data Overview",
        "Time Series Analysis",
        "Correlations",
        "Conclusions"
    ])

    with tab_overview:
        st.write("### Total Number of Observations")
        nb_obs = df_spark.count()
        st.info(f"**Total rows:** {nb_obs}")

        col1, col2 = st.columns(2)
        with col1:
            st.write("**First 40 rows**")
            st.dataframe(df_spark.limit(40).toPandas())
        with col2:
            st.write("**Last 40 rows**")
            st.dataframe(df_spark.orderBy(col("Date").desc()).limit(40).toPandas())

        st.write("### Descriptive Statistics")
        stats = df_spark.describe("Open", "Close", "High", "Low", "Volume").toPandas()
        st.table(stats)

        st.write("### Missing Values")
        missing = df_spark.select([count(when(col(c).isNull(), c)).alias(c) for c in df_spark.columns]).toPandas()
        st.table(missing)

        detected = detect_periodicity(df_spark)
        st.write(f"### Detected Periodicity: {detected}")

    with tab_time_series:
        st.subheader("Time Series Analysis")

        # Utilisation d'une fenêtre partitionnée par Ticker
        window_daily = Window.partitionBy("Ticker").orderBy("Date")
        df_spark = df_spark.withColumn("Close_Lag1", lag("Close", 1).over(window_daily))
        df_spark = df_spark.withColumn("Daily Price Change", col("Close") - col("Close_Lag1"))

        df_spark = df_spark.withColumn("MonthID", col("Date").substr(1, 7))
        window_monthly = Window.partitionBy("MonthID").orderBy("Date")
        df_spark = df_spark.withColumn(
            "Close_FirstOfMonth",
            spark_first("Close").over(window_monthly.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
        )
        df_spark = df_spark.withColumn("Monthly Price Change", col("Close") - col("Close_FirstOfMonth"))

        df_spark = df_spark.withColumn("Daily Return", (col("Close") - col("Open")) / col("Open") * 100)

        st.write("### Top 5 Daily Returns")
        top_5_returns = df_spark.orderBy(col("Daily Return").desc()).limit(5).toPandas()
        top_5_returns["Daily Return"] = top_5_returns["Daily Return"].apply(lambda x: f"{x:.2f}%")
        st.table(top_5_returns[["Date", "Open", "Close", "Daily Return"]])

        st.write("### Average Open and Close Prices (Weekly / Monthly / Yearly)")
        multi_avg = avg_open_close_different_periods(df_spark)
        with st.expander("Periods: Weekly / Monthly / Yearly"):
            c1, c2, c3 = st.columns(3)
            with c1:
                st.write("**Weekly**")
                st.dataframe(multi_avg["weekly"].toPandas())
            with c2:
                st.write("**Monthly**")
                st.dataframe(multi_avg["monthly"].toPandas())
            with c3:
                st.write("**Yearly**")
                st.dataframe(multi_avg["yearly"].toPandas())

        st.write("### Time Series Visualizations")
        plot_visualizations(df_spark, "Daily Returns", "Date", "Daily Return")
        plot_visualizations(df_spark, "Daily Price Change", "Date", "Daily Price Change")
        plot_volatility(df_spark)

        df_candle = resample_ohlc_spark(df_spark, candlestick_timeframe)
        plot_candlestick(df_candle, title=f"Candlestick Chart ({candlestick_timeframe})")

        plot_volume(df_spark)

        df_spark = add_moving_average(df_spark, "Close", window_size)
        moving_avg_col = f"Moving Average ({window_size} days)"
        plot_visualizations(df_spark, f"Moving Average ({window_size} days)", "Date", moving_avg_col)

        st.write(f"### Returns by Period: {period_for_returns}")
        periodic_df = calculate_periodic_returns(df_spark, period_for_returns).toPandas()
        st.dataframe(periodic_df)

    with tab_corr:
        st.subheader("Correlations")
        best_s, best_r = best_stock_in_period(
            tickers,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
            period_for_returns,
            min_volume_filter
        )
        st.write(f"### Period: {period_for_returns}")
        if best_s and best_r is not None:
            st.success(f"**Best stock** for period '{period_for_returns}' (volume >= {min_volume_filter}): {best_s} - average return {best_r:.2f}%")
        else:
            st.warning("Unable to determine the best stock (insufficient data or filter too restrictive).")

        st.write("#### Internal Correlation (Open, Close, High, Low, Volume)")
        df_cols = df_spark.select("Open", "Close", "High", "Low", "Volume").toPandas()
        corr_matrix_ticker = df_cols.corr()
        fig_cols_corr = px.imshow(corr_matrix_ticker, text_auto=True, title=f"Column Correlation - {selected_stock}")
        st.plotly_chart(fig_cols_corr, use_container_width=True)

        st.write("#### Correlation Between Two Selected Stocks")
        stock1 = st.selectbox("Select the first stock", list(tickers.keys()), key="stock1")
        stock2 = st.selectbox("Select the second stock", list(tickers.keys()), key="stock2")
        if stock1 and stock2:
            if stock1 == stock2:
                st.info("Select two different stocks for correlation.")
            else:
                df1 = load_data(tickers[stock1], start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")) \
                        .select("Date", "Close").orderBy("Date").toPandas()
                df2 = load_data(tickers[stock2], start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")) \
                        .select("Date", "Close").orderBy("Date").toPandas()
                merged_df = pd.merge(df1, df2, on="Date", suffixes=(f"_{stock1}", f"_{stock2}"))
                if not merged_df.empty:
                    corr_value = merged_df[f"Close_{stock1}"].corr(merged_df[f"Close_{stock2}"])
                    st.success(f"The correlation between {stock1} and {stock2} is: {corr_value:.2f}")
                else:
                    st.warning("No common data available for the two stocks.")

        st.write("#### Inter-Stock Correlation (Based on Close)")
        tickers_corr_data = pd.concat(
            [
                load_data(tickers[name], start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
                    .select("Close").withColumnRenamed("Close", name).toPandas()
                for name in tickers.keys()
            ],
            axis=1
        ).corr()
        fig_corr = px.imshow(tickers_corr_data, text_auto=True, title="Correlation Matrix (Stocks)")
        st.plotly_chart(fig_corr, use_container_width=True)

    with tab_conclusions:
        st.subheader("Conclusions & Insights")
        st.markdown("""
        <style>
        .insight-box {
            background-color: #EFEFEF;
            padding: 15px;
            margin-bottom: 10px;
            border-radius: 5px;
        }
        </style>
        """, unsafe_allow_html=True)
        insights = [
            "1. The total number of observations evaluates historical depth (reliability).",
            "2. The detected periodicity confirms the type of data (daily, weekly, etc.).",
            "3. Daily returns help identify particularly volatile days.",
            "4. Moving averages reveal medium/long-term trends.",
            "5. The OHLC candlestick chart provides a clear view of price evolution.",
            "6. The volume chart illustrates trading intensity (liquidity).",
            "7. Both internal and inter-stock correlations guide diversification.",
            "8. Analysis of top-performing periods helps identify investment opportunities."
        ]
        for ins in insights:
            st.markdown(f"<div class='insight-box'>{ins}</div>", unsafe_allow_html=True)

if __name__ == "__main__":
    st.set_page_config(
        page_title="Stock Analysis App",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    main()
