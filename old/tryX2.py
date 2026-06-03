# import pandas as pd
# import urllib.parse
# import matplotlib.pyplot as plt
# import seaborn as sns

# # 1. Load the data (we only need the query string column for this)
# df = pd.read_csv("full_rows_device_id_4cda938608ed98a8_1.csv", usecols=["queryStr"])

# # 2. Define a helper function to extract parameters from the URL strings
# def extract_query_param(query, param):
#     try:
#         # Parse the query string into a dictionary
#         parsed = urllib.parse.parse_qs(query)
#         # Extract the specific parameter (like 'channel' or 'content_title')
#         if param in parsed:
#             return parsed[param][0] 
#         return "Unknown"
#     except Exception:
#         return "Unknown"

# # 3. Create new columns by applying our function to the queryStr column
# df['channel'] = df['queryStr'].apply(lambda x: extract_query_param(x, 'channel'))
# df['content_title'] = df['queryStr'].apply(lambda x: extract_query_param(x, 'content_title'))

# # 4. Count the top 5 Channels and top 5 Content Titles
# top_channels = df['channel'].value_counts().head(5)
# top_content = df['content_title'].value_counts().head(5)

# # 5. Set up the plotting environment
# sns.set_theme(style="whitegrid")
# fig, axes = plt.subplots(1, 2, figsize=(16, 7))

# # --- Plot 1: Top Channels (Pie Chart) ---
# axes[0].pie(top_channels.values, labels=top_channels.index, autopct='%1.1f%%', 
#             startangle=140, colors=sns.color_palette("pastel"))
# axes[0].set_title("Top Watched Channels", fontsize=16, fontweight='bold')

# # --- Plot 2: Top Content Titles (Horizontal Bar Chart) ---
# sns.barplot(y=top_content.index, x=top_content.values, ax=axes[1], palette="viridis")
# axes[1].set_title("Top Watched Content/Categories", fontsize=16, fontweight='bold')
# axes[1].set_xlabel("Volume of Requests (Watch Time Proxy)", fontsize=12)
# axes[1].set_ylabel("")

# # Clean up layout and display
# plt.tight_layout()
# plt.savefig("content_analysis.png", dpi=300)
# plt.show()


import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Load the data (selecting only the necessary column to save memory)
df = pd.read_csv("full_rows_device_id_4cda938608ed98a8_1.csv", usecols=["reqTimeSec"])

# 2. Convert Unix Timestamp to IST Datetime
# First convert to UTC datetime
df['datetime_utc'] = pd.to_datetime(df['reqTimeSec'], unit='s')
# Add 5 hours and 30 minutes for IST
df['datetime_ist'] = df['datetime_utc'] + pd.Timedelta(hours=5, minutes=30)

# 3. Extract the specific 'Date' and 'Hour'
df['date'] = df['datetime_ist'].dt.date
df['hour'] = df['datetime_ist'].dt.hour

# 4. Set up the visual styling
sns.set_theme(style="whitegrid")
plt.figure(figsize=(14, 10))

# --- Plot 1: Daily Watch Activity ---
plt.subplot(2, 1, 1)
daily_counts = df.groupby('date').size()
sns.barplot(x=daily_counts.index, y=daily_counts.values, color="#3498db") # Professional blue
plt.title("Daily Watch Activity (Number of Video Fragments Requested)", fontsize=16, fontweight='bold')
plt.xlabel("Date", fontsize=12)
plt.ylabel("Activity Volume", fontsize=12)
plt.xticks(rotation=45)

# --- Plot 2: Watch Activity by Hour of the Day ---
plt.subplot(2, 1, 2)
hourly_counts = df.groupby('hour').size()
sns.barplot(x=hourly_counts.index, y=hourly_counts.values, color="#e74c3c") # Professional red
plt.title("Watch Activity by Time of Day (IST)", fontsize=16, fontweight='bold')
plt.xlabel("Hour of the Day (0-23)", fontsize=12)
plt.ylabel("Activity Volume", fontsize=12)
plt.xticks(range(0, 24))

# 5. Clean up layout and save/show the plot
plt.tight_layout()
plt.savefig("my_watch_history.png", dpi=300)
plt.show()