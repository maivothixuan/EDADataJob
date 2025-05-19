# Databricks notebook source
# MAGIC %pip install plotly

# COMMAND ----------

# MAGIC %pip install squarify

# COMMAND ----------

pip install pycountry

# COMMAND ----------

# MAGIC %r
# MAGIC install.packages("psych")

# COMMAND ----------

# MAGIC %pip install prophet
# MAGIC

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Global font config
plt.rcParams.update({
    "font.size": 14,              # Set base font size
    "font.family": "sans-serif",       # Match common web-safe font
    "figure.autolayout": True,    # Helps tight layout
    "figure.facecolor": "none",   # Match transparent background if needed
    "axes.titlesize": 18,
    "axes.titleweight": "bold",
    "axes.labelsize": 14,
    "axes.edgecolor": "none",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.spines.left": False,
    "axes.spines.bottom": False
})


# COMMAND ----------

dbutils.fs.ls("/FileStore/tables")
#dbutils.fs.rm("/FileStore/tables/awproductsubcategory.csv")

# COMMAND ----------

#dbutils.fs.rm("/FileStore/tables/awproductsubcategory.csv")


# COMMAND ----------

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark session
spark = SparkSession.builder.appName("Load_DS_Salaries").getOrCreate()

# Define file path
file_path = "dbfs:/FileStore/tables/ds_salaries.csv"

# Load CSV into a DataFrame
df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

# Display the first few rows
display(df)

# Register the table in the default database
table_name = "ds_salaries"
df.write.mode("overwrite").saveAsTable(table_name)

print(f"Table '{table_name}' created successfully!")


# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW COLUMNS  IN ds_salaries  IN default

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ds_salaries_view AS
# MAGIC SELECT 
# MAGIC     work_year AS WorkYear,
# MAGIC     
# MAGIC     TRIM(experience_level) AS ExperienceLevel,
# MAGIC     CASE 
# MAGIC         WHEN TRIM(experience_level) = 'EN' THEN 'Entry-level/Junior'
# MAGIC         WHEN TRIM(experience_level) = 'MI' THEN 'Mid-level/Intermediate'
# MAGIC         WHEN TRIM(experience_level) = 'SE' THEN 'Senior-level/Expert'
# MAGIC         WHEN TRIM(experience_level) = 'EX' THEN 'Executive-level/Director'
# MAGIC         ELSE 'Unknown'
# MAGIC     END AS ExperienceLevelName,
# MAGIC     
# MAGIC     TRIM(employment_type) AS EmploymentType,
# MAGIC     CASE 
# MAGIC         WHEN TRIM(employment_type) = 'PT' THEN 'Part-time'
# MAGIC         WHEN TRIM(employment_type) = 'FT' THEN 'Full-time'
# MAGIC         WHEN TRIM(employment_type) = 'CT' THEN 'Contract'
# MAGIC         WHEN TRIM(employment_type) = 'FL' THEN 'Freelance'
# MAGIC         ELSE 'Unknown'
# MAGIC     END AS EmploymentTypeName,
# MAGIC     
# MAGIC     TRIM(job_title) AS JobTitle,
# MAGIC     salary AS Salary,
# MAGIC     salary_currency AS SalaryCurrency,
# MAGIC     salary_in_usd AS SalaryInUSD,
# MAGIC     
# MAGIC     TRIM(employee_residence) AS EmployeeResidence,
# MAGIC     remote_ratio AS RemoteRatio,
# MAGIC     TRIM(company_location) AS CompanyLocation,
# MAGIC     TRIM(company_size) AS CompanySize,
# MAGIC
# MAGIC     -- Employee Region
# MAGIC     CASE 
# MAGIC         WHEN TRIM(employee_residence) IN ('US', 'CA') THEN 'North America'
# MAGIC         WHEN TRIM(employee_residence) IN ('GB', 'DE', 'FR', 'PL', 'IT', 'NL', 'RO', 'GR', 'BG', 'CZ', 'ES', 'PT') THEN 'Europe'
# MAGIC         WHEN TRIM(employee_residence) IN ('IN', 'SG', 'PH', 'PK', 'JP', 'VN', 'MY', 'CN') THEN 'Asia'
# MAGIC         WHEN TRIM(employee_residence) IN ('AU') THEN 'Oceania'
# MAGIC         WHEN TRIM(employee_residence) IN ('NG', 'GH', 'CV', 'TN') THEN 'Africa'
# MAGIC         WHEN TRIM(employee_residence) IN ('AR', 'BR', 'CL', 'BO') THEN 'Latin America'
# MAGIC         ELSE 'Other'
# MAGIC     END AS EmployeeRegion,
# MAGIC
# MAGIC     -- Company Region
# MAGIC     CASE 
# MAGIC         WHEN TRIM(company_location) IN ('US', 'CA') THEN 'North America'
# MAGIC         WHEN TRIM(company_location) IN ('GB', 'DE', 'FR', 'PL', 'IT', 'NL', 'RO', 'GR', 'BG', 'CZ', 'ES', 'PT') THEN 'Europe'
# MAGIC         WHEN TRIM(company_location) IN ('IN', 'SG', 'PH', 'PK', 'JP', 'VN', 'MY', 'CN') THEN 'Asia'
# MAGIC         WHEN TRIM(company_location) IN ('AU') THEN 'Oceania'
# MAGIC         WHEN TRIM(company_location) IN ('NG', 'GH', 'CV', 'TN') THEN 'Africa'
# MAGIC         WHEN TRIM(company_location) IN ('AR', 'BR', 'CL', 'BO') THEN 'Latin America'
# MAGIC         ELSE 'Other'
# MAGIC     END AS CompanyRegion
# MAGIC
# MAGIC FROM default.ds_salaries;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW COLUMNS  IN ds_salaries_view  IN default

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ds_salaries_view limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT jobTitle, COUNT(*) AS JobCount
# MAGIC FROM ds_salaries_view
# MAGIC GROUP BY jobTitle
# MAGIC ORDER BY JobCount DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

import plotly.express as px

# Run Spark SQL to get company size counts
df_company_size = spark.sql("""
    SELECT CompanySize, COUNT(*) AS Count
    FROM ds_salaries_view
    GROUP BY CompanySize
""").toPandas()

# Map single-letter codes to full names
size_map = {"S": "Small", "M": "Medium", "L": "Large"}
df_company_size["CompanySize"] = df_company_size["CompanySize"].map(size_map)

# Define fixed color mapping
color_map = {
    "Small": "#bd6e9c",
    "Medium": "#49aeb0",
    "Large": "#695da9"
}

# Create Pie Chart with defined colors
fig = px.pie(
    df_company_size,
    names="CompanySize",
    values="Count",
    title="Company Size Distribution",
    color="CompanySize",
    color_discrete_map=color_map
)

# Consistent layout formatting
fig.update_layout(
    title=dict(
        text="Company Size Distribution",
        x=0,
        xanchor='left',
        y=0.95,
        font=dict(size=20, family="sans-serif")
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    paper_bgcolor='rgba(0,0,0,0)',
    showlegend=True
)

fig.show()


# COMMAND ----------

import plotly.graph_objects as go
import pandas as pd
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.pyplot as plt

# Fetch data
df = spark.sql("""
    SELECT JobTitle, COUNT(*) AS JobCount
    FROM ds_salaries_view
    GROUP BY JobTitle
    ORDER BY JobCount DESC
    LIMIT 10
""").toPandas()

# Define 4-color gradient
gradient_colors = ["#bd6e9c", "#49aeb0", "#695da9", "#88673f"]
cmap = LinearSegmentedColormap.from_list("custom_4color", gradient_colors, N=len(df))

# Normalize job counts and generate RGBA color per bar
norm = plt.Normalize(df["JobCount"].min(), df["JobCount"].max())
rgba_colors = [cmap(norm(value)) for value in df["JobCount"]]
hex_colors = [f'rgba({int(r*255)},{int(g*255)},{int(b*255)},{a})' for r, g, b, a in rgba_colors]

# Build Plotly bar chart
fig = go.Figure()

fig.add_trace(go.Bar(
    x=df["JobCount"],
    y=df["JobTitle"],
    orientation='h',
    marker=dict(color=hex_colors),
    text=df["JobCount"],
    textposition='outside',
    hovertemplate='<b>%{y}</b><br>Job Count: %{x}<extra></extra>'
))

# Consistent layout
fig.update_layout(
    title=dict(
        text="Top 10 Job Designations",
        x=0,
        xanchor='left',
        y=0.95,
        yanchor='top',
        font=dict(size=20, family="sans-serif", color="black")
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    xaxis_title=None,
    yaxis_title=None,
    yaxis=dict(autorange="reversed"),
    showlegend=False
)

fig.show()


# COMMAND ----------

import pandas as pd
import plotly.express as px

# Step 1: Fetch job title + experience level
df = spark.sql("""
    SELECT JobTitle, ExperienceLevelName
    FROM ds_salaries_view
""").toPandas()

# Step 2: Find Top 5 Job Titles overall
top_5_jobs = (
    df["JobTitle"]
    .value_counts()
    .nlargest(5)
    .index.tolist()
)

# Step 3: Filter and group by ExperienceLevel
df_filtered = df[df["JobTitle"].isin(top_5_jobs)]
df_grouped = df_filtered.groupby(["ExperienceLevelName", "JobTitle"]).size().reset_index(name="Count")

# Optional: Set order for ExperienceLevel
experience_order = [
    "Entry-level/Junior", "Mid-level/Intermediate",
    "Senior-level/Expert", "Executive-level/Director"
]
df_grouped["ExperienceLevelName"] = pd.Categorical(df_grouped["ExperienceLevelName"], categories=experience_order, ordered=True)
df_grouped = df_grouped.sort_values("ExperienceLevelName")

# Step 4: Plot grouped bar chart
fig = px.bar(
    df_grouped,
    x="ExperienceLevelName",
    y="Count",
    color="JobTitle",
    text="Count",
    barmode="group",
    title="Top 5 Job Titles by Experience Level",
    color_discrete_sequence=px.colors.qualitative.Set2
)

# Step 5: Apply Chart 1 layout
fig.update_traces(
    texttemplate='%{text:,}',
    textposition='outside'
)

fig.update_layout(
    title=dict(
        text="Top 5 Job Titles by Experience Level",
        x=0,
        xanchor='left',
        y=0.95,
        font=dict(size=20, family="sans-serif")
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis_title=None,
    yaxis_title=None,
    legend_title="Job Title"
)

fig.show()


# COMMAND ----------

import pandas as pd
import plotly.express as px

# Step 1: Fetch and aggregate data
df = spark.sql("""
    SELECT EmployeeResidence, COUNT(*) AS Count
    FROM ds_salaries_view
    GROUP BY EmployeeResidence
    ORDER BY Count DESC
    LIMIT 10
""").toPandas()

# Optional: Map country codes to names (ISO alpha-2 if needed)
# You can use pycountry or a manual dict if needed

# Step 2: Plotly bar chart
fig = px.bar(
    df,
    x="EmployeeResidence",
    y="Count",
    text="Count",
    title="Top 10 Employee Residences",
    labels={"EmployeeResidence": "Country", "Count": "Employee Count"},
    color="EmployeeResidence",
    color_discrete_sequence=px.colors.qualitative.Set2
)

# Step 3: Apply Chart 1 layout rules
fig.update_traces(
    texttemplate='%{text:,}',  # show formatted number
    textposition='outside'
)

fig.update_layout(
    title=dict(
        text="Top 10 Employee Residences",
        x=0,
        xanchor='left',
        y=0.95,
        font=dict(size=20, family="sans-serif")
    ),
    xaxis_title=None,
    yaxis_title=None,
    margin=dict(l=10, r=10, t=40, b=10),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    showlegend=False
)

fig.show()


# COMMAND ----------

import pandas as pd
import plotly.express as px

# Step 1: Load data
df = spark.sql("SELECT WorkYear FROM ds_salaries_view").toPandas()

# Step 2: Count records per year
work_year_counts = df["WorkYear"].value_counts().sort_index()
df_counts = pd.DataFrame({
    "WorkYear": work_year_counts.index.astype(str),
    "Count": work_year_counts.values
})

# Step 3: Plot bar chart
fig = px.bar(
    df_counts,
    x="WorkYear",
    y="Count",
    text="Count",
    title="Job Record Distribution by Work Year",
    labels={"WorkYear": "Work Year", "Count": "Job Count"},
    color="WorkYear",  # auto color by year
    color_discrete_sequence=px.colors.sequential.Teal
)

# Step 4: Apply Chart 1 styling
fig.update_traces(
    texttemplate='%{text:,}',
    textposition='outside'
)

fig.update_layout(
    title=dict(
        text="Job Record Distribution by Work Year",
        x=0,
        xanchor='left',
        font=dict(size=20, family="sans-serif")
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis_title=None,
    yaxis_title=None,
    showlegend=False
)

fig.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌍 Data Science Workforce Landscape – Chart Narrator Overview
# MAGIC
# MAGIC This dashboard paints a vivid picture of the **current data science workforce**, showcasing trends in company scale, job roles, geographic distribution, and historical growth.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏢 Company Size Distribution
# MAGIC An overwhelming **84% of data science jobs originate from medium-sized companies**, suggesting these organizations are the **primary employers** of data professionals. 
# MAGIC - **Large companies** account for 12.1%
# MAGIC - **Small businesses** contribute a modest 3.9%
# MAGIC This hints at a **sweet spot** in mid-tier firms embracing scalable AI initiatives.
# MAGIC
# MAGIC ---
# MAGIC ### 🌎 Top 10 Employee Residences
# MAGIC The United States is a clear leader with over **3,000 employees**, dwarfing other regions. 
# MAGIC Following at a distance:
# MAGIC - **GB**: 167
# MAGIC - **CA**, **ES**, and **IN** each range between 70–85
# MAGIC This indicates a **heavy US-centric workforce**, but a **growing global participation** is evident.
# MAGIC
# MAGIC ---
# MAGIC ### 📈 Job Record Distribution by Work Year
# MAGIC There’s a **remarkable upward trajectory** in job counts, from **just 76 in 2020** to **1,785 in 2023**. 
# MAGIC - The spike in 2022–2023 aligns with the **post-pandemic tech hiring surge** and growing business digitization.
# MAGIC - It suggests growing maturity and increased trust in data teams across organizations.
# MAGIC
# MAGIC ---
# MAGIC ### 📌 Top 10 Job Designations
# MAGIC Unsurprisingly, **Data Engineer**, **Data Scientist**, and **Data Analyst** top the list, accounting for the lion’s share of roles.
# MAGIC Notably:
# MAGIC - **Machine Learning Engineers** and **Analytics Engineers** show rising momentum
# MAGIC - Specialized roles like **Applied Scientist** and **Data Science Manager** have smaller, more elite counts
# MAGIC
# MAGIC ---
# MAGIC ### 🏆 Top 5 Job Titles by Experience Level
# MAGIC Breaking down popular titles by experience reveals:
# MAGIC - **Senior-level roles** dominate, especially for **Data Engineers (718)** and **Data Scientists (608)**
# MAGIC - **Entry-level** distribution is evenly spread but notably smaller, highlighting a preference for **experienced hires**
# MAGIC - Executive-level positions remain scarce, implying **tight leadership bandwidth**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC ### 📌 Summary
# MAGIC This dashboard reflects a **robust, maturing data science workforce**, dominated by:
# MAGIC - **Medium-sized firms**
# MAGIC - **Senior-level practitioners**
# MAGIC - A **steady global expansion**
# MAGIC It emphasizes the **growing strategic importance** of data roles while identifying opportunities to support **early-career talent** and **expand internationally**.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Salary Insights

# COMMAND ----------

import pandas as pd
import plotly.graph_objects as go
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.pyplot as plt

# Run Spark SQL (inside your Spark environment)
df_exp = spark.sql("""
    SELECT 
        ExperienceLevelName,
        COUNT(*) AS JobCount,
        AVG(SalaryInUSD) AS AvgSalaryInUSD
    FROM ds_salaries_view
    GROUP BY ExperienceLevelName
    ORDER BY AvgSalaryInUSD DESC
""").toPandas()

# Sort by descending salary
df_exp = df_exp.sort_values(by="AvgSalaryInUSD", ascending=False).reset_index(drop=True)

# Create gradient color list
start_color = "#37bdbe"
end_color = "#a6beba"

cmap = LinearSegmentedColormap.from_list("exp_gradient", [start_color, end_color], N=len(df_exp))
norm = plt.Normalize(0, len(df_exp) - 1)
rgba_colors = [f'rgba({int(r*255)},{int(g*255)},{int(b*255)},{a:.2f})' for r, g, b, a in [cmap(norm(i)) for i in range(len(df_exp))]]

# Build vertical bar chart with custom color
fig = go.Figure(go.Bar(
    x=df_exp["ExperienceLevelName"],
    y=df_exp["AvgSalaryInUSD"],
    marker=dict(color=rgba_colors),
    text=[f"${v:,.0f}" for v in df_exp["AvgSalaryInUSD"]],
    textposition='outside'
))

# Apply consistent layout
fig.update_layout(
    title=dict(
        text="Average Salary by Experience Level",
        x=0,
        xanchor='left',
        y=0.95,
        font=dict(size=20, family="sans-serif")
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    showlegend=False,
    xaxis_title=None,
    yaxis_title=None,
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)'
)

fig.show()


# COMMAND ----------

import pandas as pd
import plotly.graph_objects as go
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

# Spark SQL query (run in your Spark environment)
df = spark.sql("""
    SELECT 
        JobTitle,
        COUNT(*) AS JobCount,
        AVG(SalaryInUSD) AS AvgSalaryInUSD
    FROM ds_salaries_view
    GROUP BY JobTitle
    ORDER BY AvgSalaryInUSD DESC
""").toPandas()

# Sort so gradient matches bar position
df = df.sort_values(by="AvgSalaryInUSD", ascending=True).reset_index(drop=True)

# Generate gradient colors
start_color = "#a6beba"
end_color = "#37bdbe"
cmap = LinearSegmentedColormap.from_list("custom_gradient", [start_color, end_color], N=len(df))
norm = plt.Normalize(0, len(df) - 1)
colors = [cmap(norm(i)) for i in range(len(df))]
rgba_colors = [f'rgba({int(r*255)},{int(g*255)},{int(b*255)},{a:.2f})' for r, g, b, a in colors]

# Build Plotly bar chart
fig = go.Figure(go.Bar(
    x=df["AvgSalaryInUSD"],
    y=df["JobTitle"],
    orientation='h',
    marker=dict(color=rgba_colors),
    text=[f"${v:,.0f}" for v in df["AvgSalaryInUSD"]],
    textposition='outside'
))

# Apply layout styling
fig.update_layout(
    title=dict(
        text="Average Salary by Job Title",
        x=0,
        xanchor='left',
        y=0.95,
        yanchor='top',
        font=dict(size=20, family="sans-serif")
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    showlegend=False,
    xaxis_title=None,
    yaxis_title=None,
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)'
)

fig.show()


# COMMAND ----------

import pandas as pd
import plotly.express as px

# Step 1: Load data from Spark
df = spark.sql("""
    SELECT ExperienceLevelName, SalaryInUSD
    FROM ds_salaries_view
    WHERE SalaryInUSD IS NOT NULL AND ExperienceLevelName IS NOT NULL
""").toPandas()

# Step 2: Define display order
experience_order = [
    "Entry-level/Junior", "Mid-level/Intermediate",
    "Senior-level/Expert", "Executive-level/Director"
]
df["ExperienceLevelName"] = pd.Categorical(df["ExperienceLevelName"], categories=experience_order, ordered=True)

# Step 3: Create box plot
fig = px.box(
    df,
    x="ExperienceLevelName",
    y="SalaryInUSD",
    color="ExperienceLevelName",
    points="all",  # show raw data points
    title="Salary Distribution by Experience Level",
    color_discrete_map={
        "Entry-level/Junior": "#bd6e9c",
        "Mid-level/Intermediate": "#49aeb0",
        "Senior-level/Expert": "#695da9",
        "Executive-level/Director": "#88673f"
    },
    labels={
        "ExperienceLevelName": "Experience Level",
        "SalaryInUSD": "Salary (USD)"
    }
    
)

# Step 4: Apply Chart 1 style

fig.add_trace(go.Scatter(
    x=mean_salary["ExperienceLevelName"],
    y=mean_salary["SalaryInUSD"],
    mode='markers+text',
    name="Mean Salary",
    marker=dict(color='black', size=8, symbol='x'),
    text=mean_salary["Label"],
    textposition="top center",
    showlegend=True
))

# Step 6: Apply Chart 1 styling
fig.update_layout(
    title=dict(
        text="Salary Distribution by Experience Level",
        x=0,
        xanchor='left',
        font=dict(size=20)
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    xaxis_title=None,
    yaxis_title=None,
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    legend_title="Legend"
)
fig.show()



# COMMAND ----------

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Step 1: Load data
df = spark.sql("""
    SELECT ExperienceLevelName, SalaryInUSD
    FROM ds_salaries_view
    WHERE ExperienceLevelName IS NOT NULL AND SalaryInUSD IS NOT NULL
""").toPandas()

# Step 2: Order experience levels
experience_order = [
    "Entry-level/Junior", "Mid-level/Intermediate",
    "Senior-level/Expert", "Executive-level/Director"
]
df["ExperienceLevelName"] = pd.Categorical(df["ExperienceLevelName"], categories=experience_order, ordered=True)

# Step 3: Calculate mean per level
mean_salary = df.groupby("ExperienceLevelName")["SalaryInUSD"].mean().reset_index()
mean_salary["Label"] = mean_salary["SalaryInUSD"].round(0).astype(int).astype(str)

# Step 4: Create box plot
fig = px.box(
    df,
    x="ExperienceLevelName",
    y="SalaryInUSD",
    points="all",
    color="ExperienceLevelName",
    title="Salary Distribution by Experience Level",
    labels={"ExperienceLevelName": "Experience Level", "SalaryInUSD": "Salary (USD)"},
    color_discrete_map={
        "Entry-level/Junior": "#4285f4",
        "Mid-level/Intermediate": "#34a853",
        "Senior-level/Expert": "#673ab7",
        "Executive-level/Director": "#ea4335"
    }
)

# Step 5: Add mean points
fig.add_trace(go.Scatter(
    x=mean_salary["ExperienceLevelName"],
    y=mean_salary["SalaryInUSD"],
    mode='markers+text',
    name="Mean Salary",
    marker=dict(color='black', size=8, symbol='x'),
    text=mean_salary["Label"],
    textposition="top center",
    showlegend=True
))

# Step 6: Apply Chart 1 styling
fig.update_layout(
    title=dict(
        text="Salary Distribution by Experience Level",
        x=0,
        xanchor='left',
        font=dict(size=20)
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    xaxis_title=None,
    yaxis_title=None,
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    legend_title="Legend"
)
fig1 = px.violin(
    df,
    x="ExperienceLevelName",
    y="SalaryInUSD",
    box=True,  # Show mini box inside
    points="all",
    color="ExperienceLevelName",
    title="Salary Density by Experience Level",
    color_discrete_map={
        "Entry-level/Junior": "#bd6e9c",
        "Mid-level/Intermediate": "#49aeb0",
        "Senior-level/Expert": "#695da9",
        "Executive-level/Director": "#88673f"
    }
)
avg_df = df.groupby("ExperienceLevelName")["SalaryInUSD"].mean().reset_index()


fig.show()
fig1.show()


["#bd6e9c", "#49aeb0", "#695da9", "#88673f"]


# COMMAND ----------

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Load from Spark
df = spark.sql("""
    SELECT WorkYear, SalaryInUSD
    FROM ds_salaries_view
    WHERE WorkYear IS NOT NULL AND SalaryInUSD IS NOT NULL
""").toPandas()

# Convert WorkYear to string (for X-axis labels)
df["WorkYear"] = df["WorkYear"].astype(str)

# Calculate mean salary per year
mean_salary = df.groupby("WorkYear")["SalaryInUSD"].mean().reset_index()
mean_salary["Label"] = mean_salary["SalaryInUSD"].round(0).astype(int).astype(str)

# Create box plot
fig = px.box(
    df,
    x="WorkYear",
    y="SalaryInUSD",
    points="all",
    color="WorkYear",
    title="Salary Distribution by Work Year",
    labels={"WorkYear": "Work Year", "SalaryInUSD": "Salary (USD)"},
    color_discrete_sequence=px.colors.sequential.Teal
)

# Add mean value markers
fig.add_trace(go.Scatter(
    x=mean_salary["WorkYear"],
    y=mean_salary["SalaryInUSD"],
    mode='markers+text',
    name="Mean Salary",
    marker=dict(color='black', size=8, symbol='x'),
    text=mean_salary["Label"],
    textposition="top center",
    showlegend=True
))

# Apply Chart 1 style
fig.update_layout(
    title=dict(
        text="Salary Distribution by Work Year",
        x=0,
        xanchor='left',
        font=dict(size=20)
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    xaxis_title=None,
    yaxis_title=None,
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    legend_title="Legend"
)

fig.show()


# COMMAND ----------

import pandas as pd
import plotly.figure_factory as ff

# Step 1: Fetch salary data
df = spark.sql("SELECT SalaryInUSD FROM ds_salaries_view WHERE SalaryInUSD IS NOT NULL").toPandas()

# Step 2: Plot distribution with KDE
fig = ff.create_distplot(
    hist_data=[df["SalaryInUSD"]],
    group_labels=["Salary in USD"],
    bin_size=10000,
    curve_type='kde',
    show_rug=False,
    colors=["#49aeb0"]
)

# Step 3: Chart 1 styling
fig.update_layout(
    title=dict(
        text="Distribution of Salary in USD",
        x=0,
        xanchor='left',
        font=dict(size=20, family="sans-serif")
    ),
    xaxis_title="Salary (USD)",
    yaxis_title="Density",
    margin=dict(l=10, r=10, t=40, b=10),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    showlegend=False
)

fig.show()


# COMMAND ----------

import pandas as pd
import plotly.express as px

# Step 1: Get top 25 max salaries from SQL
df = spark.sql("""
    SELECT JobTitle, MAX(SalaryInUSD) AS MaxSalary
    FROM ds_salaries_view
    WHERE SalaryInUSD IS NOT NULL AND JobTitle IS NOT NULL
    GROUP BY JobTitle
    ORDER BY MaxSalary DESC
    LIMIT 25
""").toPandas()

# Step 2: Sort for better category placement
df = df.sort_values("MaxSalary", ascending=False)

# Step 3: Create vertical bar chart with color gradient
fig = px.bar(
    df,
    x="JobTitle",
    y="MaxSalary",
    text="MaxSalary",
    color="MaxSalary",
    title="Top 25 Highest Salary by Designation",
    labels={"JobTitle": "Job Designation", "MaxSalary": "Salaries"},
    color_continuous_scale="plasma"  # or try: 'Viridis', 'Turbo', 'Inferno'
)

# Step 4: Update visual styling
fig.update_traces(
    texttemplate='%{text:,.0f}',
    textposition='outside'
)

fig.update_layout(
    title=dict(
        text="Top 25 Highest Salary by Designation",
        x=0,
        xanchor="left",
        font=dict(size=20)
    ),
    xaxis_title=None,
    yaxis_title="Salaries",
    xaxis_tickangle=-45,
    margin=dict(l=10, r=10, t=60, b=100),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)'
)

fig.show()


# COMMAND ----------

import plotly.express as px
df = spark.sql("""
    SELECT JobTitle, SalaryInUSD
    FROM ds_salaries_view
    WHERE SalaryInUSD IS NOT NULL AND JobTitle IS NOT NULL
    ORDER BY SalaryInUSD DESC
    LIMIT 25

""").toPandas()

# Sort for cleaner display
df = df.sort_values("SalaryInUSD", ascending=False)
df["JobLabel"] = df["JobTitle"] + " #" + (df.groupby("JobTitle").cumcount() + 1).astype(str)

fig = px.bar(
    df.sort_values("SalaryInUSD"),  # sort for horizontal bar order
    y="JobTitle",
    x="SalaryInUSD",
    orientation="h",
    text="SalaryInUSD",
    color="SalaryInUSD",
    color_continuous_scale="plasma_r",
    title="Top 25 Highest Individual Salaries by Designation",
    labels={"JobTitle": "Job Designation", "SalaryInUSD": "Salary (USD)"}
)



fig.update_traces(
    texttemplate='$%{text:,.0f}',
    textposition='outside'
)

fig.update_layout(
    height=600,
    title=dict(
        text="Top 25 Highest Individual Salaries by Designation",
        x=0,
        xanchor="left",
        font=dict(size=20)
    ),
    margin=dict(l=10, r=10, t=50, b=100),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis_tickangle=-45,
    xaxis_title=None,
    yaxis_title=None,
    coloraxis_colorbar=dict(
        title="Salary in USD",
        titlefont=dict(size=14),
        tickfont=dict(size=14)
    )
)

fig.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # 📊 Average Salary Insights: A Deep Dive Across Experience Levels
# MAGIC
# MAGIC ## 💼 Average Salary by Job Title
# MAGIC The leftmost horizontal bar chart provides a clear depiction of how different job titles stack up in terms of average salary. As expected, leadership and technical roles such as **Data Science Tech Lead** and **Principal Data Scientist** dominate the top, exceeding the $300k mark. On the other end of the spectrum, more junior technical roles like **BI Developer** and **Power BI Developer** show average salaries under $100k.
# MAGIC
# MAGIC 🔎 The steep drop from top-tier to mid-tier roles highlights how specialization and seniority significantly influence earning potential.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 Average Salary by Experience Level
# MAGIC The bar chart to the top right reveals a well-defined trend: salary increases with experience.
# MAGIC
# MAGIC | Experience Level           | Avg Salary (USD) |
# MAGIC |----------------------------|------------------|
# MAGIC | Executive-level/Director   | $194,931         |
# MAGIC | Senior-level/Expert        | $153,051         |
# MAGIC | Mid-level/Intermediate     | $104,526         |
# MAGIC | Entry-level/Junior         | $78,546          |
# MAGIC
# MAGIC 💡 Career advancement has direct monetary benefits.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌈 Top 25 Highest Individual Salaries by Designation
# MAGIC This colorful stacked bar chart highlights the top earners in the dataset. Multiple entries for roles like **Data Scientist** and **Data Engineer** show variations in compensation within the same title—an indication of how geography, company size, and domain expertise may create discrepancies.
# MAGIC
# MAGIC 🏆 Roles like **AI Scientist**, **Principal Data Scientist**, and **Applied Machine Learning Scientist** command the highest individual salaries, showcasing the value of cutting-edge skills.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📈 Salary Distribution in USD
# MAGIC The histogram displays the **salary density curve** across all roles and levels.
# MAGIC
# MAGIC - Most salaries lie in the range of **$100k–$150k**
# MAGIC - Long right tail extends past **$300k**, indicating high-end outliers (e.g., senior leadership, AI roles)
# MAGIC
# MAGIC 📊 The shape is slightly right-skewed, confirming that a small portion of data professionals earn exceptionally high pay.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🗓️ Salary Distribution by Work Year
# MAGIC Boxplots reveal an **upward salary trend over time**.
# MAGIC
# MAGIC | Year | Avg Salary (USD) |
# MAGIC |------|------------------|
# MAGIC | 2020 | $92,303          |
# MAGIC | 2021 | $94,087          |
# MAGIC | 2022 | $133,339         |
# MAGIC | 2023 | $149,046         |
# MAGIC
# MAGIC 🚀 The growth reflects increased demand for data professionals, digital transformation post-COVID, and inflation adjustments.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎓 Salary Distribution by Experience Level
# MAGIC This chart confirms the earlier salary breakdown, adding nuance with full salary ranges.
# MAGIC
# MAGIC - **Executive-level/Director**: Wide spread, from ~$100k to $450k+
# MAGIC - **Entry-level/Junior**: Tight range, indicating standardized compensation
# MAGIC
# MAGIC 📌 The spread reflects how negotiation power and role responsibility expand with experience.
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------


import pandas as pd
import plotly.express as px
df = spark.sql("""
    SELECT jobTitle, ExperienceLevelName, COUNT(*) AS JobCount
    FROM ds_salaries_view
    GROUP BY jobTitle, ExperienceLevelName
""").toPandas()

fig = px.treemap(df, 
                 path=['ExperienceLevelName', 'jobTitle'], 
                 values='JobCount', 
                 title='Job Distribution by Experience Level')

fig.show()



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  ExperienceLevelName , jobTitle, COUNT(*) AS JobCount
# MAGIC     FROM ds_salaries_view
# MAGIC     GROUP BY jobTitle, ExperienceLevelName

# COMMAND ----------

import pandas as pd
import plotly.express as px
import numpy as np
import matplotlib.colors as mcolors

# Load Spark data
df = spark.sql("""
    SELECT jobTitle, ExperienceLevelName, COUNT(*) AS JobCount
    FROM ds_salaries_view
    GROUP BY jobTitle, ExperienceLevelName
""").toPandas()

# Base colors for each experience level
base_colors = {
    "Executive-level/Director": "#bd6e9c",
    "Senior-level/Expert": "#49aeb0",
    "Mid-level/Intermediate": "#695da9",
    "Entry-level/Junior": "#88673f"
}

# Fill in any missing levels
for level in df["ExperienceLevelName"].unique():
    if level not in base_colors:
        base_colors[level] = "#999999"

# Gradient function
def get_gradient_color(row, grouped_df):
    base_rgb = np.array(mcolors.to_rgb(base_colors[row["ExperienceLevelName"]]))
    sub_df = grouped_df.get_group(row["ExperienceLevelName"])
    
    min_val = sub_df["JobCount"].min()
    max_val = sub_df["JobCount"].max()
    norm = (row["JobCount"] - min_val) / (max_val - min_val + 1e-6)

    factor = 0.4 + 0.6 * norm
    white = np.array([1, 1, 1])
    shaded_rgb = base_rgb * factor + white * (1 - factor)
    return mcolors.to_hex(np.clip(shaded_rgb, 0, 1))

# Apply gradient shading
grouped = df.groupby("ExperienceLevelName")
df["ShadedColor"] = df.apply(lambda row: get_gradient_color(row, grouped), axis=1)

# Build label for color mapping
df["Label"] = df["ExperienceLevelName"] + " - " + df["jobTitle"]
color_map = dict(zip(df["Label"], df["ShadedColor"]))

# Create treemap chart
fig = px.treemap(
    df,
    path=['ExperienceLevelName', 'jobTitle'],
    values='JobCount',
    color='Label',
    color_discrete_map=color_map,
    title="Job Distribution by Experience Level"
)

# Update layout to match Chart 1
fig.update_layout(
    title=dict(
        text="Job Distribution by Experience Level",
        x=0,
        xanchor='left',
        y=0.95,
        font=dict(size=20, family="sans-serif")
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    showlegend=False
)

fig.show()


# COMMAND ----------

import pandas as pd
import plotly.express as px

# Step 1: Fetch data
df = spark.sql("""
    SELECT ExperienceLevelName, EmploymentTypeName
    FROM ds_salaries_view
""").toPandas()

# Step 2: Group and count
df_grouped = df.groupby(["ExperienceLevelName", "EmploymentTypeName"]).size().reset_index(name="Count")

# Step 3: Clean sorting (optional)
experience_order = [
    "Entry-level/Junior", "Mid-level/Intermediate",
    "Senior-level/Expert", "Executive-level/Director"
]
df_grouped["ExperienceLevelName"] = pd.Categorical(df_grouped["ExperienceLevelName"], categories=experience_order, ordered=True)
df_grouped = df_grouped.sort_values("ExperienceLevelName")

# Step 4: Plot grouped bar chart
fig = px.bar(
    df_grouped,
    x="ExperienceLevelName",
    y="Count",
    color="EmploymentTypeName",
    text="Count",
    barmode="group",
    title="Employment Type vs. Experience Level",
    color_discrete_map={
        "Full-time": "#49aeb0",
        "Part-time": "#7fd3d4",
        "Contract": "#695da9",
        "Freelance": "#88673f"
    }
)

# Step 5: Apply Chart 1 layout
fig.update_traces(
    texttemplate='%{text:,}',
    textposition='outside'
)

fig.update_layout(
    title=dict(
        text="Employment Type vs. Experience Level",
        x=0,
        xanchor='left',
        y=0.95,
        font=dict(size=20, family="sans-serif")
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis_title=None,
    yaxis_title=None,
    legend_title="Employment Type"
)

fig.show()


# COMMAND ----------

import pandas as pd
import plotly.graph_objects as go

# Step 1: Load and clean
df = spark.sql("""
    SELECT CompanySize, ExperienceLevelName
    FROM ds_salaries_view
    WHERE CompanySize IS NOT NULL AND CompanySize != ''
      AND ExperienceLevelName IS NOT NULL AND ExperienceLevelName != ''
""").toPandas()

df["CompanySize"] = df["CompanySize"].str.strip()
df["ExperienceLevelName"] = df["ExperienceLevelName"].str.strip()

# Step 2: Define order
company_order = ["S", "M", "L"]
experience_order = [
    "Entry-level/Junior", "Mid-level/Intermediate",
    "Senior-level/Expert", "Executive-level/Director"
]

df["CompanySize"] = pd.Categorical(df["CompanySize"], categories=company_order, ordered=True)
df["ExperienceLevelName"] = pd.Categorical(df["ExperienceLevelName"], categories=experience_order, ordered=True)

# Step 3: Group and calculate % within company size
grouped = (
    df.groupby(["CompanySize", "ExperienceLevelName"])
    .size()
    .reset_index(name="JobCount")
)

grouped["TotalPerSize"] = grouped.groupby("CompanySize")["JobCount"].transform("sum")
grouped["Percent"] = grouped["JobCount"] / grouped["TotalPerSize"] * 100

# Step 4: Pivot for grouped bars
pivot_percent = grouped.pivot(index="CompanySize", columns="ExperienceLevelName", values="Percent").fillna(0).reindex(company_order)
pivot_count = grouped.pivot(index="CompanySize", columns="ExperienceLevelName", values="JobCount").fillna(0).reindex(company_order)

# Step 5: Plot side-by-side bars
colors = {
    "Entry-level/Junior": "#4285f4",
    "Mid-level/Intermediate": "#34a853",
    "Senior-level/Expert": "#673ab7",
    "Executive-level/Director": "#ea4335"
}

fig = go.Figure()

for exp in experience_order:
    y_percent = pivot_percent[exp]
    y_count = pivot_count[exp]
    labels = [f"{p:.2f}% ({int(c)})" for p, c in zip(y_percent, y_count)]
    
    fig.add_trace(go.Bar(
        name=exp,
        x=company_order,
        y=y_percent,
        text=labels,
        textposition='outside',
        marker_color=colors[exp],
        hovertemplate=f"<b>{exp}</b><br>%{{x}}: %{{text}}<extra></extra>"
    ))

# Step 6: Final layout
fig.update_layout(
    barmode='group',  # <-- this makes it separate bars, not stacked
    title=dict(
        text="Experience Level % Distribution by Company Size (Grouped Bars)",
        x=0,
        xanchor='left',
        font=dict(size=20)
    ),
    yaxis=dict(title="Percentage", tickformat=".0%"),
    xaxis=dict(title="Company Size"),
    margin=dict(l=10, r=10, t=40, b=10),
    legend_title="Experience Level",
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)'
)

fig.show()


# COMMAND ----------

import pandas as pd
import plotly.express as px

# Step 1: Fetch data from Spark
df = spark.sql("""
    SELECT ExperienceLevelName, EmploymentTypeName
    FROM ds_salaries_view
    WHERE ExperienceLevelName IS NOT NULL AND EmploymentTypeName IS NOT NULL
""").toPandas()

# Step 2: Set category order
experience_order = [
    "Entry-level/Junior", "Mid-level/Intermediate",
    "Senior-level/Expert", "Executive-level/Director"
]
df["ExperienceLevelName"] = pd.Categorical(df["ExperienceLevelName"], categories=experience_order, ordered=True)

# Step 3: Group and count combinations
grouped = df.groupby(["ExperienceLevelName", "EmploymentTypeName"]).size().reset_index(name="JobCount")

# Step 4: Create grouped bar chart
fig = px.bar(
    grouped,
    x="ExperienceLevelName",
    y="JobCount",
    color="EmploymentTypeName",
    text="JobCount",
    barmode="group",
    title="Employment Type by Experience Level",
    color_discrete_map={
        "Full-time": "#49aeb0",
        "Part-time": "#7fd3d4",
        "Contract": "#695da9",
        "Freelance": "#88673f"
    }
)

# Step 5: Chart 1 styling
fig.update_traces(
    texttemplate='%{text:,}',
    textposition='outside'
)

fig.update_layout(
    title=dict(
        text="Employment Type by Experience Level",
        x=0,
        xanchor='left',
        font=dict(size=20, family="sans-serif")
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis_title=None,
    yaxis_title=None,
    legend_title="Employment Type"
)

fig.show()


# COMMAND ----------

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from matplotlib.colors import LinearSegmentedColormap, to_hex

# Fetch data from Spark
df = spark.sql("""
    SELECT ExperienceLevelName, EmploymentTypeName, COUNT(*) AS JobCount 
    FROM ds_salaries_view
    GROUP BY ExperienceLevelName, EmploymentTypeName
""").toPandas()

# Pivot data into grid format for heatmap
pivot = df.pivot(index="EmploymentTypeName", columns="ExperienceLevelName", values="JobCount").fillna(0)
x = list(pivot.columns)
y = list(pivot.index)
z = pivot.values

# Generate dynamic green color gradient
start_color = "#e0f7e9"
end_color = "#1c9e63"
cmap = LinearSegmentedColormap.from_list("green_gradient", [start_color, end_color])
n_steps = 20
green_gradient = [
    [i / (n_steps - 1), to_hex(cmap(i / (n_steps - 1)))] for i in range(n_steps)
]

# Create heatmap with values displayed
fig = go.Figure(data=go.Heatmap(
    z=z,
    x=x,
    y=y,
    text=z,
    texttemplate="%{text}",  # Show JobCount
    textfont={"size": 12},
    colorscale=green_gradient,
    colorbar=dict(
        title="Job Count",
        titlefont=dict(size=14),
        tickfont=dict(size=12)
    )
))

# Apply consistent layout (Chart 1 style)
fig.update_layout(
    title=dict(
        text="How Jobs Are Distributed Across Experience Levels and Contracts",
        x=0,
        xanchor='left',
        y=0.95,
        font=dict(size=20, family="sans-serif")
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis_title=None,
    yaxis_title=None
)

fig.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # 📊 Experience, Employment Type, and Company Insights
# MAGIC
# MAGIC Let’s dive into how experience levels, job types, and company characteristics intertwine in today’s data job landscape.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. **Experience Level % Distribution by Company Size**
# MAGIC
# MAGIC In this grouped bar chart, we see a striking pattern:
# MAGIC
# MAGIC - **Medium-sized companies dominate** the landscape with a **surge in Senior-level/Expert roles**, taking up over 70% of their workforce share.
# MAGIC - Entry-level and mid-level roles are more evenly distributed across all company sizes.
# MAGIC - **Executive-level roles remain rare**, especially in smaller companies.
# MAGIC
# MAGIC > 🔍 Medium-sized firms seem to be the backbone of experienced data professionals, perhaps due to their scale — large enough to demand expertise, but still agile in hiring.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. **How Jobs Are Distributed Across Experience Levels and Contracts**
# MAGIC
# MAGIC This heatmap reveals a **strong preference for full-time employment** across all levels:
# MAGIC
# MAGIC - **Senior Experts** lead with over **2,500 full-time positions**, dwarfing other categories.
# MAGIC - Contract, freelance, and part-time roles are **marginal**, showing full-time roles still dominate.
# MAGIC
# MAGIC > 🔍Despite the gig economy buzz, data roles — especially experienced ones — still prefer the stability of full-time positions.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. **Employment Type by Experience Level**
# MAGIC
# MAGIC This bar chart supports the heatmap findings:
# MAGIC
# MAGIC - **Full-time work** is the default employment type across all experience levels.
# MAGIC - Entry-level roles include **some part-time opportunities** — likely internships or early roles.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. **Distribution of Job Roles Across Experience Levels (Treemap)**
# MAGIC
# MAGIC This treemap zooms into **which job titles dominate each experience bracket**:
# MAGIC
# MAGIC - **Senior-level Experts:** Mostly **Data Engineers**, **Data Scientists**, and **Data Analysts**.
# MAGIC - **Mid-level roles:** Still concentrate on the same trio.
# MAGIC - **Entry-level positions:** Slightly more varied, but still revolve around these key roles.
# MAGIC - **Executive-level Directors:** Scarce — only a handful of strategic leadership roles.
# MAGIC
# MAGIC > 🔍 Job functions don’t drastically change — but their **volume and seniority shift**. Senior experts command the stage with deep technical responsibilities.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧭 Summary Narrative
# MAGIC
# MAGIC The data reveals a clear hierarchy:
# MAGIC
# MAGIC - **Full-time roles and medium-sized companies** are the powerhouses of the data economy.
# MAGIC - **Senior-level experts dominate**, reflecting the demand for skill maturity.
# MAGIC - **Most job roles**, regardless of level, cluster around core titles like Data Engineer, Data Scientist, and Data Analyst.
# MAGIC
# MAGIC > 💡  If you're building workforce strategy, prioritize mid-sized environments, invest in expert development, and define clear growth paths — that’s where the **volume and value converge**.
# MAGIC

# COMMAND ----------

import plotly.express as px
import pandas as pd

df = spark.sql("""
    SELECT SalaryInUSD, RemoteRatio, JobTitle,ExperienceLevelName FROM ds_salaries_view
""").toPandas()
#df = df[df["SalaryInUSD"] < 300000]
fig = px.scatter(df, x='RemoteRatio', y='SalaryInUSD', 
                 title="Salary vs. Remote Work",
                 labels={'RemoteRatio': 'Remote Work Ratio (%)', 'SalaryInUSD': 'Salary in USD'},
                 hover_data=["JobTitle", "ExperienceLevelName"],
                 trendline="ols")

fig.show()


# COMMAND ----------

# Step 1: Fetch numeric-ready features from Spark SQL
df = spark.sql("""
    SELECT 
        SalaryInUSD, 
        RemoteRatio,
        CASE ExperienceLevelName
            WHEN 'Entry-level/Junior' THEN 1
            WHEN 'Mid-level/Intermediate' THEN 2
            WHEN 'Senior-level/Expert' THEN 3
            WHEN 'Executive-level/Director' THEN 4
        END AS ExperienceLevel,
        CASE CompanySize
            WHEN 'S' THEN 1
            WHEN 'M' THEN 2
            WHEN 'L' THEN 3
        END AS CompanySize
    FROM ds_salaries_view
    WHERE SalaryInUSD IS NOT NULL
      AND RemoteRatio IS NOT NULL
      AND ExperienceLevelName IS NOT NULL
      AND CompanySize IS NOT NULL
""").toPandas()
# Step 2: Normalize and apply KMeans clustering
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import plotly.express as px

features = ["SalaryInUSD", "RemoteRatio", "ExperienceLevel", "CompanySize"]
X_scaled = StandardScaler().fit_transform(df[features])

kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
df["Cluster"] = kmeans.fit_predict(X_scaled)

# Step 3: Plot clusters
fig = px.scatter(
    df,
    x="RemoteRatio",
    y="SalaryInUSD",
    color=df["Cluster"].astype(str),
    title="Cluster Analysis of Job Profiles (KMeans)",
    labels={"color": "Cluster"},
    hover_data=["ExperienceLevel", "CompanySize", "SalaryInUSD"]
)
fig.show()



# COMMAND ----------

import pandas as pd
import plotly.express as px

# Step 1: Fetch and clean data
df = spark.sql("""
    SELECT CompanySize, RemoteRatio
    FROM ds_salaries_view
    WHERE CompanySize IS NOT NULL AND RemoteRatio IS NOT NULL
""").toPandas()

# Step 2: Normalize company size labels
df["CompanySize"] = df["CompanySize"].str.strip().str.upper()
size_map = {'S': 'Small', 'M': 'Medium', 'L': 'Large'}
df["CompanySize"] = df["CompanySize"].map(size_map)
df = df.dropna(subset=["CompanySize"])

# Step 3: Map remote type
remote_map = {0: "No Remote", 50: "Partially Remote", 100: "Fully Remote"}
df["RemoteType"] = df["RemoteRatio"].map(remote_map)

# Step 4: Group by type and size
grouped = df.groupby(["CompanySize", "RemoteType"]).size().reset_index(name="JobCount")

# Step 5: Ensure category order
grouped["CompanySize"] = pd.Categorical(grouped["CompanySize"], categories=["Small", "Medium", "Large"], ordered=True)
grouped["RemoteType"] = pd.Categorical(grouped["RemoteType"], categories=["No Remote", "Partially Remote", "Fully Remote"], ordered=True)
grouped = grouped.sort_values(["CompanySize", "RemoteType"])

# Step 6: Build bar chart (with fixed color per remote type)




fig = px.bar(
    grouped,
    y="CompanySize",        # y becomes the category
    x="JobCount",           # x is the value now
    orientation="h",        # set horizontal
    color="RemoteType",
    text="JobCount",
    barmode="stack",
    title="Company Size vs. Remote Work Type (Job Count)",
    color_discrete_map={
        "No Remote": "#4285f4",          # light mint
        "Partially Remote": "#34a853",   # mid green
        "Fully Remote": "#673ab7"        # brand teal
    }
)



# Step 7: Apply dashboard styling
fig.update_traces(
    textposition="inside",
    texttemplate="%{text:,}"
)
fig.update_layout(
    autosize=True,             # Let container control height
    bargap=0.15,
    bargroupgap=0.05,
    title=dict(
        text="Company Size vs. Remote Work Type (Job Count)",
        x=0,
        xanchor='left',
        font=dict(size=20, family="sans-serif")
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    xaxis_title=None,
    yaxis_title=None,
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    legend_title="Remote Type"
)




fig.show()


# COMMAND ----------

import numpy as np
import pandas as pd
import plotly.graph_objects as go

# Step 1: Query and group
df = spark.sql("SELECT WorkYear, RemoteRatio FROM ds_salaries_view").toPandas()
remote_year = df.groupby(['WorkYear', 'RemoteRatio']).size()

# Step 2: Normalize remote ratio per year
def get_ratio(year):
    values = remote_year[year]
    return np.round(values / values.sum(), 2)

ratios = {
    "2020": get_ratio(2020),
    "2021": get_ratio(2021),
    "2022": get_ratio(2022),
    "2023": get_ratio(2023)
}


# Step 3: Remote type categories
categories = ['No Remote Work', 'Partially Remote', 'Fully Remote']

# Step 4: Styled color palette by year
colors = {
    "2020": "#4285f4",  
    "2021": "#34a853", 
    "2022": "#673ab7", 
    "2023": "#ea4335"   
}

# Step 5: Build radar chart
fig = go.Figure()

for year in ["2020", "2021", "2022", "2023"]:
    fig.add_trace(go.Scatterpolar(
        r=ratios[year],
        theta=categories,
        fill='toself',
        name=year,
        marker_color=colors[year],
        line_color=colors[year],
        hovertemplate=f"<b>{year}</b><br>%{{theta}}: %{{r:.0%}}<extra></extra>"
    ))

# Step 6: Chart styling
fig.update_layout(
    title=dict(
        text="Remote Work Ratio by Year (2020–2023)",
        x=0,
        xanchor='left',
        font=dict(size=20, family="sans-serif")
    ),
    polar=dict(
        radialaxis=dict(visible=True, range=[0, 1], tickformat=".0%"),
        angularaxis=dict(direction="clockwise")
    ),
    legend_title_text="Year",
    margin=dict(l=10, r=10, t=40, b=10),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)'
)

fig.show()


# COMMAND ----------

import pandas as pd
import plotly.express as px
import matplotlib.colors as mcolors
import numpy as np  # ← REQUIRED for array operations

# Step 1: Get remote work categories
df = spark.sql("SELECT RemoteRatio FROM ds_salaries_view").toPandas()
remote_map = {100: "Fully Remote", 50: "Partially Remote", 0: "No Remote Work"}
df["RemoteType"] = df["RemoteRatio"].map(remote_map)

# Step 2: Count each type
remote_counts = df["RemoteType"].value_counts().reindex(["Fully Remote", "Partially Remote", "No Remote Work"])
pie_df = pd.DataFrame({
    "RemoteType": remote_counts.index,
    "Count": remote_counts.values
})

# Step 3: Create gradient color
base_rgb = np.array(mcolors.to_rgb("#49aeb0"))  # Convert to np.array
white_rgb = np.array([1, 1, 1])

# Normalize
norm = (pie_df["Count"] - pie_df["Count"].min()) / (pie_df["Count"].max() - pie_df["Count"].min() + 1e-6)

# Generate hex color gradient
# Blend base green with white (ensure minimum visibility)
def get_shade(alpha):
    blended = base_rgb * alpha + white_rgb * (1 - alpha)
    return mcolors.to_hex(np.clip(blended, 0, 1))

pie_df["Color"] = norm.apply(lambda alpha: get_shade(__builtins__.max(0.3, alpha)))



color_map = dict(zip(pie_df["RemoteType"], pie_df["Color"]))

# Step 4: Create pie chart
fig = px.pie(
    pie_df,
    names="RemoteType",
    values="Count",
    color="RemoteType",
    color_discrete_map=color_map,
    title="Remote Work Type Distribution",
    hole=0.3
)

# Step 5: Style
fig.update_traces(
    textinfo='percent+label',
    textfont_size=14,
    hovertemplate='%{label}<br>Count: %{value:,}<extra></extra>'
)

fig.update_layout(
    title=dict(
        text="Remote Work Type Distribution",
        x=0,
        xanchor='left',
        y=0.95,
        font=dict(size=20, family="sans-serif")
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    showlegend=False
)

fig.show()


# COMMAND ----------

import plotly.express as px

# Run Spark SQL to get counts by RemoteRatio and ExperienceLevel
df_heatmap = spark.sql("""
    SELECT 
        RemoteRatio, 
        ExperienceLevelName, 
        COUNT(*) AS Count
    FROM ds_salaries_view
    GROUP BY RemoteRatio, ExperienceLevelName
""").toPandas()

# Define custom teal color scale based on #49aeb0
color_scale = [
    [0.0, "#e0f7f7"],
    [0.5, "#7fd3d4"],
    [1.0, "#49aeb0"]
]

# Create heatmap
fig = px.density_heatmap(
    df_heatmap,
    x="ExperienceLevelName",
    y="RemoteRatio",
    z="Count",
    color_continuous_scale=color_scale,
    title="Remote Ratio vs. Experience Level"
)

# Layout styling
fig.update_layout(
    title=dict(
        text="Remote Ratio vs. Experience Level",
        x=0,
        xanchor='left',
        y=0.95,
        font=dict(size=20, family="sans-serif")
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
     xaxis_title=None,  # 👈 hide x-axis title
    yaxis_title=None,   # 👈 hide y-axis title
     coloraxis_colorbar=dict(
        title="Job Count",        # 👈 custom legend title
        titlefont=dict(size=14),  # optional: font styling
        tickfont=dict(size=12)
    )
)

fig.show()


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 Remote Work Insights – Chart Narrator View
# MAGIC
# MAGIC ### 📌 1. Remote Work Type Distribution
# MAGIC - The majority of roles (**51.2%**) are classified as **No Remote Work**, suggesting a post-COVID shift back to office-centric models.
# MAGIC - **Fully Remote** still holds a strong share (**43.8%**), reflecting how remote flexibility has become permanent for many.
# MAGIC - Interestingly, **Partially Remote (Hybrid)** roles remain low (~5%), indicating companies prefer either full on-site or full remote.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 2. Remote Ratio vs. Experience Level
# MAGIC - **Senior-level/Expert roles** dominate the fully remote category, implying trust and flexibility are granted more with experience.
# MAGIC - **Entry-level** and **Mid-level** positions skew toward on-site, likely due to training and collaboration needs.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 3. Company Size vs. Remote Work Type
# MAGIC - **Medium-sized companies** lead in job volume and have the highest number of **fully remote jobs**.
# MAGIC - **Large companies** tend to maintain more on-site roles, possibly due to operational structure.
# MAGIC - **Small companies** show a more balanced distribution across remote types.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 4. Salary vs. Remote Work
# MAGIC - No clear correlation exists between **remote ratio and salary**. High salaries appear across all remote types.
# MAGIC - This indicates **remote flexibility isn’t strictly tied to compensation**, but may reflect company policy or job nature.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 5. Remote Work Ratio Over Time (2020–2023)
# MAGIC - In **2020–2021**, **fully remote** roles spiked, driven by the pandemic.
# MAGIC - From **2022–2023**, we see a **decline in remote jobs**, with more on-site roles returning.
# MAGIC - Hybrid adoption has remained minimal throughout.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Overall Takeaway:
# MAGIC The data tells a story of remote work **evolving**, not disappearing:
# MAGIC - Full remote work is still prominent.
# MAGIC - Hybrid hasn’t exploded as expected.
# MAGIC - The return to the office is real but **not universal**, and flexibility often scales with seniority and company size.
# MAGIC

# COMMAND ----------

import pandas as pd
import plotly.express as px
import numpy as np
import matplotlib.colors as mcolors

# Load Spark data
df = spark.sql("""
    SELECT jobTitle, ExperienceLevelName, COUNT(*) AS JobCount
    FROM ds_salaries_view
    GROUP BY jobTitle, ExperienceLevelName
""").toPandas()

# Base colors for each experience level
base_colors = {
    "Executive-level/Director": "#bd6e9c",
    "Senior-level/Expert": "#49aeb0",
    "Mid-level/Intermediate": "#695da9",
    "Entry-level/Junior": "#88673f"
}

# Fill missing keys
for level in df["ExperienceLevelName"].unique():
    if level not in base_colors:
        base_colors[level] = "#999999"

# Gradient shading
def get_gradient_color(row, grouped_df):
    base_rgb = np.array(mcolors.to_rgb(base_colors[row["ExperienceLevelName"]]))
    sub_df = grouped_df.get_group(row["ExperienceLevelName"])
    
    min_val = sub_df["JobCount"].min()
    max_val = sub_df["JobCount"].max()
    norm = (row["JobCount"] - min_val) / (max_val - min_val + 1e-6)

    factor = 0.4 + 0.6 * norm
    white = np.array([1, 1, 1])
    shaded_rgb = base_rgb * factor + white * (1 - factor)
    return mcolors.to_hex(np.clip(shaded_rgb, 0, 1))

grouped = df.groupby("ExperienceLevelName")
df["ShadedColor"] = df.apply(lambda row: get_gradient_color(row, grouped), axis=1)

# Add label and value text
df["Label"] = df["ExperienceLevelName"] + " - " + df["jobTitle"]
df["Text"] = df["JobCount"].apply(lambda v: f"{v:,} jobs")
color_map = dict(zip(df["Label"], df["ShadedColor"]))

# Create treemap with text
fig = px.treemap(
    df,
    path=['ExperienceLevelName', 'jobTitle'],
    values='JobCount',
    color='Label',
    color_discrete_map=color_map,
    title="Treemap Job Titles theo Gradient màu gốc làm đậm",
    custom_data=['JobCount'],
    hover_data={"JobCount": True},
)

# Show text value (JobCount)
fig.update_traces(
    textinfo='label+value',
    texttemplate='%{label}<br>%{value:,}',
    textfont=dict(size=12)
)

# Apply chart 1 layout
fig.update_layout(
    title=dict(
        text="Distribution of Job Roles Across Experience Levels",
        x=0,
        xanchor='left',
        y=0.95,
        font=dict(size=20, family="sans-serif")
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    showlegend=False
)

fig.show()


# COMMAND ----------

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Query cleaned region-to-region data
df_sankey = spark.sql("""
    SELECT 
        EmployeeRegion,
        CompanyRegion,
        COUNT(*) AS Count
    FROM ds_salaries_view
    GROUP BY EmployeeRegion, CompanyRegion
""").toPandas()

# Remove self-flows
df_sankey = df_sankey[df_sankey["EmployeeRegion"] != df_sankey["CompanyRegion"]]

# Generate unique nodes
nodes = pd.unique(df_sankey[["EmployeeRegion", "CompanyRegion"]].values.ravel()).tolist()
node_map = {label: i for i, label in enumerate(nodes)}

# Map source/target ids
df_sankey["source_id"] = df_sankey["EmployeeRegion"].map(node_map)
df_sankey["target_id"] = df_sankey["CompanyRegion"].map(node_map)

# Format value for hover
df_sankey["hovertext"] = df_sankey.apply(
    lambda row: f"{row['EmployeeRegion']} → {row['CompanyRegion']}<br><b>Job Count:</b> {row['Count']:,}",
    axis=1
)

# Build Sankey chart
sankey = go.Sankey(
    arrangement="perpendicular",
    node=dict(
        pad=12,
        thickness=16,
        line=dict(color="black", width=0.3),
        label=nodes,
        color="#49aeb0",
        hovertemplate="%{label}<extra></extra>"
    ),
    link=dict(
        source=df_sankey["source_id"],
        target=df_sankey["target_id"],
        value=df_sankey["Count"],
        color="rgba(73, 174, 176, 0.4)",
        hovertemplate=df_sankey["hovertext"]
    )
)

# Build table (text column version)
table = go.Table(
    header=dict(
        values=["Employee Region", "Company Region", "Job Count"],
        fill_color='#49aeb0',
        font=dict(color='white', size=14),
        align="left"
    ),
    cells=dict(
        values=[
            df_sankey["EmployeeRegion"],
            df_sankey["CompanyRegion"],
            df_sankey["Count"].apply(lambda v: f"{v:,}")
        ],
        fill_color='rgba(224, 247, 233, 0.7)',
        font=dict(color='black', size=12),
        align="left"
    )
)

# Combine Sankey and Table in one canvas
fig = make_subplots(
    rows=1, cols=2,
    column_widths=[0.65, 0.35],
    specs=[[{"type": "sankey"}, {"type": "table"}]],
    horizontal_spacing=0.05
)

fig.add_trace(sankey, row=1, col=1)
fig.add_trace(table, row=1, col=2)

# Apply Chart 1 style
fig.update_layout(
    title=dict(
        text="Employee Region → Company Region",
        x=0,
        xanchor='left',
        y=0.95,
        font=dict(size=20, family="sans-serif")
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    width=1150,
    height=600,
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)'
)

# Display in Databricks
displayHTML(fig.to_html(include_plotlyjs='cdn'))


# COMMAND ----------

import pandas as pd
import plotly.express as px

# Step 1: Fetch counts from Spark
df_emp = spark.sql("""
    SELECT EmployeeResidence AS Location, COUNT(*) AS Count
    FROM ds_salaries_view
    GROUP BY EmployeeResidence

""").toPandas()

df_comp = spark.sql("""
    SELECT CompanyLocation AS Location, COUNT(*) AS Count
    FROM ds_salaries_view
    GROUP BY CompanyLocation
        limit 10
""").toPandas()

# Step 2: Label and combine
df_emp["Type"] = "Employee Residence"
df_comp["Type"] = "Company Location"
df_combined = pd.concat([df_emp, df_comp], ignore_index=True)

# Step 3: Top 15 Employee Residence locations
top_locations = df_emp.sort_values(by="Count", ascending=False).head(15)["Location"].tolist()
df_combined = df_combined[df_combined["Location"].isin(top_locations)]

# Step 4: Apply sorting order
df_combined["Location"] = pd.Categorical(df_combined["Location"], categories=top_locations[::-1], ordered=True)
df_combined = df_combined.sort_values(by="Location")

# Step 5: Plot horizontal grouped bar
fig = px.bar(
    df_combined,
    y="Location",
    x="Count",
    color="Type",
    text="Count",
    orientation='h',
    barmode="group",
    title="Comparison of Employee Residence vs. Company Location",
    color_discrete_map={
        "Employee Residence": "#49aeb0",
        "Company Location": "#ea4335"
    }
)

# Step 6: Apply Chart 1 layout
fig.update_traces(
    texttemplate='%{text:,}',
    textposition='outside'
)
bar_height = 40
n_rows = df_combined["Location"].nunique()
chart_height = n_rows * bar_height
chart_width = 900  # adjust if needed

fig.update_layout(
    autosize=True,
    title=dict(
        text="Comparison of Employee Residence vs. Company Location",
        x=0,
        xanchor='left',
        font=dict(size=20, family="sans-serif")
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis_title=None,
    yaxis_title=None,
    legend_title="Type"
)



fig.show()


# COMMAND ----------

import pandas as pd
import plotly.express as px
import pycountry

# Step 1: Query average salary by CompanyLocation (ISO-2 codes)
df = spark.sql("""
    SELECT CompanyLocation, AVG(SalaryInUSD) AS AvgSalary
    FROM ds_salaries_view
    WHERE SalaryInUSD IS NOT NULL AND CompanyLocation IS NOT NULL
    GROUP BY CompanyLocation
""").toPandas()

# Step 2: Convert ISO-2 to ISO-3 for plotly
def iso2_to_iso3(code):
    try:
        return pycountry.countries.get(alpha_2=code).alpha_3
    except:
        return None

df["CountryCode"] = df["CompanyLocation"].apply(iso2_to_iso3)
df = df.dropna(subset=["CountryCode"])

# Step 3: Create choropleth map
fig = px.choropleth(
    df,
    locations="CountryCode",
    locationmode="ISO-3",
    color="AvgSalary",
    hover_name="CompanyLocation",
    hover_data={"AvgSalary": ':.0f'},
    color_continuous_scale="plasma_r",
    title="Average Salary by Company Location"
)

# Step 4: Apply Chart 1 styling
fig.update_layout(
    title=dict(
        text="Average Salary by Company Location",
        x=0,
        xanchor="left",
        font=dict(size=20)
    ),
    margin=dict(l=10, r=10, t=50, b=10),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    coloraxis_colorbar=dict(
        title="Avg Salary (USD)",
        titlefont=dict(size=14),
        tickfont=dict(size=12)
    )
)

fig.show()


# COMMAND ----------

import pandas as pd
import plotly.express as px

# Step 1: Aggregate average salary by employee residence
df = spark.sql("""
    SELECT EmployeeResidence, AVG(SalaryInUSD) AS AvgSalary
    FROM ds_salaries_view
    WHERE SalaryInUSD IS NOT NULL AND EmployeeResidence IS NOT NULL
    GROUP BY EmployeeResidence
    limit 15
""").toPandas()

# Step 2: Sort by salary for better layout
df = df.sort_values(by="AvgSalary", ascending=True)

# Step 3: Plot horizontal bar chart
fig = px.bar(
    df,
    x="AvgSalary",
    y="EmployeeResidence",
    orientation="h",
    text="AvgSalary",
    color="AvgSalary",
    color_continuous_scale="plasma_r",
    title="Average Salary by Employee Residence",
    labels={"AvgSalary": "Average Salary (USD)", "EmployeeResidence": "Residence Country"}
)

# Step 4: Style
fig.update_traces(
    texttemplate='$%{text:,.0f}',
    textposition='outside'
)
bar_height = 40  # adjust for spacing
fig.update_layout(
    height=len(df) * bar_height,
    title=dict(
        text="Average Salary by Employee Residence",
        x=0,
        xanchor='left',
        font=dict(size=20)
    ),
    margin=dict(l=10, r=10, t=50, b=10),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis_title="Salary (USD)",
    yaxis_title=None,
    yaxis=dict(tickfont=dict(size=12)),
    xaxis=dict(tickfont=dict(size=12)),
    coloraxis_colorbar=dict(
        title="Avg Salary (USD)",
        titlefont=dict(size=14),
        tickfont=dict(size=12)
    )
)


fig.show()


# COMMAND ----------

import pandas as pd
import pycountry
import plotly.express as px

# Step 1: Load data
df = spark.sql("""
    SELECT CompanyLocation, ExperienceLevelName
    FROM ds_salaries_view
    WHERE CompanyLocation IS NOT NULL AND ExperienceLevelName IS NOT NULL
""").toPandas()

# Step 2: Convert to ISO-3
def iso2_to_iso3(code):
    try:
        return pycountry.countries.get(alpha_2=code).alpha_3
    except:
        return None

df["CountryCode"] = df["CompanyLocation"].apply(iso2_to_iso3)
df = df.dropna(subset=["CountryCode"])

# Step 3: Group and get top experience level per country
grouped = df.groupby(["CountryCode", "ExperienceLevelName"]).size().reset_index(name="JobCount")
top_levels = grouped.sort_values("JobCount", ascending=False).drop_duplicates("CountryCode")

# Step 4: Color map for levels
level_colors = {
    "Entry-level/Junior": "#4285f4",
    "Mid-level/Intermediate": "#34a853",
    "Senior-level/Expert": "#673ab7",
    "Executive-level/Director": "#ea4335"
}

# Step 5: Categorical choropleth
fig = px.choropleth(
    top_levels,
    locations="CountryCode",
    locationmode="ISO-3",
    color="ExperienceLevelName",
    hover_name="CountryCode",
    hover_data={"ExperienceLevelName": True, "JobCount": True},
    color_discrete_map=level_colors,
    title="Top Experience Level by Company Location"
)

# Step 6: Layout styling
fig.update_layout(
    title=dict(
        text="Top Experience Level by Company Location",
        x=0,
        xanchor='left',
        font=dict(size=20)
    ),
    margin=dict(l=10, r=10, t=40, b=10),
    paper_bgcolor='rgba(0,0,0,0)',
    geo=dict(showframe=False, showcoastlines=False),
    legend_title_text="Experience Level"
)

fig.show()


# COMMAND ----------

import pandas as pd
import pycountry
import plotly.express as px

# Load data from Spark
df = spark.sql("""
    SELECT CompanyLocation, ExperienceLevelName
    FROM ds_salaries_view
    WHERE CompanyLocation IS NOT NULL AND ExperienceLevelName IS NOT NULL
""").toPandas()

# Convert country codes to ISO-3
def iso2_to_iso3(code):
    try:
        return pycountry.countries.get(alpha_2=code).alpha_3
    except:
        return None

df["CountryCode"] = df["CompanyLocation"].apply(iso2_to_iso3)
df = df.dropna(subset=["CountryCode"])

# Experience level definitions
experience_levels = {
    "Entry-level/Junior": "#4285f4",         # blue
    "Mid-level/Intermediate": "#34a853",     # green
    "Senior-level/Expert": "#673ab7",        # purple
    "Executive-level/Director": "#ea4335"    # red
}

# Generate map per level
for level, color in experience_levels.items():
    df_level = df[df["ExperienceLevelName"] == level]
    
    df_grouped = df_level.groupby("CountryCode").size().reset_index(name="JobCount")
    
    fig = px.choropleth(
        df_grouped,
        locations="CountryCode",
        locationmode="ISO-3",
        color="JobCount",
        title=f"{level} Distribution by Company Location",
        color_continuous_scale=[[0, "white"], [1, color]],
        labels={"JobCount": "Job Count"}
    )
    
    fig.update_layout(
        title=dict(
            text=f"{level} Distribution by Company Location",
            x=0,
            xanchor='left',
            font=dict(size=20)
        ),
        margin=dict(l=10, r=10, t=40, b=10),
        paper_bgcolor='rgba(0,0,0,0)',
        geo=dict(showframe=False, showcoastlines=False),
        coloraxis_colorbar=dict(title="Job Count")
    )
    
    fig.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # 🌍 Geo-Level Insights: Location, Movement, and Salary Dynamics
# MAGIC
# MAGIC This section uncovers where data professionals work, live, move — and how it reflects in their pay and company structures globally.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. **Top Experience Level by Company Location (Map)**
# MAGIC
# MAGIC This choropleth map highlights the **dominant experience level in each country**:
# MAGIC
# MAGIC - Countries like **USA and Canada** are heavily populated with **Senior-level Experts**.
# MAGIC - **India and other parts of Asia** trend toward **Mid-level or Entry-level roles**.
# MAGIC - A few regions show **Executive roles** leading — likely HQ or leadership-heavy countries.
# MAGIC
# MAGIC > 🌐  Location reflects maturity. Developed markets lean on seasoned professionals, while emerging tech hubs are **growing their mid-tiers rapidly**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. **Experience Distribution Maps (4 Submaps)**
# MAGIC
# MAGIC Each experience level has its own distribution across the globe:
# MAGIC
# MAGIC - **Senior-level/Expert:** Strongly concentrated in **North America and parts of Europe**.
# MAGIC - **Mid-level/Intermediate:** Diverse, but especially high in **India and Latin America**.
# MAGIC - **Entry-level/Junior:** Distributed globally, with peaks in **developing regions**.
# MAGIC - **Executive-level/Director:** Sparse, focused in **strategic markets** like the US.
# MAGIC
# MAGIC > 🗺️ Career maturity isn't just about skills — it's also about **economic stage and market leadership** in each region.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. **Employee Region → Company Region (Sankey Diagram)**
# MAGIC
# MAGIC This Sankey flow shows the **global migration or outsourcing pattern**:
# MAGIC
# MAGIC - Many employees from **Asia and Latin America** are working for companies in **North America**.
# MAGIC - There’s also a surprising two-way flow between **Europe and North America**.
# MAGIC - **Other regions**, like Africa and Oceania, have less movement — but still show minor participation.
# MAGIC
# MAGIC > 🔁 This reveals strong **remote and cross-border hiring trends**. Data talent travels — often digitally.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. **Comparison of Employee Residence vs. Company Location (Bar Chart)**
# MAGIC
# MAGIC This horizontal grouped bar chart compares **where employees live** versus **where companies are located**:
# MAGIC
# MAGIC - **US dominates** as both **residence and company location**.
# MAGIC - In some countries (like GB, IN, CA), **employee counts outpace company presence**, indicating **outsourced or remote hiring**.
# MAGIC - Other countries show inverse trends.
# MAGIC
# MAGIC > 📌The global job market isn’t symmetric. There’s more **talent exporting** than company presence in several regions.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. **Average Salary by Employee Residence (Bar Chart)**
# MAGIC
# MAGIC This bar chart, with gradient coloring, reveals:
# MAGIC
# MAGIC - **China, Russia, Iraq** and **North European nations** top the salary chart.
# MAGIC - **Eastern and Southern Europe**, along with **South America**, lag behind.
# MAGIC - The **gradient color** shows a clear divide between high and low-salary zones.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6. **Average Salary by Company Location (Map)**
# MAGIC
# MAGIC The choropleth heatmap visualizes salary variation:
# MAGIC
# MAGIC - **US, Switzerland, and Nordic countries** offer the highest **average salaries**.
# MAGIC - Countries in **Africa, Latin America, and Southeast Asia** offer the lowest.
# MAGIC - **Color transition** clearly reflects salary inequality on a global scale.
# MAGIC
# MAGIC > 💰Where the company is **based** impacts the **budget for salaries**, not just where employees live.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧭 Summary Narrative
# MAGIC
# MAGIC Global insights tell a nuanced story:
# MAGIC
# MAGIC - **High experience levels concentrate in North America and Western Europe**.
# MAGIC - **Employees often reside in cheaper labor markets**, but **work for premium-paying regions**.
# MAGIC - **Salary disparities are sharp**, reflecting both regional cost-of-living and strategic HQ positioning.
# MAGIC
# MAGIC > 🌍Geo dynamics matter. They affect **talent pools, salary bands, hiring strategy**, and ultimately, **business competitiveness** in the data economy.
# MAGIC

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)
# MAGIC library(ggplot2)
# MAGIC library(reshape2)
# MAGIC
# MAGIC # Ensure Spark session is initialized
# MAGIC SparkR::sparkR.session()
# MAGIC
# MAGIC # Step 1: Query from Spark
# MAGIC sdf <- SparkR::sql("
# MAGIC   SELECT 
# MAGIC     RemoteRatio,
# MAGIC     SalaryInUSD,
# MAGIC     CASE 
# MAGIC       WHEN CompanySize = 'S' THEN 1
# MAGIC       WHEN CompanySize = 'M' THEN 2
# MAGIC       WHEN CompanySize = 'L' THEN 3
# MAGIC       ELSE NULL
# MAGIC     END AS CompanySizeNum
# MAGIC   FROM ds_salaries_view
# MAGIC   WHERE SalaryInUSD IS NOT NULL AND RemoteRatio IS NOT NULL AND CompanySize IS NOT NULL
# MAGIC ")
# MAGIC
# MAGIC # Step 2: Use SparkR::collect() to convert to R data.frame
# MAGIC df <- SparkR::collect(sdf)
# MAGIC
# MAGIC # Step 3: Calculate correlation matrix
# MAGIC corr_matrix <- round(cor(df, use = "complete.obs"), 2)
# MAGIC
# MAGIC # Step 4: Melt to long format
# MAGIC melted_corr <- melt(corr_matrix)
# MAGIC
# MAGIC # Step 5: Plot with ggplot2
# MAGIC ggplot(melted_corr, aes(x = Var1, y = Var2, fill = value)) +
# MAGIC   geom_tile(color = "white") +
# MAGIC   geom_text(aes(label = value), color = "black", size = 4) +
# MAGIC   scale_fill_gradient2(low = "#e0f7e9", high = "#1c9e63", mid = "white", midpoint = 0, limit = c(-1, 1), name = "Correlation") +
# MAGIC   labs(title = "Correlation Matrix: Remote Work vs. Company Attributes", x = NULL, y = NULL) +
# MAGIC   theme_minimal(base_size = 14) +
# MAGIC   theme(
# MAGIC     plot.title = element_text(size = 18, hjust = 0),
# MAGIC     axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1),
# MAGIC     plot.margin = margin(10, 10, 10, 10)
# MAGIC   )
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  EmploymentTypeName, CompanySize, CompanyLocation, RemoteRatio, JobTitle FROM ds_salaries_view FROM ds_salaries_view limit 10

# COMMAND ----------

# MAGIC  %r
# MAGIC
# MAGIC # Load libraries
# MAGIC library(SparkR)
# MAGIC library(psych)
# MAGIC
# MAGIC # Step 1: Sample 10% of the dataset without replacement
# MAGIC rdf_ds_salaries_view <- collect(
# MAGIC   sample(
# MAGIC     sql("SELECT EmploymentTypeName, CompanySize, CompanyLocation, RemoteRatio, JobTitle FROM ds_salaries_view"),
# MAGIC     withReplacement = FALSE,
# MAGIC     fraction = 1
# MAGIC   )
# MAGIC )
# MAGIC
# MAGIC # Step 2: Encode categorical columns as numeric for correlation analysis
# MAGIC rdf_ds_salaries_view$EmploymentTypeNum <- as.numeric(factor(rdf_ds_salaries_view$EmploymentTypeName))
# MAGIC rdf_ds_salaries_view$CompanySizeNum <- as.numeric(factor(rdf_ds_salaries_view$CompanySize, levels = c("S", "M", "L")))
# MAGIC rdf_ds_salaries_view$CompanyLocationNum <- as.numeric(factor(rdf_ds_salaries_view$CompanyLocation))
# MAGIC
# MAGIC # Step 3: Select only numeric columns for the plot
# MAGIC numeric_df <- rdf_ds_salaries_view[, c("EmploymentTypeNum", "CompanySizeNum", "CompanyLocationNum", "RemoteRatio","JobTitle")]
# MAGIC
# MAGIC # Step 4: Plot scatterplot matrix with correlations
# MAGIC cat("Correlation Matrix of Remote Work and Company Attributes (Sampled Data)\n")
# MAGIC pairs.panels(numeric_df, 
# MAGIC              method = "pearson", 
# MAGIC              hist.col = "#49aeb0",
# MAGIC              main = "Correlation Matrix: Remote Work vs. Company Attributes")
# MAGIC

# COMMAND ----------

# Step 1: Fetch numeric-ready features from Spark SQL
df = spark.sql("""
    SELECT 
        SalaryInUSD, 
        RemoteRatio,
        CASE ExperienceLevelName
            WHEN 'Entry-level/Junior' THEN 1
            WHEN 'Mid-level/Intermediate' THEN 2
            WHEN 'Senior-level/Expert' THEN 3
            WHEN 'Executive-level/Director' THEN 4
        END AS ExperienceLevel,
        CASE CompanySize
            WHEN 'S' THEN 1
            WHEN 'M' THEN 2
            WHEN 'L' THEN 3
        END AS CompanySize
    FROM ds_salaries_view
    WHERE SalaryInUSD IS NOT NULL
      AND RemoteRatio IS NOT NULL
      AND ExperienceLevelName IS NOT NULL
      AND CompanySize IS NOT NULL
""").toPandas()
# Step 2: Normalize and apply KMeans clustering
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import plotly.express as px

features = ["SalaryInUSD", "RemoteRatio", "ExperienceLevel", "CompanySize"]
X_scaled = StandardScaler().fit_transform(df[features])

kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
df["Cluster"] = kmeans.fit_predict(X_scaled)

# Step 3: Plot clusters
fig = px.scatter(
    df,
    x="RemoteRatio",
    y="SalaryInUSD",
    color=df["Cluster"].astype(str),
    title="Cluster Analysis of Job Profiles (KMeans)",
    labels={"color": "Cluster"},
    hover_data=["ExperienceLevel", "CompanySize", "SalaryInUSD"]
)
fig.show()



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ds_salaries_view limit 10

# COMMAND ----------

import plotly.graph_objects as go
import pandas as pd

# Example dataframe (replace with actual Spark SQL result)
df = pd.DataFrame({
    'Source': ['Data Analyst', 'Data Scientist', 'BI Analyst', 'Data Engineer'],
    'Target': ['Data Scientist', 'ML Engineer', 'Data Analyst', 'ML Engineer'],
    'TransitionCount': [124, 87, 65, 42]
})

# Prepare nodes and mappings
all_roles = list(set(df['Source']).union(set(df['Target'])))
role_to_index = {role: i for i, role in enumerate(all_roles)}

# Sankey source and target indices
df['source_id'] = df['Source'].map(role_to_index)
df['target_id'] = df['Target'].map(role_to_index)

# Build Sankey chart
fig = go.Figure(data=[go.Sankey(
    node=dict(
        pad=15,
        thickness=20,
        label=all_roles,
        color="#49aeb0"
    ),
    link=dict(
        source=df["source_id"],
        target=df["target_id"],
        value=df["TransitionCount"],
        hovertemplate='%{source.label} → %{target.label}<br>Count: %{value}<extra></extra>',
        color="rgba(73, 174, 176, 0.5)"
    )
)])

fig.update_layout(
    title_text="Career Path Recommendation Simulation",
    font_size=14,
    margin=dict(l=10, r=10, t=40, b=10),
    paper_bgcolor='rgba(0,0,0,0)'
)

fig.show()


# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import plotly.express as px

# Step 1: Load average salary per year from Spark
df = spark.sql("""
    SELECT WorkYear, ROUND(AVG(SalaryInUSD), 2) AS AvgSalary
    FROM ds_salaries_view
    WHERE SalaryInUSD IS NOT NULL AND WorkYear IS NOT NULL
    GROUP BY WorkYear
    ORDER BY WorkYear
""").toPandas()

# Step 2: Prepare future years (e.g., 2024, 2025)
future_years = pd.DataFrame({"WorkYear": [2024, 2025]})
df_all = pd.concat([df, future_years], ignore_index=True)

# Step 3: Train Linear Regression Model
X = df["WorkYear"].values.reshape(-1, 1)
y = df["AvgSalary"].values
model = LinearRegression()
model.fit(X, y)

# Step 4: Predict for all years
df_all["ForecastedSalary"] = model.predict(df_all["WorkYear"].values.reshape(-1, 1))

# Step 5: Plot
fig = px.line(
    df_all,
    x="WorkYear",
    y="ForecastedSalary",
    title="Forecast: Average Salary Growth (2020–2025)",
    labels={"WorkYear": "Year", "ForecastedSalary": "Avg Salary (USD)"}
)

# Add actual points
fig.add_scatter(
    x=df["WorkYear"],
    y=df["AvgSalary"],
    mode="markers+lines",
    name="Historical Data"
)

fig.update_layout(
    title=dict(text="Time Series Forecast: Average Salary Growth", x=0, xanchor='left', font=dict(size=20)),
    margin=dict(l=10, r=10, t=40, b=10),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)'
)

fig.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Advanced Patterns – Analytical Storytelling
# MAGIC
# MAGIC This dashboard reveals deeper patterns in the data, using correlation analysis, clustering, job structure mapping, and salary forecasting.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔄 Correlation Matrix: Remote Work vs. Company Attributes
# MAGIC At a glance, the correlation matrix shows **very weak relationships** between `RemoteRatio` and company factors like `EmploymentType`, `CompanySize`, or `Location`. This suggests that remote work opportunities are **evenly distributed** across different company types, with no strong preference pattern emerging from the data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧩 How Jobs Are Distributed Across Experience Levels and Contracts
# MAGIC The majority of roles are **full-time**, and that trend holds across all experience levels. Interestingly:
# MAGIC - Over **2,500 Senior-level/Expert** roles are full-time.
# MAGIC - Contract and freelance roles are minimal, hinting at the **structured nature of hiring** in this sector.
# MAGIC This reinforces the idea that **stability and expertise** are highly valued in the data industry.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔮 Time Series Forecast: Average Salary Growth
# MAGIC Historical trends from 2020 to 2023 reveal a **steady upward salary growth**. Based on linear regression forecasting:
# MAGIC - Salaries are expected to continue rising into 2024 and 2025.
# MAGIC - This aligns with the global demand surge for advanced data professionals.
# MAGIC The projection tells a positive story: **investing in skills yields increasing financial returns**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Cluster Analysis of Job Profiles (KMeans)
# MAGIC By grouping jobs based on `RemoteRatio` and `SalaryInUSD`, three main clusters emerge:
# MAGIC - **Cluster 0**: High remote ratio, high salary (likely Senior Data Engineers, Scientists)
# MAGIC - **Cluster 1**: Mid-salary, mixed remote work
# MAGIC - **Cluster 2**: Lower salary or entry-level roles
# MAGIC This segmentation can help guide **career pathway recommendations**, team balancing, and future hiring strategies.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔍 Insights Summary
# MAGIC This panel gives decision-makers:
# MAGIC - A sense of **structural patterns** in job types
# MAGIC - The **impact of experience and time** on pay
# MAGIC - Clear **clusters of workforce** by salary and work style
# MAGIC
# MAGIC Together, these visualizations allow **forecasting**, **profiling**, and **actionable segmentation**, making it a powerful tool for workforce planning and career growth analytics.
# MAGIC