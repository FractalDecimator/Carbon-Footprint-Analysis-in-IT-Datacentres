{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "75cfe607-cb6f-41c3-ad56-8acdd3e39df0",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Load sample IT carbon emission dataset\n",
    "df = spark.read.format(\"csv\").option(\"header\", \"true\").load(\"/Volumes/carbonfootprint/default/carbonfootprint/datacentresworldwide.csv\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "fe00b559-7159-4422-9221-51af2102f192",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "display(df)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "d30a9b51-3da6-433b-a754-6528bbf49ebc",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "df.columns"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "371d2944-2b18-400d-b89b-40185151d04f",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "df.describe()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "0d1574b0-6d02-4eb1-bc5a-f71096bffcc5",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Remove missing values\n",
    "df_dropped_any = df.na.drop(how=\"any\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "c3b82a61-5a45-43fc-b9a0-8a64f34a2cb7",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Data cleaning and type casting\n",
    "from pyspark.sql.functions import col\n",
    "df_clean = df.withColumn(\"_c5\", col(\"_c5\").cast(\"string\"))\\\n",
    ".withColumn(\"_c7\", col(\"_c7\").cast(\"string\"))\\\n",
    ".withColumn(\"Number of data center\", col(\"Number of data center\").cast(\"string\"))   \n",
    "\n",
    "        "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "4ed4a4ac-9899-4fdc-a821-25be8525d6a6",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Real-time aggregation - Carbon footprint per country\n",
    "\n",
    "from pyspark.sql.functions import col, expr\n",
    "\n",
    "df_emission_bycountry = (\n",
    "    df_clean.withColumn(\n",
    "        \"_c5\",\n",
    "        expr(\"try_cast(`_c5` as double)\")\n",
    "    ).withColumn(\n",
    "        \"_c7\",\n",
    "        expr(\"try_cast(`_c7` as double)\")\n",
    "    ).groupBy(\n",
    "        col(\"Country\"),\n",
    "        col(\"Region\")\n",
    "    ).agg(\n",
    "        {\"_c5\": \"sum\", \"_c7\": \"sum\"}\n",
    "    ).withColumnRenamed(\n",
    "        \"sum(_c5)\",\n",
    "        \"gross_carbon_emission\"\n",
    "    ).withColumnRenamed(\n",
    "        \"sum(_c7)\",\n",
    "        \"available_gross_carbon_emission\"\n",
    "    )\n",
    ")\n",
    "display(df_emission_bycountry)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "b32a3708-e77d-4079-bc7b-644fdb3a4931",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Emission by Region with high number of Datacenters\n",
    "from pyspark.sql.functions import desc\n",
    "max_dc = df.orderBy(desc(\"Number of data centre\")).limit(10)\n",
    "max_dc.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "d53e0c4f-aa81-40f1-b5ca-b077037398da",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Identify top emitting Countries for carbon optimisation\n",
    "top_emitters = df_emission_bycountry.orderBy(\n",
    "    col(\"gross_carbon_emission\").desc()\n",
    ").limit(10)\n",
    "display(top_emitters)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "39da249c-b828-49f0-b76d-809fb0b0faca",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Visualize carbon emission trends over region\n",
    "import matplotlib.pyplot as plt\n",
    "\n",
    "pandas_df = df_emission_bycountry.toPandas()\n",
    "plt.figure(figsize=(10,6))\n",
    "for country in pandas_df['Region'].unique():\n",
    "    country_data = pandas_df[pandas_df['Region'] == country]\n",
    "    plt.plot(\n",
    "        country_data['Region'],\n",
    "        country_data['gross_carbon_emission'],\n",
    "        label=country\n",
    "    )\n",
    "plt.xlabel('Region')\n",
    "plt.ylabel('Gross Emission')\n",
    "plt.title('Carbon Emission per Region')\n",
    "plt.legend()\n",
    "plt.tight_layout()\n",
    "display(plt)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "ded1fc9a-848a-4ff7-9f9b-c879c70711cd",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Visualise Box plot with available carbon emission for Datacenters by Region\n",
    "df_for_plot = df.select(\"Region\",\"Number of data centre\",\"_c7\").toPandas()\n",
    "import matplotlib.pyplot as plt\n",
    "import seaborn as sns\n",
    "plt.figure(figsize=(10,6))\n",
    "sns.boxplot(x=\"Region\", y=\"_c7\", data=df_for_plot)\n",
    "plt.title(\"Available Carbon emission by Region\")\n",
    "plt.xticks(rotation=90)\n",
    "plt.xlabel(\"Region\")\n",
    "plt.ylabel(\"Carbon emission\")\n",
    "plt.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "2ccadbc3-6e2c-45ee-866f-10d536ccbc31",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "3"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "Carbon Footprint Analysis in IT 2025-09-17 20:09:37",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
