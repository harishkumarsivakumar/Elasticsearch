# # Elasticsearch Data Analytics

This project demonstrates how to perform basic data analytics operations using Elasticsearch. We will create collections, index data, search by column, count employees, delete employees by ID, and retrieve department facets.

## Features
- **Create Collection**: Create a new collection in Elasticsearch.
- **Index Data**: Index employee data into the specified collection, excluding a specified column.
- **Search by Column**: Search within the specified collection for records matching a given column and value.
- **Get Employee Count**: Retrieve the count of employees in the specified collection.
- **Delete Employee by ID**: Delete an employee by their ID from the specified collection.
- **Get Department Facet**: Retrieve the count of employees grouped by department from the specified collection.

## Prerequisites
- Elasticsearch running on port 8989.
- JDK 8 or later installed.
- `employee_data.csv` downloaded from [Kaggle](https://www.kaggle.com/datasets/williamlucas0/employee-sample-data).

## Setup
1. **Start Elasticsearch Service**:
   ```sh
   sudo service elasticsearch start
   netstat -an | find "8989"   # for Windows
lsof -i:8989                # for macOS/Linux
git clone <your-github-repo-url>
cd ElasticsearchDataAnalytics


