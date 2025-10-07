#Data Catalog for Gold Layer 

Overview

The Gold Layer is the business-level data representation, structured to support analytical and reporting use cases. It consists of dimension 

tables and fact tables for specific business metrics.



1. gold.dim_customers
   
. Purpose: Stores customer details enriched with demographic and geographic data.

. Columns:

<img width="830" height="373" alt="image" src="https://github.com/user-attachments/assets/fcaba338-4b71-4ba6-9345-e8208c2951e3" />


2. gold.dim_products

. Purpose: Provides information about the products and their attributes.

. Columns:

<img width="912" height="424" alt="image" src="https://github.com/user-attachments/assets/43870a33-85d7-4199-a53f-27ce6ef91bea" />


3. gold.fact_sales

. Purpose: Stores transactional sales data for analytical purposes.

. Columns:

<img width="816" height="338" alt="image" src="https://github.com/user-attachments/assets/9034eeca-ea59-4225-b505-80e44f666e1d" />
