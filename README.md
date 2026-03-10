# Versioned Retail Lakehouse on AWS with Apache Iceberg & Nessie

## Overview
This project demonstrates a mini retail lakehouse architecture using:
- AWS S3 for object storage
- Apache Iceberg for table format
- Project Nessie for versioned catalog
- Spark notebooks for ingestion and transformations
- Bronze / Silver / Gold layers
- GitHub Actions for lightweight automation

## Sources
- CSV retail sales dataset
- XML product catalog dataset

## Architecture
Raw CSV/XML -> S3 -> Bronze Iceberg -> Silver Iceberg -> Gold Iceberg -> Nessie Catalog

## Layers
- Bronze: raw append-only tables
- Silver: cleaned and standardized tables
- Gold: curated business-ready tables