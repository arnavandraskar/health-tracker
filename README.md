# Personalized Health Monitoring

Personalized Health Monitoring is a project aimed at providing elderly individuals (age group 65+) with a holistic approach to tracking and managing their health. It utilizes technology to gather health data from various sources and presents it in an easy-to-understand format, allowing users to monitor their health parameters and make informed decisions about their well-being.

## Table of Contents
- [Introduction](#introduction)
- [Features](#features)
- [Technology Used](#technology-used)
- [Prototype Description](#prototype-description)
- [Data Visualization](#data-visualization)
- [Deployment](#deployment)
- [Front-end View](#front-end-view)


## Introduction

The Personalized Health Monitoring project aims to empower users (age group 65+) to take control of their health by providing them with a comprehensive platform to track and monitor their vital health parameters. By utilizing wearable devices and manual input, the system gathers data such as heart rate, daily steps, sleep patterns, and additional health information. It then presents this information through a user-friendly interface, offering visualizations and trends over time.

## Features

- Real-time tracking of health parameters, including heart rate, steps, sleep patterns, and more.
- Integration with wearable devices for automatic data collection.
- User-friendly dashboard for visualizing and analyzing health data.
- Continuous data monitoring and updates.

## Technology Used

The Personalized Health Monitoring project utilizes the following technologies:

- **Apache Kafka**: For stream processing and real-time data ingestion.
- **Flask**: For front-end view of the smartwatch-api and dashboard.
- **PostgreSQL**: A powerful and open-source relational database management system.
- **Google Looker Studio**: A data visualization and reporting tool for creating interactive dashboards.

## Prototype Description

The prototype of the Personalized Health Monitoring project consists of several components that work together to provide an enhanced health-tracking experience:

1. **API Simulation:** The `smartwatch_api.py` script simulates the functionality of a smartwatch API. It generates random health data and sends it to the system at regular intervals.

2. **ETL Process:** The `ETL.py` script performs the Extract, Transform, and Load (ETL) process. It consumes the health data sent by the API simulation script, transforms it into a structured format, and loads it into a PostgreSQL database for further analysis.

3. **Data Visualization:** The project includes a Google Looker Studio dashboard ([link](https://lookerstudio.google.com/reporting/b3895062-ea5d-4b8f-9f58-cb7c2232bef0)) that provides an intuitive visualization of the health data. The dashboard offers various charts, graphs, and insights to help users monitor their health parameters and progress toward their goals.


## Deployment

The Personalized Health Monitoring project is deployed on an AWS EC2 instance, ensuring continuous availability. The scripts are running continuously to capture and process real-time health data.


## Front-end View
This flask-based link: http://3.111.196.232:5000/ is made for front-end visualization.

### Page 1: Dashboard View
Google Loocker Studio dashboard is deployed on this page link: http://3.111.196.232:5000/Sunita_Sharma/dashboard

![image](https://github.com/arnavandraskar/fitness-tracker/assets/80948956/d236af21-3088-4721-91f4-b279543ed7eb)

 
### Page 2: Developer View
This page link: http://3.111.196.232:5000/Sunita_Sharma/health_data is made for front-end visualization of the live streaming data (JSON) as well as for further development purposes. This web application shows the latest 10 data points of the tracked parameters. We are dumping it in the JSON format for development purposes.

![image](https://github.com/arnavandraskar/fitness-tracker/assets/80948956/b9cba0fb-65b1-4e18-a3b8-cc05eeb92786)





