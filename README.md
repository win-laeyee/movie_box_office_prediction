# Movie Box Office Prediction

## Objectives

This project aims to provide comprehensive analysis through visualisations on box office performance, including past trends and key factors influencing success, presented through an interactive dashboard. This project also aims to train a machine learning model that predicts box office performance, where users can interact by providing inputs. This project will require us to collect and transform large amounts of movie-related data from various sources.

## Final Analytics Product
Access our final analytics product here: https://movie-box-office-dashboard-and-prediction.streamlit.app.

<img width="626" alt="Screenshot 2024-04-28 at 12 45 30 AM" src="https://github.com/win-laeyee/movie_box_office_prediction/assets/97848295/b53cb0b4-1770-4768-9b50-80140898ab60">
<img width="569" alt="Screenshot 2024-04-28 at 12 45 36 AM" src="https://github.com/win-laeyee/movie_box_office_prediction/assets/97848295/67368e06-bda2-4124-a069-2ac745e59b3a">


## Getting Started

### Prerequisites
Before you begin, ensure you have Docker installed on your machine. If not, you can download it from Docker's official website.

## Install packages
`pip install -r requirements.txt`

### Environment Setup
To avoid permission issues with Airflow, it's recommended to create a .env file with your user ID. Run the following command in your project's root directory:
`echo -e "AIRFLOW_UID=$(id -u)" > .env`

### Running Airflow
- Start the Containers: Use Docker Compose to start the containers. This command will also build the containers if they haven't been built before:
`docker compose up -d`

- Access the Airflow web interface by going to http://localhost:8080. 
- Shutting Down the Containers: When you're done, you can shut down the containers by running:
`docker compose down`

### Running Streamlit
- To run the streamlit UI: `streamlit run app.py`

## Data Sources
- Movie Database API (TMDB)
- YouTube API
- Vimeo API
- BoxOfficeMojo
  
## Toolchain

- BeautifulSoup for web scraping
- Google BigQuery for data warehousing
- Google BigQuery ML for model building and prediction
- Apache Airflow for scheduling and monitoring workflows
- Streamlit for dashboard building
