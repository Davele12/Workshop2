---

# ETL Data Pipeline with Apache Airflow

This repository contains the necessary files to set up an ETL (Extract, Transform, Load) pipeline using Apache Airflow, Docker, and Python. The pipeline is designed to process and merge data from Spotify's dataset and Grammy Awards dataset.

## Prerequisites

Before you can run this project, you'll need to have the following installed:

- Docker
- Docker Compose
- Python 3.8 or higher
- Apache Airflow

## Installation

1. **Clone the repository**
   
   ```bash
   git clone https://your-repository-url.git
   cd your-repository-directory
   ```

2. **Set up Airflow using Docker**

   This project uses Docker to run Apache Airflow. You can set up Airflow using the `docker-compose.yml` file provided in the repository.

   ```bash
   docker-compose up -d
   ```

   This command will start all the necessary services defined in `docker-compose.yml`, including the Airflow webserver, scheduler, and PostgreSQL database.

3. **Initial Airflow Setup**

   After starting the Docker containers, you will need to initialize the Airflow database and create an admin user. You can do this by running:

   ```bash
   docker exec -it airflow-webserver airflow db init
   docker exec -it airflow-webserver airflow users create \
       --username admin \
       --firstname YOUR_FIRST_NAME \
       --lastname YOUR_LAST_NAME \
       --role Admin \
       --email YOUR_EMAIL
   ```

## Project Structure

- `dags/`: Contains all the DAG files for Airflow.
- `plugins/`: Contains any custom plugins for Airflow.
- `docker-compose.yml`: Docker compose file to set up the Airflow environment.
- `requirements.txt`: Python dependencies required for the project.

## Running the Pipeline

Once everything is set up, you can access the Airflow web interface by visiting `http://localhost:8080` in your web browser. Use the credentials you created during the setup to log in.

You can trigger the pipeline manually from the Airflow web interface or schedule it to run automatically at your specified intervals.

## Data

This pipeline processes data from two sources:

- **Spotify Dataset**: Contains various attributes of songs from Spotify, including popularity, genre, and other musical features.
- **Grammy Awards Dataset**: Contains information about Grammy nominations and awards.

The pipeline extracts data from these sources, transforms them for analysis, and merges the results based on specified criteria.

## Transformations

The pipeline includes several transformation steps to prepare the data for merging and analysis, such as converting date fields, normalizing text fields, and more.

## Merging Data

The final step in the pipeline is to merge the Spotify and Grammy datasets based on common fields like artist names and track titles.

---

## Configuration and Credentials

### Environment Configuration

The project uses `.env` files to manage environment variables for different components like PostgreSQL and Airflow. You can find and edit these files in the `config/` directory of the project. It's crucial to configure these files before running the project to ensure that all components can communicate with each other properly.

### Editing Credentials

The default credentials for both the Airflow webserver and the PostgreSQL database used by Airflow are set as follows:

- **Airflow Webserver**:
  - Username: `airflow`
  - Password: `airflow`

- **PostgreSQL Database**:
  - Username: `airflow`
  - Password: `airflow`
  - Database Name: `airflow`

These credentials are specified in the `.env` files located in the `config/` directory. To change any of these credentials, you must update the respective `.env` files and restart the Docker containers to apply the changes.

### Security Notice

For production environments, it is highly recommended to change the default passwords to secure ones and keep the `.env` files confidential to protect sensitive information.

---

## Conclusion

Basic overview of setting up and running the ETL pipeline with Apache Airflow. For detailed customization and advanced usage, refer to the official [Airflow documentation](https://airflow.apache.org/docs/).

---

