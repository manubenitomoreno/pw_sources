# Project Walknet (Sources)

## Description
Project Walknet (Sources) is a data orchestration program designed for the general Project Walknet, which is part of a PhD Research Project at the Universidad Politécnica de Madrid. This program contains several modules to run data pipelines, save and display performance statistics, an Object-Relational Mapping (ORM) for database interaction, and a directory `/sources` containing the pipelines themselves.

The key components of the repository are as follows:

- `config.ini`: This file includes essential configurations for the project such as repository path, database connection information, and datalake paths.

- JSON files: These files (`database_statistics.json`, `execution_statistics.json`, `table_statistics.json`) contain various statistics and feed the Streamlit dashboard. They get updated after each pipeline execution.

- `sources` directory: This contains the pipelines for different data sources. The pipelines included are: `ine_geo`, `ine_adrh`, `edm2018`, `edm2018_geo`, `osm`, `catastro`, `ine_movilidad`, `amm_network`, and `cartociudad`.

- `project_walknet` directory: This contains the main modules of the program:
    - `app.py`: A Streamlit application to visualize the state of the datalake, database, and pipeline performance, as well as configuration parameters for each pipeline.
    - `db_models.py`: Provides an ORM to interact with the PostgreSQL database.
    - `run.py`: Orchestrates the execution of pipelines. For each data source, it gathers, transforms, and persists the data.
    - `run_statistics.py`: Generates various statistics after each execution of the pipeline, which then get saved into the JSON files for the dashboard.
    - `source_factory.py`: Implements the `Source` class that orchestrates pipelines.
    - `sources.yaml`: A configuration file for the pipelines.

## Project Structure

The directory structure of this repository is:

\n
├── README.md\n
├── .gitignore\n
├── config.ini\n
│\n
├── database_statistics.json\n
├── execution_statistics.json\n
├── table_statistics.json\n
│\n
├── sources\n
│ ├── ine_geo.py\n
│ ├── ine_adrh.py\n
│ ├── edm2018.py\n
│ ├── edm2018_geo.py\n
│ ├── osm.py\n
│ ├── catastro.py\n
│ ├── ine_movilidad.py\n
│ ├── amm_network.py\n
│ └── cartociudad.py\n
│\n
└── project_walknet\n
├── app.py\n
├── db_models.py\n
├── run.py\n
├── run_statistics.py\n
├── source_factory.py\n
└── sources.yaml\n
\n
## Installation
Follow these steps to get the project up and running on your local machine for development and testing purposes:

1. **Create and activate a virtual environment:**

python -m venv env
Requirements lay in the requirements.txt file
source env/bin/activate  # On Windows use `.\env\Scripts\activate`

2. **Clone the repository and install the requirements:**

git clone https://github.com/yourusername/projectname.git
cd projectname
pip install -r requirements.txt

3. **Build a local directory structure for your datalake to store the intermediate data:**
 Set the path to your datalake in the config.ini file
 It should look like this

├── {your datalake}\n
│ ├── source_1\n
│ │    ├── level0\n
│ │    ├── level1\n
│ │    ├── level2\n
│ ├── source_2\n
│ │    ├── level0\n
│ │    ├── level1\n
│ │    ├── level2\n
│ ├── ...\n
\n
4. **Setup a Postgres/PostGIS enabled database. Follow the instructions from PostGIS installation documentation.** 
(https://postgis.net/documentation/getting_started/)
Set the connection parameters in your config.ini

## Usage

You can simply run:
\n
python run.py --arg1 --arg2 if you are running a pipeline segment\n
--arg1 should be one of the implemented source names\n
--arg2 should be one of the following: gather, level0, level1 and persist\n
Other arguments include\n
--list-sources: list of the implemented sources\n
--config-params: shows actual configuration parameters\n
--reset-db: reboots the whole database\n
--reset-source: reboots the datalake for a particular source\n

## DASHBOARD APP

It can be run inside /project_walnet with:
streamlit run app.py
This will display it locally
Also, the app has been published at
(https://pwsources.streamlit.app/)
