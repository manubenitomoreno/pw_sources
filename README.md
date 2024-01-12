# Project Walknet (Sources)

## Description
Project Walknet (Sources) is a data orchestration program designed for the general Project Walknet, which is part of the ACC<15 RTD project (PID2020-116584RB-I00/AEI/10.13039/501100011033) financed by the Spanish
Ministry of Science and Innovation, at the Universidad Politécnica de Madrid. You can find more details of this program at its [website](https://blogs.upm.es/proximityplanning/en/)
This program contains several modules to run data pipelines, save and display performance statistics, an Object-Relational Mapping (ORM) for database interaction, and a directory `/sources` containing the pipelines themselves.

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

```
├── README.md
├── .gitignore
├── config.ini
│
├── database_statistics.json
├── execution_statistics.json
├── table_statistics.json
│
├── sources
│ ├── ine_geo.py
│ ├── ine_adrh.py
│ ├── edm2018.py
│ ├── edm2018_geo.py
│ ├── osm.py
│ ├── catastro.py
│ ├── ine_movilidad.py
│ ├── amm_network.py
│ └── cartociudad.py
├── networks
│ ├── make_netkork.py
│ ├── set_network.py
│ └── metrics.py
├── extractions
│ └── amm_network.sql
└── project_walknet
    ├── app.py
    ├── db_models.py
    ├── run.py
    ├── run_statistics.py
    ├── source_factory.py
    ├── network_factory.py
    ├── extraction_factory.py
    ├── model_factory.py
    ├── extractions.yaml
    ├── networks.yaml
    ├── models.yaml
    └── sources.yaml
```
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
```
├── {your datalake}
│ ├── source_1
│ │    ├── level0
│ │    ├── level1
│ │    ├── level2
│ ├── source_2
│ │    ├── level0
│ │    ├── level1
│ │    ├── level2
│ ├── ...
```
4. **Setup a Postgres/PostGIS enabled database. Follow the instructions from PostGIS installation documentation.** 
(https://postgis.net/documentation/getting_started/)
Set the connection parameters in your config.ini

## Usage

You can simply run:
```
python run.py 
    -s if you are running a pipeline segment
        --arg1 should be one of the implemented source names ()
        --arg2 should be one of the following: gather, level0, level1 and persist
    -n if you are running a network process
        --arg1 should be one of the implemented network names ()
        --arg2 should be one of the following: construct, 
Other arguments include
--list-sources: list of the implemented sources
--config-params: shows actual configuration parameters
--reset-db: reboots the whole database
--reset-source: reboots the datalake for a particular source
```
## DASHBOARD APP

It can be run inside /project_walnet with:
streamlit run app.py
This will display it locally
Also, the app has been published at
(https://pwsources.streamlit.app/)

## To-Do List

- [x] Implement schema parametrization in declarative bases (ORM)
- [ ] Implement prefix parametrization in declarative bases (ORM) for network tables
- [ ] Implement a method to retrieve data on demand in `db_models.py`

- [ ] Implement the `make_network.py` module with the DB interface
- [ ] Implement the `set_network.py` module
- [ ] Implement the `network_factory.py` module

- [ ] Implement `ine_movilidad` pipeline
- [ ] Implement `cartociudad` pipeline
- [ ] Implement `edm_geo.py` pipeline
- [ ] Implement `edm2028.py` pipeline

- [ ] Make a decision on CRS handling
- [ ] Make a function to re-populate the whole sources schema automatically

- [ ] Make a logger pipeline to query it and inspect performance in detail


- [ ] Implement the `extractions_factory.py` module
- [ ] Implement the `edm_extraction.py` module


