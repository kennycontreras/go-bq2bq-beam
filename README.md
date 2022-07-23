# Apache Beam pipeline with Golang

This is a simple pipeline that reads a table from BigQuery and writes the data into a JSON files.

    []: # Language: golang
    []: # Path: bqtostorage.go

It can be deployed with a Dataflow Runner and connect to a Google Cloud Storage.

# How to Use

## Define the Query

The pipeline is using Standard SQL syntax. The query is executed in BigQuery and the pipeline parse the data.

## Define the Struct

It's important that the struct has the same structure/schema as the SQL query to execute.
If the struct is not defined, the pipeline will fail.

## Execute the pipeline with the correct parameters

At the moment, the pipeline is expecting the following parameters:
- `--project_id`: The project id of the Google Cloud Platform project.
- `--output_file`: The name of the output file.
- `--extension`: The extension of the output file.

## TODO

- Implement option to process diferent output formats. (JSON, CSV, etc.)
- Add pipeline options to define SQL Query from input parameters.
- Struct from input parameters (?) Reflects can be an option for this. 




