# netflix-streams-denorm

A production-scale ETL pipeline with Netflix streams data *(simulated)*

## Pipeline

* Apache Beam, Java SDK

* Google Cloud Platform & Dataflow

* Performs 2 relational joins using "map reduce" style algorithms

```
(Streams JSON Dataset) ->> Filter JSON ->> Join #1
                                                    ->> (Streams + Media)
(Media JSON Dataset)   ->> Filter JSON ->> Join #1


(Streams + Media)      ->> Filter JSON ->> Join #2
                                                    ->> (Final Output)
(Users JSON Dataset)   ->> Filter JSON ->> Join #2
```


## Execute the pipeline in Google Cloud Platform

* As you run your pipeline with `mvn`, you can watch the progress from cloud.google.com

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/json/credential/file.json

mvn compile exec:java \
      -Pdataflow-runner \
      -Dexec.mainClass=org.apache.beam.examples.NetflixStreamsDenorm \
      -Dexec.args="\
        --runner=DataflowRunner \
        --project=heerman-gcp \
        --input_streams=gs://netflix-dataset/netflix-streams.json.gz \
        --input_media=gs://netflix-dataset/netflix-users.json.gz \
        --input_users=gs://netflix-dataset/netflix-media.json.gz \
        --stagingLocation=gs://netflix-denorm-output/staging \
        --gcpTempLocation=gs://netflix-denorm-output/temp \
        --output=gs://netflix-denorm-output/output/streams_denorm.json"
```


## Execute the pipeline locally with the provided test data
```bash
gzip netflix-streams-simulated
gzip netflix-media-simulated
gzip netflix-users-simulated

mvn compile exec:java \
      -Dexec.mainClass=org.apache.beam.examples.NetflixStreamsDenorm \
      -Dexec.args="\
        --input_streams=netflix-streams-simulated.json.gz \
        --input_media=netflix-users-simulated.json.gz \
        --input_users=netflix-media-simulated.json.gz \
        --output=output"
```
