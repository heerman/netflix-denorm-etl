# Quickstart


## Execute the pipeline locally with the provided test data

$ gzip netflix-streams-simulated.json
$ gzip netflix-media-simulated.json
$ gzip netflix-users-simulated.json

$ mvn compile exec:java \
      -Dexec.mainClass=org.apache.beam.examples.NetflixStreamsDenorm \
      -Dexec.args="\
        --input_streams=netflix-streams-simulated.json.gz \
        --input_media=netflix-users-simulated.json.gz \
        --input_users=netflix-media-simulated.json.gz \
        --output=output"


## Execute the pipeline in Google Cloud Platform

$ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/json/credential/file.json

$ mvn compile exec:java \
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
