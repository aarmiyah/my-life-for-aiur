import logging
from mapreduce import input_readers
from mapreduce import mapreduce_pipeline
from mapreduce import base_handler
from mapreduce import output_writers

STORAGE_BUCKET = 'schedulr-mapreduce-bucket'

MAX_BLOBS = 20

N_SHARDS = 8


def schedulr_map ((line_number, line_str)):
    print line_str
    for word in line_str.strip().split():
        yield (word, "")

def schedulr_reduce (key, values):
    yield str ((key, len(values)))

# MapReduce class to start the job
class SchedulrMapReducePipeline(base_handler.PipelineBase):
    def run(self, records_file_blobkey):
        job_name = "schedulrMapReduce"
        logging.info ("***map ***reduce ***library ***cool****** about 2 running: %s" % records_file_blobkey)
        # Run Mapreduce
        output = yield mapreduce_pipeline.MapreducePipeline(
            job_name,
            __name__ + ".schedulr_map",
            __name__ + ".schedulr_reduce",
            input_reader_spec=input_readers.__name__ + ".BlobstoreLineInputReader",
            output_writer_spec=(
                output_writers.__name__ + ".FileOutputWriter"),
            mapper_params={
                "input_reader": {
                    "blob_keys": [records_file_blobkey]
                }
            },
            reducer_params={
                "output_writer": {
                    "mime_type": "text/plain",
                    "output_sharding": output_writers.FileOutputWriterBase.OUTPUT_SHARDING_NONE,
                    "filesystem": "blobstore"
                },
            },
            shards=N_SHARDS)  
#         yield StoreOutput("WordCount", filekey, output) 
           
    #Called when the mapper finishes
    def finalized(self):
        logging.info("Done: " + self.pipeline_id)