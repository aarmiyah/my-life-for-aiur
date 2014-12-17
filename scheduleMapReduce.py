import logging
import os
import jinja2
import webapp2



from google.appengine.ext import blobstore
from google.appengine.ext import ndb
from google.appengine.ext.webapp import blobstore_handlers


from mapreduce import input_readers
from mapreduce import mapreduce_pipeline
from mapreduce import base_handler
from mapreduce import output_writers




JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)

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
    
    
    
class BlobFileModel (ndb.Model):
    filename = ndb.StringProperty()
    blobkey = ndb.StringProperty()



class SitePage (object):
    def __init__ (self, htmlFile):
            self.values = {
                'title': 'Schedulr Mapreduce',
                'description': "surfing the worldwide inter-tube-way",
                'nav_links': []
            }
            # add css links
            self.values['css_links'] = [
                '//maxcdn.bootstrapcdn.com/bootstrap/3.3.1/css/bootstrap.min.css',
                '//cdnjs.cloudflare.com/ajax/libs/animate.css/3.2.0/animate.min.css',
                'css/main.css'
            ]
            # add site nav links
            nav_links = ['populate', 'run']
            for link in nav_links:
                self.values['nav_links'].append ({
                                           'text': link,
                                           'url': '/' + link
                                           })
            self.htmlFile = htmlFile
    def render (self):
        template = JINJA_ENVIRONMENT.get_template('templates/' + self.htmlFile)
        return template.render (self.values)
    
    
            
class PopulatePage (SitePage):
    def __init__ (self):
        super(PopulatePage, self).__init__('populate.html')
        
        
        

class RunPage (SitePage):
    def __init__ (self):
        super(RunPage, self).__init__('run.html')
        # fetch all blob filenames and build a dict that will 
        # be used to handle requests for blobkeys based on their name
        # TODO
        blob_files = BlobFileModel.query().fetch(limit=20)
        
        
        
        
        
class ReadMePage (SitePage):
    def __init__ (self):
        super(ReadMePage, self).__init__('readme.html')
        
        

class PopulateHandler (webapp2.RequestHandler):
    def get(self):
        page = PopulatePage()
        page.values['upload_url'] = blobstore.create_upload_url ('/upload')
        just_uploaded_blobkey = self.request.get("just_uploaded_blobkey")
        page.values['remaining_blobspace'] = MAX_BLOBS - BlobFileModel.query().count()
        if (just_uploaded_blobkey is not None):
            page.values['just_uploaded_blobkey'] = just_uploaded_blobkey
        self.response.write (page.render())
        
        
        
class RunHandler (webapp2.RequestHandler):
    page = RunPage()
    def get(self):
        page = RunPage()
        page.values['blobfiles'] = BlobFileModel.query().fetch(limit=20)
        self.response.write (page.render())
        
    def post(self):
        # get params
        records_file_blobkey = self.request.get('blobkey')
        # actually do a job
        pipeline = SchedulrMapReducePipeline(records_file_blobkey)
        pipeline.start()

        redirect_url = "%s/status?root=%s" % (pipeline.base_path,
                                               pipeline.pipeline_id)
        self.redirect(redirect_url)
        

        
class ReadMeHandler (webapp2.RequestHandler):
    def get(self):
        page = ReadMePage()
        self.response.write (page.render())
    
    
        
class UploadHandler (blobstore_handlers.BlobstoreUploadHandler, webapp2.RequestHandler):
    @ndb.transactional
    def post(self):
        uploads = self.get_uploads()
        upload_file = self.get_uploads('file')
        blob_info = upload_file[0]
        # store the blob we just uploaded in datastore
        blobFile = BlobFileModel(filename=self.request.get('filename'),
                                 blobkey = str(blob_info.key()))
        blobFile.put()
        self.redirect('/populate?just_uploaded_blobkey=%s' % blob_info.key())



application = webapp2.WSGIApplication([
                                       ('/populate', PopulateHandler),
                                       ('/run', RunHandler),
                                       ('/upload', UploadHandler),
                                       ('/*', ReadMeHandler)
                                       ], debug=True)
