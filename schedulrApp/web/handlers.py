import os
import webapp2
import jinja2

from google.appengine.ext import blobstore
from google.appengine.ext import ndb
from google.appengine.ext.webapp import blobstore_handlers

import schedulrApp.processing.map_reduce
from schedulrApp.storage.db import BlobFileModel
import schedulrApp.web.pages as pages

        

class PopulateHandler (webapp2.RequestHandler):
    def get(self):
        page = pages.PopulatePage(self.request)
        self.response.write (page.render())
        
        
        
class RunHandler (webapp2.RequestHandler):
    def get(self):
        page = pages.RunPage(self.request)
        self.response.write (page.render())
        
    def post(self):
        # get params
        records_file_blobkey = self.request.get('blobkey')
        # actually do a job
        pipeline = schedulrApp.processing.map_reduce.SchedulrMapReducePipeline(records_file_blobkey)
        pipeline.start()
        self.redirect('/monitor?pipeline_id=%s&pipeline_base_path=%s' % (pipeline.pipeline_id, pipeline.base_path))
        

        
class ReadMeHandler (webapp2.RequestHandler):
    def get(self):
        page = pages.ReadMePage(self.request)
        self.response.write (page.render())
        
        
        
class MonitorHandler (webapp2.RequestHandler):
    def get(self):
        page = pages.MonitorPage(self.request)
        
        self.response.write (page.render())
    
    
        
class UploadHandler (blobstore_handlers.BlobstoreUploadHandler, webapp2.RequestHandler):
    @ndb.transactional
    def post(self):
        uploads = self.get_uploads()
        upload_file = self.get_uploads('file')
        blob_info = upload_file[0]
        # store the blob we just uploaded in datastore
        blobFile = BlobFileModel(filename=self.request.get('filename'),
                                 blobkey=str(blob_info.key()))
        blobFile.put()
        self.redirect('/populate?just_uploaded_blobkey=%s' % blob_info.key())
        
    



application = webapp2.WSGIApplication([
                                       ('/populate', PopulateHandler),
                                       ('/run', RunHandler),
                                       ('/monitor', MonitorHandler),
                                       ('/upload', UploadHandler),
                                       ('/*', ReadMeHandler)
                                       ], debug=True)
