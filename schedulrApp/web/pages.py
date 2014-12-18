import jinja2
import os

import schedulrApp.processing.map_reduce
from schedulrApp.storage.db import BlobFileModel

from google.appengine.ext import blobstore





JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)



class SitePage (object):
    def __init__ (self, htmlFile):
            self.values = {
                'title': 'Schedulr Mapreduce',
                'description': "surfing the worldwide inter-tube-way",
                'nav_links': [],
                # add css links
                'css_links': [
                    '//maxcdn.bootstrapcdn.com/bootstrap/3.3.1/css/bootstrap.min.css',
                    'css/main.css'
                ],
                'js_scripts': []
            }
            # add site nav links
            nav_links = ['populate', 'run']
            for link in nav_links:
                self.values['nav_links'].append ({
                                           'text': link,
                                           'url': '/' + link
                                           })
            self.htmlFile = htmlFile
    def render (self):
        template = JINJA_ENVIRONMENT.get_template('/templates/' + self.htmlFile)
        return template.render (self.values)
    
    
            
class PopulatePage (SitePage):
    def __init__ (self, request):
        super(PopulatePage, self).__init__('populate.html')
        
        self.values['upload_url'] = blobstore.create_upload_url ('/upload')
        remaining_blobspace = schedulrApp.processing.map_reduce.MAX_BLOBS - BlobFileModel.query().count()
        self.values['remaining_blobspace'] = remaining_blobspace
        just_uploaded_blobkey = request.get("just_uploaded_blobkey")
        if (just_uploaded_blobkey is not None):
            self.values['just_uploaded_blobkey'] = just_uploaded_blobkey
        
class RunPage (SitePage):
    def __init__ (self, request):
        super(RunPage, self).__init__('run.html')
        # fetch up to 20 blobs to put in a simple dropdown
        self.values['blobfiles'] = BlobFileModel.query().fetch(limit=20)
        
        
class ReadMePage (SitePage):
    def __init__ (self, request=None):
        super(ReadMePage, self).__init__('readme.html')
        
        
        
class MonitorPage (SitePage):
    def __init__ (self, request):
        super(MonitorPage, self).__init__('monitor.html')
        
        self.values['pipeline_id'] = request.get('pipeline_id')
        self.values['pipeline_base_path'] = request.get('pipeline_base_path')
        
        self.values['css_links'].append ('//cdnjs.cloudflare.com/ajax/libs/animate.css/3.2.0/animate.min.css',)
        self.values['js_scripts'].append ('js/monitor-scripts.js')
        self.values['js_scripts'].append ('https://apis.google.com/js/client.js?onload=init')
        