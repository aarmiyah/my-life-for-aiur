from google.appengine.ext import ndb

class BlobFileModel (ndb.Model):
    filename = ndb.StringProperty()
    blobkey = ndb.StringProperty()