import endpoints
from protorpc import messages
from protorpc import message_types
from protorpc import remote
from mapreduce.mapreduce_pipeline import MapreducePipeline

package = 'schedulr_api'

class TruthValue(messages.Message):
    value = messages.BooleanField(1)
    
class PipelineIdValue(messages.Message):
    pipeline_id = messages.StringField(1)

@endpoints.api(name='schedulr_api', version='v1')
class SchedulrApi(remote.Service):
    """schedulr API v1."""

    @endpoints.method(PipelineIdValue, TruthValue,
                      path='job_status_check/{pipeline_id}', http_method='GET',
                      name='jobs.check_is_finished')
    def is_job_finished(self, request):
        pipeline = MapreducePipeline.from_id (request.pipeline_id)
        return TruthValue (value=(pipeline is not None) and (pipeline.has_finalized))
        

application = endpoints.api_server([SchedulrApi])