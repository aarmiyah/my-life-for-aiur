function getParameterByName(name) {
    name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
    var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
        results = regex.exec(location.search);
    return results === null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
}



function init() {
	console.log ('in init');
	gapi.client.setApiKey('320519671625-7svuqihoci9vsaclpjfhsflaskn7fk0q.apps.googleusercontent.com');
	gapi.client.load('schedulr_api', 'v1', function() {
		console.log ('in updateBread creation');
		var updateBread = (function () {
			var looper;
			var updateBreadState = function () {
				// do api call
				console.log (getParameterByName('pipeline_id'));
				gapi.client.schedulr_api.jobs.check_is_finished({
					'pipeline_id': getParameterByName('pipeline_id')
					}).execute (function (resp) {
						console.log (resp);
					});
				// if the pipeline is done
				// clear the looper and update the image
			};
			
			return {
				run: function () {
					clearInterval(looper);
					setInterval(updateBreadState, 5000);
				}
			}
		}());

		updateBread.run();
	});
}