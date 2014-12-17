This is an example jinja2 templated web app that behind the post 
forms, creates a mapreduce pipeline in python on the app engine that:

1) reads a file containing user schedule-preferencea and user schedule data

2) maps to yield the (k, v): 
		((day of week:start:end) of schedule in userschedules, 
		 row of user-prefs matching date of k)
		 
3) shuffle combines all matching k and concatenates their v-lists
	so that we get (k = a (day of week:start:end)
