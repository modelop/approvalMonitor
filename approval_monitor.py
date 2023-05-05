import json
import os
import uuid
from pathlib import Path

import modelop_sdk.apis.model_manage_api as mm_api
import modelop_sdk.restclient.moc_client as moc_client
import pandas as pd

DEPLOYABLE_MODEL = {}


#
# This is the model initialization function.  This function will be called once when the model is initially loaded.  At
# this time we can read information about the job that is resulting in this model being loaded, including the full
# job in the initialization parameter
#
# Note that in a monitor, the actual model on which the monitor is being run will be in the referenceModel parameter,
# and the monitor code itself will be in the model parameter.
#

# modelop.init
def init(init_param):
	global DEPLOYABLE_MODEL

	job = json.loads(init_param["rawJson"])
	DEPLOYABLE_MODEL = job.get('referenceModel', {})


#
# This method is the modelops metrics method.  This is always called with a pandas dataframe that is arraylike, and
# contains individual rows represented in a dataframe format that is representative of all of the data that comes in
# as the results of the first input asset on the job.  This method will not be invoked until all data has been read
# from that input asset.
#
# In this monitor we do not utilize the input data for anything, as we are instead reading information from the modelop
# sdk based on the provided target model.
#
# data - The input data of the first input asset of the job, as a pandas dataframe
#

# modelop.metrics
def metrics(data: pd.DataFrame):
	results = {
		'totalApprovals': 0,
		'outstandingApprovals': 0,
		'approvals': []
	}
	client = moc_client.MOCClient()
	notifications_api = mm_api.NotificationsApi(client)
	response = notifications_api.find_all_by_deployable_model_id(uuid.UUID(DEPLOYABLE_MODEL.get('id')))
	if response.get('_embedded', {}).get('notifications', None) is not None:
		notifications = response['_embedded']['notifications']
		for notification in notifications:
			if notification.get('notificationType', None) == 'MODEL_APPROVAL_NOTIFICATION':
				results['totalApprovals'] += 1
				if notification.get('open', True):
					results['outstandingApprovals'] += 1
				results[notification.get('approvalType', 'UNKNOWN')] = \
					notification.get('assignment', {}).get('currentStatus', 'UNKNOWN')
				results['approvals'].append(
					{
						'created': notification.get('createdDate', None),
						'open': notification.get('open', True),
						'issue': notification.get('assignment', {}).get('issueId', 'UNKNOWN'),
						'issueLink': notification.get('assignment', {}).get('issueLink', 'UNKNOWN'),
						'status': notification.get('assignment', {}).get('currentStatus', 'UNKNOWN'),
						'reporter': notification.get('assignment', {}).get('jiraIssue', {}).get('reporter', {}).get('displayName', notification.get('approver', 'UNKNOWN')),
						'email': notification.get('assignment', {}).get('jiraIssue', {}).get('reporter', {}).get('emailAddress', 'UNKNOWN')
					}
				)

	yield results


#
# This main method is utilized to simulate what the engine will do when calling the above metrics function.  It takes
# the json formatted data, and converts it to a pandas dataframe, then passes this into the metrics function for
# processing.  This is a good way to develop your models to be conformant with the engine in that you can run this
# locally first and ensure the python is behaving correctly before deploying on a ModelOp engine.
#
def main():
	os.environ["MODELOP_GATEWAY_LOCATION"] = "http://localhost:8090/"
	raw_json = Path('example_job.json').read_text()
	init_param = {'rawJson': raw_json}
	init(init_param)

	data = '''
		{ "foo": 2.2,
			"bar": 1.3,
			"strvalue": "foo",
			"objectvalue": {
				"val1": 0.8392,
				"val2": 0.987
			}
		}
	'''
	data_dict = json.loads(data)
	df = pd.DataFrame.from_dict([data_dict])
	print(json.dumps(next(metrics(df)), indent=2))


if __name__ == '__main__':
	main()
