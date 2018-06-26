from tdutils import argutil

def cli_args(): 
    parser = argutil.get_parser(
        'secondary_signals_updater.py',
        'Update traffic signal records with secondary signal relationships.',
        'app_name'
    )
    
    args = parser.parse_args()
    
    return args


# KNACK_CREDENTIALS = {   
#     'data_tracker_prod': {
#         'app_id' : '5815f29f7f7252cc2ca91c4f',
#         'api_key' : 'f3cfcf20-30c4-43a3-8fa1-a47b0b6d08bb'
#     },
#     'data_tracker_test' : {  #  version 6/20/2018
#         'app_id' : '5b2a515864e03861684b6f98',
#         'api_key' : '3285aca0-762c-11e8-a79c-017fb91751ab'
#     },
#     'data_tracker_test_fulcrum' : {  #  for fulcrum integration testing
#         'app_id' : '5a021921ebddc84e8c83d47a',
#         'api_key' : 'e073a0c0-c488-11e7-b930-a72e24c99def'
#     },
#     'visitor_sign_in_prod' : {
#         'app_id' : '594acb1764d5af4daf93531c',
#         'api_key' : '41bd0b60-56bf-11e7-9a2f-5bd06fd1bcdc'
#     }
# }

# print(type(KNACK_CREDENTIALS["data_tracker_test"]))

# mydict = {'fruits': ['banana', 'apple', 'orange'],
#          'vegetables': ['pepper', 'carrot'], 
#          'cheese': ['swiss', 'cheddar', 'brie']}

# print(mydict['fruits'][0])

