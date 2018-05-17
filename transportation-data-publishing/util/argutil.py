'''
Utility to create an argparser with predefined arguments. 
https://docs.python.org/3/library/argparse.html#module-argparse
'''
import argparse

ARGUMENTS = {
    'dataset' : {
        'action' : 'store',
        'type' : str,
        'help' : 'Name of the dataset that will be published. Must match entry in Knack config file.'
    },
    'device_type' : {
        'action' : 'store',
        'type' : str,
        'choices' : ['signals', 'travel_sensors', 'cameras', 'gridsmart'],
        'help' : 'Type of device to ping.'
    },
    'eval_type' : {
        'action' : 'store',  
        'choices' : ['phb', 'traffic_signal'],
        'type' : str,
        'help' : 'The type of evaluation score to rank.'
    },
    'app_name' : {
        'action' : 'store',
        'choices' : ['data_tracker_prod', 'data_tracker_test', 'visitor_sign_in_prod'],
        'type' : str,
        'help' : 'Name of the knack application that will be accessed'
    },
    '--destination' : {
        'flag' : '-d',
        'action' : 'append' ,
        'choices' : ['socrata', 'agol', 'csv'],
        'required' : True,
        'type' : str,
        'help' : 'Destination dataset(s) to which data will be published. Can be repeated for multiple destinations.'
    },
    '--json' : {
        'action' : 'store_true' ,
        'default' : False,
        'help' : 'Write device data to JSON.'
    },
    '--replace' : {
        'flag' : '-r',
        'action' : 'store_true',
        'default' : False,
        'help' : 'Replace all destination data with source data.'
    }
}



def get_parser(prog, description, *args):
    '''
    Return a parser with the specified arguments. Each arg
    in *args must be defined in ARGUMENTS.
    '''
    parser = argparse.ArgumentParser(
        prog=prog,
        description=description
    )
    
    for arg_name in args:
        arg_def = ARGUMENTS[arg_name]

        if arg_def.get('flag'):
            parser.add_argument(
                arg_name,
                arg_def.pop('flag'),
                **arg_def)
        else:
            parser.add_argument(
                arg_name,
                **arg_def)

    return(parser)


if __name__ == '__main__':
    # tests
    name = 'fake_program.py'
    description ='Fake program which does nothing useful.'
    
    parser = get_parser(
        name,
        description,
        'dataset',
        'app_name',
        '--destination',
        '--replace')

    print(parser.parse_args(['cameras', 'data_tracker_prod', '-d=socrata', '-r']))








