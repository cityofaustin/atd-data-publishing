#  conifguration file for email notifications
cfg = {
    'recipients' {
        #  config for fetching recipient email addresses
        'obj' : None,
        'primary_key' : '',
        'scene' : None,
        'view' : None,
        'ref_obj' : [''],
        'filter_field' : 'this is the name of the notifications multiselect to be compared against the nofication name'
    },
    'signal_projects' : {
        'obj' : None,
        'primary_key' : '',
        'scene' : None,
        'view' : None,
        'ref_obj' : [''],
        'filter_field' : 'SEND_NOTIFICATION',  #  sned notificaiton when field is false
        'filters' : 'compare field is True'
    }
}


