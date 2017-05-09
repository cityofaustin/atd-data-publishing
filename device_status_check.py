import os
import logging
import pdb
import arrow

'''
todo:
get ips
create config for get knack data
update knack records
d
'''

def get_ips():
    print("getting ip addresses")


def ping_ip(ip):
    print(ip)
    logging.info( 'ping {}'.format(ip) )

    response = os.system("ping -n 1 " + ip)
    
    print(response)

    if response != 0:
        logging.warning( 'no response from {}'.format(ip) )
        return "COMM_FAIL"

    else:
        return "OK"


def update_records():
    print("update device status and date in knack")


def cli_args():
    parser = argparse.ArgumentParser(prog='device_status+check.py', description='Ping network devices to verify connenectivity.')
    parser.add_argument('device_type', action="store", type=str, help='Type of device to ping. \'bluetooth\' or \'cctv\'.')
    args = parser.parse_args()
    
    return(args)


def main():
    
    ping_results = []

    for ip in ips:
        result = ping_ip(ip)
        ping_results.append({
            "address" : ip,
            "status" : result
        })

     return ping_results


if __name__ == '__main__':
    
    #  parse command-line arguments
    args = cli_args()
    device_type = args.device_type

    now = arrow.now()
    now_s = now.format('YYYY_MM_DD')
    
    #  init logging 
    #  with one logfile per dataset per day
    logfile = './log/check_{}_{}.log'.format(device_type, now_s)
    logging.basicConfig(filename=logfile, level=logging.INFO)
    logging.info( 'args: {}'.format( str(args) ) )




    results = main()

print(results)